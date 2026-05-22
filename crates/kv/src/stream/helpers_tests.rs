use storage_types::{AttributeValue, ItemKey, StreamItemId, StreamName, TableName};
use stream_provider::{StoredStreamPointer, StreamDataType, StreamPointer};

use crate::stream::{
    helpers::create_item_update_stream_entries_wire_encoded,
    item_codec::{StoredStreamItem, decode_stream_item as decode_stored_stream_item},
};

fn table_name() -> TableName {
    TableName::new("stream-envelope-copy-tests")
}

fn item_key(table_name: &TableName) -> ItemKey {
    ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("ORG#1".to_string()),
        Some(AttributeValue::S("ITEM#1".to_string())),
    )
}

fn decode_stream_item(bytes: &[u8]) -> StoredStreamItem {
    decode_stored_stream_item(bytes).expect("decode stream item")
}

fn decode_pointer(pointer_item: &StoredStreamItem) -> StoredStreamPointer {
    storage_types::storage_serde::from_bytes(pointer_item.data.as_slice())
        .expect("decode stored stream pointer")
}

fn extract_pointer_and_image_items(
    entries: &[(crate::key_template::KeyTemplate, Vec<u8>)],
) -> (Vec<StoredStreamItem>, StoredStreamItem) {
    let mut pointer_items = Vec::new();
    let mut image_item = None;

    for (_, value) in entries {
        let stream_item = decode_stream_item(value.as_slice());
        if stream_item.data_type == StreamDataType::StreamPointer {
            pointer_items.push(stream_item);
        } else {
            image_item = Some(stream_item);
        }
    }

    (
        pointer_items,
        image_item.expect("missing image stream item"),
    )
}

#[test]
fn wire_encoded_stream_entries_use_pointer_envelope_for_insert_tests() {
    let table_name = table_name();
    let item_key = item_key(&table_name);
    let item_bytes = br#"{"pk":{"S":"ORG#1"},"sk":{"S":"ITEM#1"}}"#;
    let stream_item_id = StreamItemId::random();

    let entries = create_item_update_stream_entries_wire_encoded(
        &table_name,
        &item_key,
        item_bytes,
        None,
        stream_item_id,
        false,
        None,
    )
    .expect("create stream entries");

    assert_eq!(entries.len(), 3);
    let (pointer_items, image_item) = extract_pointer_and_image_items(&entries);
    assert_eq!(pointer_items.len(), 2);
    assert_eq!(image_item.data, item_bytes);
    assert_eq!(image_item.data_type, StreamDataType::DynamoDbJson);
    assert!(image_item.stream_name.is_some());
    assert!(*image_item.created_at > 0);

    for pointer_item in pointer_items {
        assert!(pointer_item.stream_name.is_none());
        assert!(*pointer_item.created_at > 0);
        let stored_pointer = decode_pointer(&pointer_item);
        assert_eq!(
            stored_pointer.target_item_stream_version(),
            storage_types::ItemStreamVersion::from(stream_item_id)
        );
        let embedded = stored_pointer
            .embedded_items()
            .expect("small insert should embed the new image");
        assert_eq!(embedded.len(), 1);
        assert_eq!(embedded[0].data, item_bytes);
        assert_eq!(embedded[0].data_type, StreamDataType::DynamoDbJson);
    }
}

#[test]
fn wire_encoded_stream_entries_embed_old_and_new_images_for_updates_tests() {
    let table_name = table_name();
    let item_key = item_key(&table_name);
    let new_item = br#"{"pk":{"S":"ORG#1"},"sk":{"S":"ITEM#1"},"v":{"N":"2"}}"#;
    let old_item = br#"{"pk":{"S":"ORG#1"},"sk":{"S":"ITEM#1"},"v":{"N":"1"}}"#;
    let stream_item_id = StreamItemId::random();

    let entries = create_item_update_stream_entries_wire_encoded(
        &table_name,
        &item_key,
        new_item,
        Some(old_item),
        stream_item_id,
        false,
        None,
    )
    .expect("create stream entries");

    assert_eq!(entries.len(), 3);
    let (pointer_items, image_item) = extract_pointer_and_image_items(&entries);
    assert_eq!(pointer_items.len(), 2);
    assert_eq!(image_item.data, new_item);
    assert!(image_item.stream_name.is_some());
    assert!(*image_item.created_at > 0);

    for pointer_item in pointer_items {
        assert!(pointer_item.stream_name.is_none());
        assert!(*pointer_item.created_at > 0);
        let stored_pointer = decode_pointer(&pointer_item);
        assert_eq!(
            stored_pointer.target_item_stream_version(),
            storage_types::ItemStreamVersion::from(stream_item_id)
        );
        let expected_stream_name =
            StreamName::table_item_stream(&table_name, &item_key).expect("item stream");
        match stored_pointer {
            StoredStreamPointer::Embedded {
                stream_name,
                table_name: pointer_table,
                items,
                ..
            } => {
                assert_eq!(stream_name, expected_stream_name);
                assert_eq!(pointer_table, table_name);
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].data, new_item);
                assert_eq!(items[0].data_type, StreamDataType::DynamoDbJson);
                assert_eq!(items[1].data, old_item);
                assert_eq!(items[1].data_type, StreamDataType::DynamoDbJson);

                let pointer: StreamPointer = StoredStreamPointer::Embedded {
                    stream_name,
                    table_name: pointer_table,
                    item_stream_version: storage_types::ItemStreamVersion::from(stream_item_id),
                    items,
                    replication: None,
                }
                .into_stream_pointer(stream_item_id);
                assert_eq!(
                    pointer.stream_name,
                    StreamName::table_item_stream(&table_name, &item_key).expect("item stream")
                );
            }
            StoredStreamPointer::Pointer { .. } => {
                panic!("expected embedded pointer for update stream entry")
            }
        }
    }
}
