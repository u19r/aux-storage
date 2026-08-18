use storage_types::{AttributeValue, ItemKey, StreamItemId, TableName};
use stream_provider::StreamDataType;

use crate::{
    key_template::VersionstampedWriteConflictPolicy,
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    stream::{
        helpers::{StreamEntryContext, create_item_update_stream_entries_wire_encoded},
        item_codec::{StoredStreamItem, decode_stream_item as decode_stored_stream_item},
        pointer_codec::{CompactStoredStreamPointer, decode_compact_pointer},
    },
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

fn table_identity(table_name: &TableName) -> TableIdentity {
    TableIdentity::new(TableStorageId::new(1), table_name.clone(), Vec::new())
}

fn decode_stream_item(bytes: &[u8]) -> StoredStreamItem {
    decode_stored_stream_item(bytes).expect("decode stream item")
}

fn decode_pointer(pointer_item: &StoredStreamItem) -> CompactStoredStreamPointer {
    decode_compact_pointer(pointer_item.data.as_slice()).expect("decode compact stream pointer")
}

fn extract_pointer_and_image_items(
    entries: &[(crate::key_template::KeyTemplate, Vec<u8>)],
) -> (Vec<StoredStreamItem>, StoredStreamItem) {
    let mut pointer_items = Vec::new();
    let mut image_item = None;

    for (_, value) in entries {
        if value.is_empty() {
            continue;
        }
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
    let table_identity = table_identity(&table_name);

    let entries = create_item_update_stream_entries_wire_encoded(
        StreamEntryContext {
            table_identity: &table_identity,
            table_name: &table_name,
            item_key: &item_key,
            indexers: &[],
            old_indexers: None,
        },
        item_bytes,
        None,
        stream_item_id,
        false,
        None,
    )
    .expect("create stream entries");

    assert_eq!(entries.len(), 5);
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
            stored_pointer.item_stream_version,
            storage_types::ItemStreamVersion::from(stream_item_id)
        );
        let embedded = stored_pointer
            .items
            .as_deref()
            .expect("small insert should embed the new image");
        assert_eq!(embedded.len(), 1);
        assert_eq!(embedded[0].data, item_bytes);
        assert_eq!(embedded[0].data_type, StreamDataType::DynamoDbJson);
    }
}

#[test]
fn wire_encoded_stream_entries_keep_item_rows_on_target_version_tests() {
    let table_name = table_name();
    let item_key = item_key(&table_name);
    let item_bytes = br#"{"pk":{"S":"ORG#1"},"sk":{"S":"ITEM#1"}}"#;
    let stream_item_id = StreamItemId::random();
    let table_identity = table_identity(&table_name);

    let entries = create_item_update_stream_entries_wire_encoded(
        StreamEntryContext {
            table_identity: &table_identity,
            table_name: &table_name,
            item_key: &item_key,
            indexers: &[],
            old_indexers: None,
        },
        item_bytes,
        None,
        stream_item_id,
        false,
        None,
    )
    .expect("create stream entries");

    let target_item_stream_id =
        StreamItemId::from(storage_types::ItemStreamVersion::from(stream_item_id));
    let versionstamped_entries = entries
        .iter()
        .filter(|(template, _)| template.foundationdb_key().is_some())
        .count();
    let conflict_free_versionstamped_entries = entries
        .iter()
        .filter(|(template, _)| {
            template.versionstamped_write_conflict_policy()
                == VersionstampedWriteConflictPolicy::OmitWriteConflictForUniqueKey
        })
        .count();
    let literal_target_entries = entries
        .iter()
        .filter(|(template, _)| {
            template.foundationdb_key().is_none()
                && template
                    .rocks_key()
                    .ends_with(target_item_stream_id.as_bytes())
        })
        .count();

    assert_eq!(versionstamped_entries, 3);
    assert_eq!(conflict_free_versionstamped_entries, 3);
    assert_eq!(literal_target_entries, 2);
}

#[test]
fn wire_encoded_stream_entries_embed_old_and_new_images_for_updates_tests() {
    let table_name = table_name();
    let item_key = item_key(&table_name);
    let new_item = br#"{"pk":{"S":"ORG#1"},"sk":{"S":"ITEM#1"},"v":{"N":"2"}}"#;
    let old_item = br#"{"pk":{"S":"ORG#1"},"sk":{"S":"ITEM#1"},"v":{"N":"1"}}"#;
    let stream_item_id = StreamItemId::random();
    let table_identity = table_identity(&table_name);

    let entries = create_item_update_stream_entries_wire_encoded(
        StreamEntryContext {
            table_identity: &table_identity,
            table_name: &table_name,
            item_key: &item_key,
            indexers: &[],
            old_indexers: None,
        },
        new_item,
        Some(old_item),
        stream_item_id,
        false,
        None,
    )
    .expect("create stream entries");

    assert_eq!(entries.len(), 5);
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
            stored_pointer.item_stream_version,
            storage_types::ItemStreamVersion::from(stream_item_id)
        );
        assert_eq!(stored_pointer.table_id, table_identity.table_id);
        assert_eq!(
            stored_pointer.item_scope,
            item_key.hash_range_key_part().expect("item scope")
        );
        let items = stored_pointer.items.expect("expected embedded pointer");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].data, new_item);
        assert_eq!(items[0].data_type, StreamDataType::DynamoDbJson);
        assert_eq!(items[1].data, old_item);
        assert_eq!(items[1].data_type, StreamDataType::DynamoDbJson);
    }
}
