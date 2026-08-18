use storage_types::{ItemStreamVersion, TableName};
use stream_provider::{EmbeddedStreamItem, StreamDataType};

use crate::{
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    stream::pointer_codec::{
        CompactStoredStreamPointer, decode_compact_pointer, encode_compact_pointer,
        item_stream_name,
    },
};

#[test]
fn compact_pointer_round_trips_embedded_items_without_table_or_stream_names() {
    let table = TableIdentity::new(
        TableStorageId::new(42),
        TableName::new("orders"),
        Vec::new(),
    );
    let pointer = CompactStoredStreamPointer::embedded(
        &table,
        b"pk\x00sk".to_vec(),
        ItemStreamVersion::new(7),
        vec![
            EmbeddedStreamItem {
                data: b"new".to_vec(),
                data_type: StreamDataType::DynamoDbJson,
            },
            EmbeddedStreamItem {
                data: b"old".to_vec(),
                data_type: StreamDataType::DeleteMarker,
            },
        ],
        None,
        Vec::new(),
        None,
    );

    let encoded = encode_compact_pointer(&pointer).expect("encode compact pointer");

    assert!(
        !encoded
            .windows(b"orders".len())
            .any(|window| window == b"orders")
    );
    assert!(
        !encoded
            .windows(b"/stream-item/".len())
            .any(|window| window == b"/stream-item/")
    );

    let decoded = decode_compact_pointer(&encoded).expect("decode compact pointer");
    assert_eq!(decoded.table_id, table.table_id);
    assert_eq!(decoded.item_scope, b"pk\x00sk");
    assert_eq!(decoded.item_stream_version, ItemStreamVersion::new(7));
    let items = decoded.items.expect("embedded items");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].data, b"new");
    assert_eq!(items[0].data_type, StreamDataType::DynamoDbJson);
    assert_eq!(items[1].data, b"old");
    assert_eq!(items[1].data_type, StreamDataType::DeleteMarker);
}

#[test]
fn compact_pointer_reconstructs_public_pointer_at_provider_boundary() {
    let table = TableIdentity::new(TableStorageId::new(9), TableName::new("orders"), Vec::new());
    let pointer = CompactStoredStreamPointer::pointer(
        &table,
        b"item-scope".to_vec(),
        ItemStreamVersion::new(99),
        None,
        Vec::new(),
        None,
    );

    let stored_pointer = pointer.into_stored_pointer(&table).expect("public pointer");

    assert_eq!(stored_pointer.table_name(), &table.table_name);
    assert_eq!(
        stored_pointer.target_item_stream_version(),
        ItemStreamVersion::new(99)
    );
    assert_eq!(
        stored_pointer.stream_name(),
        &item_stream_name(&table.table_name, b"item-scope")
    );
}

#[test]
fn compact_pointer_rejects_truncated_payloads() {
    assert!(decode_compact_pointer(b"P\x01\x00").is_err());
}
