use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexName, ItemKey, KeySchemaElement, KeyType,
    Projection, TableKey, TableName,
};

use crate::keyspace::{
    compact::TableStorageId, table_identity::TableIdentity, table_keys::item_key,
};

#[test]
fn primary_item_key_uses_compact_table_id_prefix() {
    let table = TableIdentity::new(
        TableStorageId::new(42),
        TableName::new("orders"),
        Vec::new(),
    );
    let item = ItemKey::table_key(
        TableName::new("orders"),
        AttributeValue::S("pk".to_string()),
        None,
    );

    let key = item_key(&table, &item).expect("key");

    assert_eq!(&key[..5], b"p\0\0\0*");
    assert!(
        !key.windows("orders".len())
            .any(|window| window == b"orders")
    );
}

#[test]
fn gsi_item_key_uses_compact_index_id_prefix() {
    let table = TableIdentity::user_indexes_for_table(
        TableStorageId::new(42),
        &TableName::new("orders"),
        Some(&[gsi("by_status")]),
    );
    let table_key = TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("pk".to_string()),
        None,
    );
    let item = ItemKey::index_key(
        TableName::new("orders"),
        IndexName::new("by_status"),
        AttributeValue::S("status".to_string()),
        None,
        table_key,
    );

    let key = item_key(&table, &item).expect("key");

    assert_eq!(&key[..7], b"g\0\0\0*\0\x01");
    assert!(
        !key.windows("orders".len())
            .any(|window| window == b"orders")
    );
    assert!(
        !key.windows("by_status".len())
            .any(|window| window == b"by_status")
    );
}

fn gsi(name: &str) -> GlobalSecondaryIndex {
    GlobalSecondaryIndex {
        index_name: IndexName::new(name),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: None,
            non_key_attributes: None,
        },
    }
}
