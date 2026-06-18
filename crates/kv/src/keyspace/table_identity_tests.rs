use storage_types::{
    GlobalSecondaryIndex, IndexName, KeySchemaElement, KeyType, Projection, TableName,
};

use crate::keyspace::{
    compact::{IndexStorageId, TableStorageId},
    table_identity::TableIdentity,
};

#[test]
fn assigns_stable_user_index_ids_in_metadata_order() {
    let table = TableName::new("orders");
    let identity = TableIdentity::user_indexes_for_table(
        TableStorageId::new(42),
        &table,
        Some(&[gsi("by_status"), gsi("by_tenant")]),
    );

    assert_eq!(identity.table_id, TableStorageId::new(42));
    assert_eq!(
        identity.index_id_for_name(&IndexName::new("by_status")),
        Some(IndexStorageId::new(1))
    );
    assert_eq!(
        identity.index_id_for_name(&IndexName::new("by_tenant")),
        Some(IndexStorageId::new(2))
    );
    assert_eq!(identity.next_user_index_id(), IndexStorageId::new(3));
}

#[test]
fn tombstone_preserves_identity_and_marks_deleted() {
    let table = TableName::new("orders");
    let identity = TableIdentity::new(TableStorageId::new(7), table.clone(), Vec::new());
    let deleted = identity.clone().mark_deleted();

    assert_eq!(deleted.table_id, identity.table_id);
    assert_eq!(deleted.table_name, table);
    assert!(deleted.deleted);
}

fn gsi(name: &str) -> GlobalSecondaryIndex {
    GlobalSecondaryIndex {
        index_name: IndexName::new(name),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: None,
            non_key_attributes: None,
        },
    }
}
