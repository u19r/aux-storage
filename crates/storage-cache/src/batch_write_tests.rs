use std::collections::{BTreeMap, HashMap};

use storage_types::{
    AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest, DeleteRequest,
    EncodePutRequest, EncodeWriteRequest, PutRequest, TableName, WireEntity, WireItem,
    WriteRequest,
};

use crate::batch_write::{
    PhysicalToLogicalWriteTableMap, RoutedBatchWriteTarget,
    insert_routed_batch_write_encode_request, insert_routed_batch_write_request,
    merge_unprocessed_batch_write_items,
};

fn item(value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_string(), AttributeValue::S(value.to_string()))])
}

#[test]
fn routed_batch_write_request_builder_tracks_physical_to_logical_tables() {
    let mut per_connection = BTreeMap::new();
    let mut physical_to_logical = PhysicalToLogicalWriteTableMap::default();
    insert_routed_batch_write_request(
        &mut per_connection,
        &mut physical_to_logical,
        &Some("TOTAL".into()),
        &Some("SIZE".into()),
        RoutedBatchWriteTarget {
            connection_id: "conn-a".into(),
            physical_table: TableName::new("shared-users"),
            logical_table: TableName::new("users"),
        },
        "conn-a".to_string(),
        vec![WriteRequest {
            put_request: Some(PutRequest {
                item: item("1"),
                indexers: None,
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        }],
    );

    assert!(
        per_connection["conn-a"]
            .request_items
            .contains_key(&TableName::new("shared-users"))
    );
    assert_eq!(
        physical_to_logical.resolve_or_physical("conn-a", TableName::new("shared-users")),
        TableName::new("users")
    );
}

#[test]
fn routed_batch_write_encode_request_builder_tracks_physical_to_logical_tables() {
    let mut per_connection = BTreeMap::new();
    let mut physical_to_logical = PhysicalToLogicalWriteTableMap::default();
    insert_routed_batch_write_encode_request(
        &mut per_connection,
        &mut physical_to_logical,
        &Some("TOTAL".into()),
        &Some("SIZE".into()),
        RoutedBatchWriteTarget {
            connection_id: "conn-a".into(),
            physical_table: TableName::new("shared-users"),
            logical_table: TableName::new("users"),
        },
        "conn-a".to_string(),
        vec![EncodeWriteRequest {
            put_request: Some(EncodePutRequest {
                item: WireEntity::unindexed(
                    WireItem::from_attribute_map(&item("1")).expect("wire item"),
                ),
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        }],
    );

    assert!(
        per_connection["conn-a"]
            .request_items
            .contains_key(&TableName::new("shared-users"))
    );
    assert_eq!(
        physical_to_logical.resolve_or_physical("conn-a", TableName::new("shared-users")),
        TableName::new("users")
    );
}

#[test]
fn merge_unprocessed_batch_write_items_rewrites_physical_tables() {
    let mut merged = HashMap::new();
    let mut physical_to_logical = PhysicalToLogicalWriteTableMap::default();
    physical_to_logical.insert(RoutedBatchWriteTarget {
        connection_id: "conn-a".into(),
        physical_table: TableName::new("shared-users"),
        logical_table: TableName::new("users"),
    });

    merge_unprocessed_batch_write_items(
        &mut merged,
        &physical_to_logical,
        "conn-a",
        HashMap::from([(
            TableName::new("shared-users"),
            vec![WriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest {
                    key: item("1").into(),
                    aux_item_stream_ttl_hours: None,
                }),
            }],
        )]),
    );

    assert!(merged.contains_key(&TableName::new("users")));
}

#[test]
fn request_builders_preserve_request_metadata() {
    let mut plain = BTreeMap::<String, BatchWriteItemRequest>::new();
    let mut encoded = BTreeMap::<String, BatchWriteItemEncodeRequest>::new();
    let mut map = PhysicalToLogicalWriteTableMap::default();
    let target = RoutedBatchWriteTarget {
        connection_id: "conn-a".into(),
        physical_table: TableName::new("shared-users"),
        logical_table: TableName::new("users"),
    };
    insert_routed_batch_write_request(
        &mut plain,
        &mut map,
        &Some("TOTAL".into()),
        &Some("SIZE".into()),
        target.clone(),
        "conn-a".to_string(),
        Vec::new(),
    );
    insert_routed_batch_write_encode_request(
        &mut encoded,
        &mut map,
        &Some("TOTAL".into()),
        &Some("SIZE".into()),
        target,
        "conn-a".to_string(),
        Vec::new(),
    );

    assert_eq!(
        plain["conn-a"].return_consumed_capacity.as_deref(),
        Some("TOTAL")
    );
    assert_eq!(
        plain["conn-a"].return_item_collection_metrics.as_deref(),
        Some("SIZE")
    );
    assert_eq!(
        encoded["conn-a"].return_consumed_capacity.as_deref(),
        Some("TOTAL")
    );
    assert_eq!(
        encoded["conn-a"].return_item_collection_metrics.as_deref(),
        Some("SIZE")
    );
}

#[test]
fn routed_batch_write_request_builder_keeps_primary_and_migration_dispatch_separate() {
    let mut per_dispatch = BTreeMap::new();
    let mut physical_to_logical = PhysicalToLogicalWriteTableMap::default();
    let target = RoutedBatchWriteTarget {
        connection_id: "conn-a".into(),
        physical_table: TableName::new("shared-users"),
        logical_table: TableName::new("users"),
    };

    insert_routed_batch_write_request(
        &mut per_dispatch,
        &mut physical_to_logical,
        &None,
        &None,
        target.clone(),
        ("conn-a".to_string(), "primary".to_string()),
        vec![WriteRequest {
            put_request: Some(PutRequest {
                item: item("1"),
                indexers: None,
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        }],
    );
    insert_routed_batch_write_request(
        &mut per_dispatch,
        &mut physical_to_logical,
        &None,
        &None,
        target,
        ("conn-a".to_string(), "migration".to_string()),
        vec![WriteRequest {
            put_request: Some(PutRequest {
                item: item("2"),
                indexers: None,
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        }],
    );

    assert_eq!(per_dispatch.len(), 2);
}
