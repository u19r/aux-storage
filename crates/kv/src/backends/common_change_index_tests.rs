use std::collections::HashMap;

use storage_condition::Condition;
use storage_types::{
    AttributeDefinition, AttributeValue, ItemStreamVersion, KeyAttributeType, KeySchemaElement,
    KeyType, StreamSpecification, StreamViewType, TableName, TableStatus, TimestampMillis,
};

use crate::{
    backends::common::plan_table_write_preflighted,
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    sorted_kv_store::TransactWriteTableOperation,
    storage_ops::CHANGE_INDEX_PREFIX,
};

#[test]
fn given_transaction_put_with_stream_when_planned_then_change_index_marker_is_atomic_mutation() {
    let table_info = storage_types::StoredTableInfo {
        table_name: TableName::new("orders"),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        max_indexers: storage_types::MaxIndexers::ZERO,
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from(0),
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewImage),
        }),
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        table_stream_duration:
            storage_types::StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        default_item_stream_duration:
            storage_types::StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    };
    let table_identity =
        TableIdentity::new(TableStorageId::new(1), TableName::new("orders"), Vec::new());
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("order-1".to_string()));
    let operations = vec![TransactWriteTableOperation::Put {
        table_identity,
        table_info,
        item,
        indexers: None,
        item_stream_ttl_hours: None,
        condition: None::<Condition>,
        return_values_on_condition_check_failure: None,
        replication: None,
        ttl_config: None,
    }];
    let stream_ids = vec![Some(storage_types::StreamItemId::from(
        ItemStreamVersion::new(42),
    ))];

    let plan = plan_table_write_preflighted(&operations, vec![None], &stream_ids, false)
        .expect("transaction write plan");

    assert!(plan.mutations.iter().any(|mutation| {
        matches!(
            mutation,
            crate::backends::common::KvMutation::Put { key, value }
                if value.is_empty()
                    && key.starts_with(CHANGE_INDEX_PREFIX.as_bytes())
        )
    }));
}
