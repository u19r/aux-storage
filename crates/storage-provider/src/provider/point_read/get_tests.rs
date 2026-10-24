//! Wide contract tests for `GetItemPlan`: empty-key no-read behavior,
//! metadata identity, and schema validation delegation.

use std::{collections::HashMap, sync::Arc};

use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeyAttributes, KeySchemaElement,
    KeyType, MaxIndexers, StorageError, StoredTableInfo, StreamRetentionDuration, TableName,
    TableStatus, TimestampMillis, context::WrappedError as _,
};

use super::get::GetItemPlan;

fn table_metadata(table_name: &str) -> Arc<StoredTableInfo> {
    Arc::new(StoredTableInfo {
        table_name: TableName::new(table_name),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }],
        max_indexers: MaxIndexers::ZERO,
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        default_item_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    })
}

fn item_key(value: &str) -> KeyAttributes {
    HashMap::from([("id".to_string(), AttributeValue::S(value.to_string()))]).into()
}

#[test]
fn given_empty_key_when_checked_then_no_database_read_is_needed() {
    let plan = GetItemPlan::new(TableName::new("table"), KeyAttributes::new());

    assert!(plan.key.is_empty());
    assert_eq!(plan.table, TableName::new("table"));
}

#[test]
fn given_matching_metadata_when_bind_then_borrows_the_single_key() {
    let plan = GetItemPlan::new(TableName::new("table"), item_key("item-1"));

    assert!(!plan.key.is_empty());
    let bound = plan
        .bind(Arc::clone(&table_metadata("table")))
        .expect("bind");

    assert_eq!(bound.table.table_name, TableName::new("table"));
    assert_eq!(bound.keys, &[item_key("item-1")]);
}

#[test]
fn given_foreign_metadata_when_bind_then_internal_identity_error() {
    let plan = GetItemPlan::new(TableName::new("table"), item_key("item-1"));

    let error = plan
        .bind(table_metadata("other"))
        .expect_err("identity must match");

    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("identity")
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn given_key_missing_schema_attribute_when_bind_then_key_error() {
    let plan = GetItemPlan::new(
        TableName::new("table"),
        HashMap::from([("other".to_string(), AttributeValue::S("x".to_string()))]).into(),
    );

    let error = plan
        .bind(table_metadata("table"))
        .expect_err("schema validation must fail");

    assert_eq!(
        error.to_string(),
        StorageError::invalid_or_missing_key().to_string()
    );
}
