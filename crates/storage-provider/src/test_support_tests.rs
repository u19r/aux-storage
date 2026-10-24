//! Bare-table fixtures shared by the kv and sql harmonised table tests.
//!
//! Provider construction stays per backend; everything here is provider
//! agnostic. Available under the `test-support` feature only.

use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, PutItemRequest, StoredTableInfo, TableName,
};

use crate::{PhysicalWrite, TablePutInput, TableStreamContext, TtlWriteContext};

#[must_use]
pub fn unique_table_name(prefix: &str) -> TableName {
    TableName::new(&format!("{prefix}_{}", uuid::Uuid::new_v4()))
}

pub fn key_attributes(pk: &str) -> KeyAttributes {
    let mut key = KeyAttributes::default();
    key.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    key
}

pub fn item(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("data".to_owned(), AttributeValue::S("value".to_owned())),
    ])
}

pub fn stream_context() -> TableStreamContext {
    stream_context_with_immediate(false)
}

pub fn stream_context_with_immediate(immediate_gsi_consistency: bool) -> TableStreamContext {
    // Bare fixtures mirror the provider default (`false`); armed-table
    // fixtures pass `true` explicitly.
    TableStreamContext {
        item_revision: None,
        stream_item_id: None,
        target_item_stream_version: None,
        created_at: None,
        immediate_gsi_consistency,
        gsi_lifecycle: None,
    }
}

pub fn ttl_context_for(table_name: &TableName) -> TtlWriteContext {
    TtlWriteContext::direct(table_name.clone()).expect("ttl context")
}

pub fn create_request(name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
}

pub fn put_input(
    table_name: &TableName,
    table: &StoredTableInfo,
    values: HashMap<String, AttributeValue>,
) -> TablePutInput {
    put_input_with_immediate(table_name, table, values, false)
}

pub fn put_input_with_immediate(
    table_name: &TableName,
    table: &StoredTableInfo,
    values: HashMap<String, AttributeValue>,
    immediate_gsi_consistency: bool,
) -> TablePutInput {
    TablePutInput {
        write: PhysicalWrite::try_new(
            0,
            table_name.clone(),
            PutItemRequest::new(
                table_name.clone(),
                storage_types::WireItem::try_from(&(values)).unwrap(),
            ),
            ttl_context_for(table_name),
        )
        .expect("physical write"),
        table: std::sync::Arc::new(table.clone()),
        stream: stream_context_with_immediate(immediate_gsi_consistency),
        ttl_config: None,
        item_stream_ttl_hours: None,
    }
}
