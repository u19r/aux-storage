use std::collections::HashMap;

use crate::{
    AttributeValue, DynamoRequestValidate, IndexName, ItemStreamVersion, KeyAttributes,
    QueryRequest, StorageError, StorageResult, StoredTableInfo, StreamRetentionDuration, TableName,
    WireItem, WriteRequest, project_wire_items,
};

#[derive(Debug, Clone)]
pub enum PreparedBatchOperation {
    Put {
        table_name: TableName,
        table_info: StoredTableInfo,
        write_request: WriteRequest,
        key_attributes: KeyAttributes,
        non_key_attributes: HashMap<String, AttributeValue>,
        full_item: HashMap<String, AttributeValue>,
        indexers: Option<Vec<String>>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    },
    Delete {
        table_name: TableName,
        table_info: StoredTableInfo,
        write_request: WriteRequest,
        key: KeyAttributes,
        existing_item: Option<HashMap<String, AttributeValue>>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    },
}

/// Request object for `scan_table` operation
#[derive(Debug, Clone)]
pub struct ScanTableRequest {
    pub table_name: TableName,
    pub index_name: Option<IndexName>,
    pub limit: Option<u32>,
    pub exclusive_start_key: Option<String>,
    pub consistent_read: bool,
}

/// Internal scan item carrying the backend's per-key item stream version.
///
/// This is not a public DynamoDB scan shape. It exists for logical export,
/// catchup, and snapshot workflows that need to compare present scan images
/// against concurrent stream records.
#[derive(Debug, Clone)]
pub struct ItemVersionedWireItem {
    pub item: WireItem,
    pub indexers: Vec<String>,
    pub item_stream_version: ItemStreamVersion,
}

/// Request object for `query_table` operation
#[derive(Debug, Clone)]
pub struct QueryTableRequest {
    pub table_name: TableName,
    pub index_name: Option<IndexName>,
    pub key_condition_expression: String,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub projection_expression: Option<String>,
    pub limit: Option<u32>,
    pub exclusive_start_key: Option<String>,
    pub scan_index_forward: Option<bool>,
    pub consistent_read: bool,
}

impl QueryTableRequest {
    pub fn validate_for_dynamodb(&self) -> StorageResult<()> {
        let mut request = QueryRequest::new(
            self.table_name.clone(),
            self.key_condition_expression.clone(),
        )
        .with_index_name(self.index_name.clone())
        .with_expression_attribute_names(self.expression_attribute_names.clone())
        .with_expression_attribute_values(self.expression_attribute_values.clone())
        .with_limit(self.limit)
        .with_scan_index_forward(self.scan_index_forward);
        request.projection_expression = self.projection_expression.clone();
        request.consistent_read = Some(self.consistent_read);
        request
            .validate_for_dynamodb()
            .map_err(StorageError::validation)
    }

    pub fn project_wire_items(&self, items: Vec<WireItem>) -> StorageResult<Vec<WireItem>> {
        project_wire_items(
            items,
            self.projection_expression.as_deref(),
            self.expression_attribute_names.as_ref(),
        )
    }
}
