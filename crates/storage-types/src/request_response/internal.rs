use crate::{
    AttributeMap, IndexName, KeyAttributes, StorageError, StorageResult, StoredTableInfo,
    StreamRetentionDuration, TableName, WriteRequest, serde_types::validate_query_fields,
};

#[derive(Debug, Clone)]
pub enum PreparedBatchOperation {
    Put {
        table_name: TableName,
        table_info: StoredTableInfo,
        write_request: WriteRequest,
        key_attributes: KeyAttributes,
        non_key_attributes: AttributeMap,
        full_item: AttributeMap,
        indexers: Option<Vec<String>>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    },
    Delete {
        table_name: TableName,
        table_info: StoredTableInfo,
        write_request: WriteRequest,
        key: KeyAttributes,
        existing_item: Option<AttributeMap>,
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

/// Request object for `query_table` operation and its Aux remote source-page
/// transport.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryTableRequest {
    pub table_name: TableName,
    pub index_name: Option<IndexName>,
    pub key_condition_expression: String,
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,
    pub expression_attribute_values: Option<AttributeMap>,
    pub projection_expression: Option<String>,
    pub limit: Option<u32>,
    pub exclusive_start_key: Option<String>,
    pub scan_index_forward: Option<bool>,
    pub consistent_read: bool,
}

impl QueryTableRequest {
    pub fn validate_for_dynamodb(&self) -> StorageResult<()> {
        validate_query_fields(
            &self.table_name,
            &self.key_condition_expression,
            None,
            self.projection_expression.as_deref(),
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
        )
        .map_err(StorageError::validation)
    }
}

impl From<QueryTableRequest> for crate::QueryRequest {
    fn from(request: QueryTableRequest) -> Self {
        let mut query = Self::new(request.table_name, request.key_condition_expression);
        query.index_name = request.index_name;
        query.expression_attribute_names = request.expression_attribute_names;
        query.expression_attribute_values = request.expression_attribute_values;
        query.projection_expression = request.projection_expression;
        query.limit = request.limit;
        query.exclusive_start_key = request
            .exclusive_start_key
            .map(crate::ExclusiveStartKey::Token);
        query.scan_index_forward = request.scan_index_forward;
        query.consistent_read = Some(request.consistent_read);
        query
    }
}
