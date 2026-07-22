use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use utoipa::ToSchema;

use crate::{AttributeMap, AttributeValue, KeyAttributes, StreamRetentionDuration, TableName};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct BatchWriteItemRequest {
    pub request_items: HashMap<TableName, Vec<WriteRequest>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct WriteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_request: Option<PutRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_request: Option<DeleteRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct PutRequest {
    pub item: HashMap<String, AttributeValue>,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct TransactPutRequest {
    pub table_name: TableName,

    pub item: HashMap<String, AttributeValue>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteRequest {
    pub key: KeyAttributes,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct TransactDeleteRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchWriteItemResponse {
    #[serde(rename = "UnprocessedItems", skip_serializing_if = "Option::is_none")]
    pub unprocessed_items: Option<HashMap<TableName, Vec<WriteRequest>>>,

    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<serde_json::Value>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct BatchGetItemRequest {
    pub request_items: HashMap<TableName, KeysAndAttributes>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct KeysAndAttributes {
    #[schema(value_type = Vec<HashMap<String, AttributeValue>>)]
    pub keys: SmallVec<[KeyAttributes; 8]>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchGetItemResponse {
    #[serde(rename = "Responses", skip_serializing_if = "Option::is_none")]
    pub responses: Option<HashMap<TableName, Vec<AttributeMap>>>,

    #[serde(rename = "UnprocessedKeys", skip_serializing_if = "Option::is_none")]
    pub unprocessed_keys: Option<HashMap<TableName, KeysAndAttributes>>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TransactGetItemsRequest {
    pub transact_items: Vec<TransactGetItem>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TransactGetItem {
    pub get: TransactGetRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TransactGetRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactGetItemsResponse {
    #[serde(rename = "Responses")]
    pub responses: Vec<ItemResponse>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<AttributeMap>,
}
