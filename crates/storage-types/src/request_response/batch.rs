use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeMap, AttributeValue, KeyAttributes, ReturnConsumedCapacity, StreamRetentionDuration,
    TableName, VecMap, WireItem,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct BatchWriteItemRequest {
    #[schema(value_type = HashMap<TableName, Vec<WriteRequest>>)]
    pub request_items: VecMap<TableName, Vec<WriteRequest>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

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
    #[serde(with = "crate::wire_item::dynamo_json")]
    #[schema(value_type = HashMap<String, AttributeValue>)]
    pub item: WireItem,

    /// Names total at most 1,024 UTF-8 bytes per item, including absent
    /// attributes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexers: Option<Vec<String>>,

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

    #[serde(with = "crate::wire_item::dynamo_json")]
    #[schema(value_type = HashMap<String, AttributeValue>)]
    pub item: WireItem,

    /// Names total at most 1,024 UTF-8 bytes per item, including absent
    /// attributes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexers: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<AttributeMap>,

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
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<AttributeMap>,

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
    #[schema(value_type = HashMap<TableName, Vec<WriteRequest>>)]
    pub unprocessed_items: Option<VecMap<TableName, Vec<WriteRequest>>>,

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
    #[schema(value_type = HashMap<TableName, KeysAndAttributes>)]
    pub request_items: VecMap<TableName, KeysAndAttributes>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct KeysAndAttributes {
    #[schema(value_type = Vec<HashMap<String, AttributeValue>>)]
    pub keys: Vec<KeyAttributes>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchGetItemResponse {
    #[serde(
        rename = "Responses",
        skip_serializing_if = "Option::is_none",
        default,
        with = "crate::wire_item::dynamo_json::optional_table_items"
    )]
    #[schema(value_type = HashMap<TableName, Vec<AttributeMap>>)]
    pub responses: Option<VecMap<TableName, Vec<WireItem>>>,

    #[serde(rename = "UnprocessedKeys", skip_serializing_if = "Option::is_none")]
    #[schema(value_type = HashMap<TableName, KeysAndAttributes>)]
    pub unprocessed_keys: Option<VecMap<TableName, KeysAndAttributes>>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TransactGetItemsRequest {
    pub transact_items: Vec<TransactGetItem>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,
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
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,
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
    #[serde(
        rename = "Item",
        skip_serializing_if = "Option::is_none",
        default,
        with = "crate::wire_item::dynamo_json::optional"
    )]
    #[schema(value_type = Option<AttributeMap>)]
    pub item: Option<WireItem>,
}
