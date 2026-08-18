use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeValue, KeyAttributes, StreamRetentionDuration, TableName, TransactDeleteRequest,
    TransactPutRequest,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TransactWriteItemsRequest {
    pub transact_items: Vec<TransactWriteItem>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TransactWriteItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put: Option<TransactPutRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<TransactUpdateRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<TransactDeleteRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_check: Option<TransactConditionCheckRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct TransactUpdateRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    pub update_expression: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexers: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

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
pub struct TransactConditionCheckRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    pub condition_expression: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactWriteItemsResponse {
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,

    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<serde_json::Value>,
}
