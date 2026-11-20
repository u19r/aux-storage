use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeMap, KeyAttributes, ReturnConsumedCapacity, TableName, TransactDeleteRequest,
    TransactPutRequest,
};

#[derive(Clone, Serialize, Deserialize, ToSchema, Default)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TransactWriteItemsRequest {
    pub transact_items: Vec<TransactWriteItem>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, ToSchema, Default)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
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

impl TransactWriteItem {
    /// The member's table, whichever action it carries.
    #[must_use]
    pub fn table_name(&self) -> Option<&TableName> {
        self.put
            .as_ref()
            .map(|put| &put.table_name)
            .or_else(|| self.update.as_ref().map(|update| &update.table_name))
            .or_else(|| self.delete.as_ref().map(|delete| &delete.table_name))
            .or_else(|| self.condition_check.as_ref().map(|check| &check.table_name))
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[serde(rename_all = "PascalCase")]
pub struct TransactUpdateRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    pub update_expression: String,

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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[serde(rename_all = "PascalCase")]
pub struct TransactConditionCheckRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    pub condition_expression: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<AttributeMap>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactWriteItemsResponse {
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::ConsumedCapacity>>,

    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<serde_json::Value>,
}
