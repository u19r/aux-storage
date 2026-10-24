use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use utoipa::ToSchema;

use crate::{
    AttributeMap, AttributeValue, AttributeValueUpdate, ConditionalOperator,
    ExpectedAttributeValue, KeyAttributes, ReturnConsumedCapacity, StorageResult,
    StreamRetentionDuration, TableName, WireItem,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(rename_all = "PascalCase")]
pub struct PutItemRequest {
    pub table_name: TableName,

    #[serde(with = "crate::wire_item::dynamo_json")]
    #[schema(value_type = HashMap<String, AttributeValue>)]
    pub item: WireItem,

    /// Names total at most 1,024 UTF-8 bytes per item, including absent
    /// attributes. Aux-storage extension: ordered top-level string
    /// attributes addressable by read plans.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub indexers: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_values: Option<AttributeMap>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values: Option<AllOld>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_item_collection_metrics: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values_on_condition_check_failure: Option<String>,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    #[builder(default)]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl PutItemRequest {
    #[must_use]
    pub fn new(table_name: TableName, item: WireItem) -> Self {
        Self {
            table_name,
            item,
            indexers: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }
    }

    /// Validate and canonicalize the owned item before billing or table
    /// preparation. Both entry points consume the same request policy.
    pub fn normalized_for_write(mut self) -> StorageResult<Self> {
        self.item = self.item.normalized_for_write()?.0;
        Ok(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AllOld {
    None,
    AllOld,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PutItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<AttributeMap>,
}

impl PutItemResponse {
    /// Shape a put response from the previous image. Only
    /// `ReturnValues::AllOld` surfaces old attributes; every other shape
    /// reports no attributes. Shared by KV and SQL put paths so response
    /// shaping never diverges per backend.
    #[must_use]
    pub fn for_return_values(
        old_item: Option<AttributeMap>,
        return_values: Option<AllOld>,
    ) -> Self {
        let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
            old_item
        } else {
            None
        };

        Self { attributes }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetItemRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,

    /// When true, request a strongly consistent read. Defaults to false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,
}

impl GetItemRequest {
    #[must_use]
    pub fn new(table_name: TableName, key: impl Into<KeyAttributes>) -> Self {
        Self {
            table_name,
            key: key.into(),
            attributes_to_get: None,
            consistent_read: None,
            projection_expression: None,
            expression_attribute_names: None,
            return_consumed_capacity: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetItemResponse {
    #[serde(
        rename = "Item",
        skip_serializing_if = "Option::is_none",
        default,
        with = "crate::wire_item::dynamo_json::optional"
    )]
    #[schema(value_type = Option<AttributeMap>)]
    pub item: Option<WireItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteItemRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_values: Option<AttributeMap>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub conditional_operator: Option<ConditionalOperator>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values: Option<AllOld>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_item_collection_metrics: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values_on_condition_check_failure: Option<String>,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    #[builder(default)]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl DeleteItemRequest {
    #[must_use]
    pub fn new(table_name: TableName, key: impl Into<KeyAttributes>) -> Self {
        Self {
            table_name,
            key: key.into(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<AttributeMap>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateItemRequest {
    pub table_name: TableName,
    #[builder(setter(into))]
    pub key: KeyAttributes,
    /// Names total at most 1,024 UTF-8 bytes per item, including absent
    /// attributes. Aux-storage extension: omitted preserves, present
    /// replaces, and an empty list clears.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub indexers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default, setter(strip_option, into))]
    pub update_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub attribute_updates: Option<HashMap<String, AttributeValueUpdate>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_values: Option<AttributeMap>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values: Option<ReturnValuesOldNewUpdated>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_item_collection_metrics: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values_on_condition_check_failure: Option<String>,

    /// Aux-storage extension: item stream retention duration in hours.
    #[serde(
        rename = "AuxItemStreamTtlHours",
        skip_serializing_if = "Option::is_none"
    )]
    #[builder(default)]
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReturnValuesOldNewUpdated {
    None,
    AllOld,
    UpdatedOld,
    AllNew,
    UpdatedNew,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<AttributeMap>,
}
