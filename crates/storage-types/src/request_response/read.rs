use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use utoipa::ToSchema;

use crate::{
    AttributeMap, AttributeValue, ExclusiveStartKey, IndexName, KeyAttributes,
    ReturnConsumedCapacity, TableName, VecMap, WireItem,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(rename_all = "PascalCase")]
pub struct ScanRequest {
    pub table_name: TableName,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub index_name: Option<IndexName>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub attributes_to_get: Option<Vec<String>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub filter_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub scan_filter: Option<HashMap<String, Condition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_values: Option<AttributeMap>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub limit: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub exclusive_start_key: Option<ExclusiveStartKey>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub total_segments: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub segment: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub consistent_read: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub select: Option<String>,
}

impl ScanRequest {
    #[must_use]
    pub fn new(table_name: TableName) -> Self {
        Self {
            table_name,
            index_name: None,
            attributes_to_get: None,
            conditional_operator: None,
            projection_expression: None,
            filter_expression: None,
            scan_filter: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            limit: None,
            exclusive_start_key: None,
            return_consumed_capacity: None,
            total_segments: None,
            segment: None,
            consistent_read: None,
            select: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScanResponse {
    #[serde(
        rename = "Items",
        skip_serializing_if = "Option::is_none",
        default,
        with = "crate::wire_item::dynamo_json::optional_items"
    )]
    #[schema(value_type = Option<Vec<AttributeMap>>)]
    pub items: Option<Vec<WireItem>>,

    #[serde(rename = "Count")]
    pub count: u32,

    #[serde(rename = "ScannedCount")]
    pub scanned_count: u32,

    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<KeyAttributes>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct ConsumedCapacity {
    #[serde(rename = "TableName")]
    pub table_name: TableName,

    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,

    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<HashMap<String, ConsumedCapacityMetrics>>)]
    pub global_secondary_indexes: Option<VecMap<String, ConsumedCapacityMetrics>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct ConsumedCapacityMetrics {
    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConditionalOperator {
    And,
    Or,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct Condition {
    pub comparison_operator: String,

    pub attribute_value_list: Vec<AttributeValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ExpectedAttributeValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<AttributeValue>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute_value_list: Option<Vec<AttributeValue>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_operator: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct AttributeValueUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<AttributeValue>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<AttributeAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AttributeAction {
    Put,
    Delete,
    Add,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(rename_all = "PascalCase")]
pub struct QueryRequest {
    pub table_name: TableName,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub index_name: Option<IndexName>,

    #[serde(default)]
    pub key_condition_expression: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub attributes_to_get: Option<Vec<String>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub filter_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub query_filter: Option<HashMap<String, Condition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    #[schema(value_type = HashMap<String, String>)]
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_values: Option<AttributeMap>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub limit: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub exclusive_start_key: Option<ExclusiveStartKey>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub consistent_read: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub scan_index_forward: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub select: Option<String>,
}

impl QueryRequest {
    #[must_use]
    pub fn new(table_name: TableName, key_condition_expression: String) -> Self {
        Self {
            table_name,
            index_name: None,
            key_condition_expression,
            attributes_to_get: None,
            conditional_operator: None,
            filter_expression: None,
            query_filter: None,
            projection_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            limit: None,
            exclusive_start_key: None,
            return_consumed_capacity: None,
            consistent_read: None,
            scan_index_forward: None,
            select: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryResponse {
    #[serde(
        rename = "Items",
        skip_serializing_if = "Option::is_none",
        default,
        with = "crate::wire_item::dynamo_json::optional_items"
    )]
    #[schema(value_type = Option<Vec<AttributeMap>>)]
    pub items: Option<Vec<WireItem>>,

    #[serde(rename = "Count")]
    pub count: u32,

    #[serde(rename = "ScannedCount")]
    pub scanned_count: u32,

    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<KeyAttributes>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
}
