use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use typed_builder::TypedBuilder;
use utoipa::ToSchema;

use crate::{
    AttributeDefinition, AttributeMap, AttributeValue, GlobalSecondaryIndex, IndexName, ItemKey,
    ItemStreamVersion, KeyAttributes, KeySchemaElement, MultiRegionConsistency, Projection,
    ReplicaDescription, ReplicaUpdate, StorageError, StoredTableInfo, StreamRecord,
    StreamSpecification, TableName, TableStatus, TimestampSecondsFractional, WireItem,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ExclusiveStartKey {
    Key(KeyAttributes),
    Token(String),
}

impl ExclusiveStartKey {
    pub fn to_page_token(
        &self,
        table_info: &StoredTableInfo,
        index_name: Option<&IndexName>,
    ) -> Result<String, StorageError> {
        match self {
            Self::Token(token) => Ok(token.clone()),
            Self::Key(key) => {
                let item_key = if let Some(index_name) = index_name {
                    let index_key_schema = table_info
                        .global_secondary_indexes
                        .as_ref()
                        .and_then(|indexes| indexes.iter().find(|i| i.index_name == *index_name))
                        .map_or(&table_info.key_schema, |idx| &idx.key_schema);
                    ItemKey::from_key_schema_for_index(
                        table_info.table_name.clone(),
                        &table_info.key_schema,
                        index_name,
                        index_key_schema,
                        key,
                    )?
                    .ok_or_else(StorageError::invalid_or_missing_key)?
                } else {
                    ItemKey::from_key_schema(
                        table_info.table_name.clone(),
                        &table_info.key_schema,
                        key,
                    )?
                };
                Ok(item_key.next_page_token()?)
            }
        }
    }
}

impl From<String> for ExclusiveStartKey {
    fn from(value: String) -> Self {
        Self::Token(value)
    }
}

impl From<KeyAttributes> for ExclusiveStartKey {
    fn from(value: KeyAttributes) -> Self {
        Self::Key(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTableRequest {
    pub table_name: TableName,

    pub attribute_definitions: Vec<AttributeDefinition>,

    pub key_schema: Vec<KeySchemaElement>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_secondary_indexes: Option<Vec<CreateGlobalSecondaryIndex>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_specification: Option<StreamSpecification>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndex>>,

    /// Used for DynamoDB compatibility on create-table calls.
    /// Local providers currently ignore this value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<BillingMode>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<OnDemandThroughput>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_policy: Option<serde_json::Value>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse_specification: Option<SseSpecification>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<Tag>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_class: Option<TableClass>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_protection_enabled: Option<bool>,
}

impl CreateTableRequest {
    #[must_use]
    pub fn new(
        table_name: TableName,
        attribute_definitions: Vec<AttributeDefinition>,
        key_schema: Vec<KeySchemaElement>,
        billing_mode: BillingMode,
    ) -> Self {
        Self {
            table_name,
            attribute_definitions,
            key_schema,
            global_secondary_indexes: None,
            stream_specification: None,
            local_secondary_indexes: None,
            billing_mode: Some(billing_mode),
            provisioned_throughput: None,
            on_demand_throughput: None,
            resource_policy: None,
            sse_specification: None,
            tags: None,
            table_class: None,
            deletion_protection_enabled: None,
        }
    }

    #[must_use]
    pub fn with_global_secondary_indexes(
        mut self,
        global_secondary_indexes: Option<Vec<CreateGlobalSecondaryIndex>>,
    ) -> Self {
        self.global_secondary_indexes = global_secondary_indexes;
        self
    }

    #[must_use]
    pub fn with_stream_specification(
        mut self,
        stream_specification: Option<StreamSpecification>,
    ) -> Self {
        self.stream_specification = stream_specification;
        self
    }

    pub fn validate_key_schema(&self) -> Result<(), StorageError> {
        let validation_error = |field: &'static str, message: &'static str| {
            StorageError::validation(format!("{field}:{message}"))
        };

        if self.key_schema.is_empty() {
            return Err(validation_error("key_schema", "must_provide_hash"));
        }
        if self.key_schema.len() > 2 {
            return Err(validation_error("key_schema", "too_many_elements"));
        }

        let mut seen = HashSet::new();
        for key_schema in &self.key_schema {
            if !seen.insert(&key_schema.attribute_name) {
                return Err(validation_error("key_schema", "duplicate_attribute"));
            }
        }

        let definitions: HashMap<_, _> = self
            .attribute_definitions
            .iter()
            .map(|definition| (&definition.attribute_name, definition))
            .collect();
        for key_schema in &self.key_schema {
            if !definitions.contains_key(&key_schema.attribute_name) {
                return Err(validation_error("attribute_definitions", "missing_for_key"));
            }
        }
        Ok(())
    }

    pub fn validate_storage_common(&self) -> Result<(), StorageError> {
        if let Some(global_secondary_indexes) = &self.global_secondary_indexes
            && global_secondary_indexes.len() > 20
        {
            return Err(StorageError::validation(format!(
                "Too many global secondary indexes: {} (max 20)",
                global_secondary_indexes.len()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TableDescription {
    #[serde(rename = "TableName")]
    pub table_name: TableName,

    #[serde(rename = "TableStatus")]
    pub table_status: TableStatus,

    #[serde(rename = "CreationDateTime")]
    pub created_at: TimestampSecondsFractional,

    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,

    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,

    #[serde(rename = "TableSizeBytes")]
    pub table_size_bytes: u64,

    #[serde(rename = "ItemCount")]
    pub item_count: u64,

    #[serde(rename = "TableArn")]
    pub table_arn: String,

    #[serde(rename = "BillingModeSummary", skip_serializing_if = "Option::is_none")]
    pub billing_mode_summary: Option<BillingModeSummary>,

    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndexDescription>>,

    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndexDescription>>,

    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,

    #[serde(
        rename = "StreamSpecification",
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_specification: Option<StreamSpecification>,

    #[serde(rename = "LatestStreamArn", skip_serializing_if = "Option::is_none")]
    pub latest_stream_arn: Option<String>,

    #[serde(rename = "LatestStreamLabel", skip_serializing_if = "Option::is_none")]
    pub latest_stream_label: Option<String>,

    #[serde(rename = "Replicas", skip_serializing_if = "Option::is_none")]
    pub replicas: Option<Vec<ReplicaDescription>>,

    #[serde(
        rename = "MultiRegionConsistency",
        skip_serializing_if = "Option::is_none"
    )]
    pub multi_region_consistency: Option<MultiRegionConsistency>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BillingModeSummary {
    #[serde(rename = "BillingMode", skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<BillingMode>,

    #[serde(
        rename = "LastUpdateToPayPerRequestDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update_to_pay_per_request_date_time: Option<TimestampSecondsFractional>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BillingMode {
    Provisioned,
    PayPerRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableClass {
    Standard,
    StandardInfrequentAccess,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ProvisionedThroughput {
    pub read_capacity_units: i64,
    pub write_capacity_units: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct OnDemandThroughput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_read_request_units: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_write_request_units: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct SseSpecification {
    pub enabled: bool,
    #[serde(rename = "KMSMasterKeyId", skip_serializing_if = "Option::is_none")]
    pub kms_master_key_id: Option<String>,
    #[serde(rename = "SSEType", skip_serializing_if = "Option::is_none")]
    pub sse_type: Option<SseType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum SseType {
    #[serde(rename = "AES256")]
    Aes256,
    #[serde(rename = "KMS")]
    Kms,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct CreateGlobalSecondaryIndex {
    pub index_name: IndexName,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

impl From<CreateGlobalSecondaryIndex> for GlobalSecondaryIndex {
    fn from(value: CreateGlobalSecondaryIndex) -> Self {
        Self {
            index_name: value.index_name,
            key_schema: value.key_schema,
            projection: value.projection,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct LocalSecondaryIndex {
    pub index_name: IndexName,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexStatus {
    Creating,
    Updating,
    Deleting,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProvisionedThroughputDescription {
    #[serde(
        rename = "LastIncreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_increase_date_time: Option<TimestampSecondsFractional>,
    #[serde(
        rename = "LastDecreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_decrease_date_time: Option<TimestampSecondsFractional>,
    #[serde(
        rename = "NumberOfDecreasesToday",
        skip_serializing_if = "Option::is_none"
    )]
    pub number_of_decreases_today: Option<i64>,
    #[serde(rename = "ReadCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub read_capacity_units: Option<i64>,
    #[serde(rename = "WriteCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub write_capacity_units: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GlobalSecondaryIndexDescription {
    #[serde(rename = "IndexName")]
    pub index_name: IndexName,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexStatus", skip_serializing_if = "Option::is_none")]
    pub index_status: Option<IndexStatus>,
    #[serde(rename = "Backfilling", skip_serializing_if = "Option::is_none")]
    pub backfilling: Option<bool>,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,
    #[serde(rename = "IndexSizeBytes", skip_serializing_if = "Option::is_none")]
    pub index_size_bytes: Option<i64>,
    #[serde(rename = "ItemCount", skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,
    #[serde(rename = "IndexArn", skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalSecondaryIndexDescription {
    #[serde(rename = "IndexName")]
    pub index_name: IndexName,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexSizeBytes", skip_serializing_if = "Option::is_none")]
    pub index_size_bytes: Option<i64>,
    #[serde(rename = "ItemCount", skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,
    #[serde(rename = "IndexArn", skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ListTablesRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_table_name: Option<TableName>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListTablesResponse {
    #[serde(
        rename = "LastEvaluatedTableName",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_table_name: Option<TableName>,

    #[serde(rename = "TableNames")]
    pub table_names: Vec<TableName>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteTableRequest {
    pub table_name: TableName,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeTableRequest {
    pub table_name: TableName,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DescribeTableResponse {
    #[serde(rename = "Table")]
    pub table: TableDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct PutItemRequest {
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
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values: Option<AllOld>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_item_collection_metrics: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,
}

impl PutItemRequest {
    #[must_use]
    pub fn new(table_name: TableName, item: HashMap<String, AttributeValue>) -> Self {
        Self {
            table_name,
            item,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
        }
    }

    #[must_use]
    pub fn with_condition_expression(mut self, condition_expression: Option<String>) -> Self {
        self.condition_expression = condition_expression;
        self
    }

    #[must_use]
    pub fn with_expression_attribute_names(
        mut self,
        expression_attribute_names: Option<HashMap<String, String>>,
    ) -> Self {
        self.expression_attribute_names = expression_attribute_names;
        self
    }

    #[must_use]
    pub fn with_expression_attribute_values(
        mut self,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.expression_attribute_values = expression_attribute_values;
        self
    }

    #[must_use]
    pub fn with_return_values(mut self, return_values: Option<AllOld>) -> Self {
        self.return_values = return_values;
        self
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct GetItemRequest {
    pub table_name: TableName,

    pub key: KeyAttributes,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,

    /// When true, request a strongly consistent read. Defaults to false.
    /// When true, request a strongly consistent read. Defaults to false.
    /// When true, request a strongly consistent read. Defaults to false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,
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
#[serde(rename_all = "PascalCase")]
pub struct GetStreamRecordsRequest {
    pub table_name: TableName,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 1000)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetStreamRecordsResponse {
    pub table_name: TableName,
    pub records: Vec<StreamRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<AttributeMap>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct DeleteItemRequest {
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
    pub expected: Option<HashMap<String, ExpectedAttributeValue>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditional_operator: Option<ConditionalOperator>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values: Option<AllOld>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_item_collection_metrics: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,
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
        }
    }

    #[must_use]
    pub fn with_condition_expression(mut self, condition_expression: Option<String>) -> Self {
        self.condition_expression = condition_expression;
        self
    }

    #[must_use]
    pub fn with_expression_attribute_names(
        mut self,
        expression_attribute_names: Option<HashMap<String, String>>,
    ) -> Self {
        self.expression_attribute_names = expression_attribute_names;
        self
    }

    #[must_use]
    pub fn with_expression_attribute_values(
        mut self,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.expression_attribute_values = expression_attribute_values;
        self
    }

    #[must_use]
    pub fn with_return_values(mut self, return_values: Option<AllOld>) -> Self {
        self.return_values = return_values;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<AttributeMap>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateItemRequest {
    pub table_name: TableName,
    #[builder(setter(into))]
    pub key: KeyAttributes,
    #[builder(setter(into))]
    pub update_expression: String,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub attribute_updates: Option<HashMap<String, AttributeValueUpdate>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

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
    pub return_consumed_capacity: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_item_collection_metrics: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub return_values_on_condition_check_failure: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ScanRequest {
    pub table_name: TableName,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<IndexName>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_filter: Option<HashMap<String, Condition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_key: Option<ExclusiveStartKey>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_segments: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
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

    #[must_use]
    pub fn with_index_name(mut self, index_name: Option<IndexName>) -> Self {
        self.index_name = index_name;
        self
    }

    #[must_use]
    pub fn with_limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    #[must_use]
    pub fn with_exclusive_start_key(mut self, exclusive_start_key: Option<String>) -> Self {
        self.exclusive_start_key = exclusive_start_key.map(ExclusiveStartKey::from);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScanResponse {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<AttributeMap>>,

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
    pub global_secondary_indexes: Option<HashMap<String, ConsumedCapacityMetrics>>,
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct QueryRequest {
    pub table_name: TableName,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<IndexName>,

    #[serde(default)]
    pub key_condition_expression: String,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_get: Option<Vec<String>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditional_operator: Option<ConditionalOperator>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_filter: Option<HashMap<String, Condition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_key: Option<ExclusiveStartKey>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_index_forward: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
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

    #[must_use]
    pub fn with_index_name(mut self, index_name: Option<IndexName>) -> Self {
        self.index_name = index_name;
        self
    }

    #[must_use]
    pub fn with_expression_attribute_names(
        mut self,
        expression_attribute_names: Option<HashMap<String, String>>,
    ) -> Self {
        self.expression_attribute_names = expression_attribute_names;
        self
    }

    #[must_use]
    pub fn with_expression_attribute_values(
        mut self,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.expression_attribute_values = expression_attribute_values;
        self
    }

    #[must_use]
    pub fn with_limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    #[must_use]
    pub fn with_exclusive_start_key(mut self, exclusive_start_key: Option<String>) -> Self {
        self.exclusive_start_key = exclusive_start_key.map(ExclusiveStartKey::from);
        self
    }

    #[must_use]
    pub fn with_scan_index_forward(mut self, scan_index_forward: Option<bool>) -> Self {
        self.scan_index_forward = scan_index_forward;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryResponse {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<AttributeMap>>,

    #[serde(rename = "Count")]
    pub count: u32,

    #[serde(rename = "ScannedCount")]
    pub scanned_count: u32,

    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<KeyAttributes>,

    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<ConsumedCapacity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateTableRequest {
    pub table_name: TableName,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<BillingMode>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<OnDemandThroughput>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_protection_enabled: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_updates: Option<Vec<ReplicaUpdate>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse_specification: Option<SseSpecification>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_specification: Option<StreamSpecification>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_class: Option<TableClass>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateTimeToLiveRequest {
    pub table_name: TableName,
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecification,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateTimeToLiveResponse {
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecification,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TimeToLiveSpecification {
    pub attribute_name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct DescribeTimeToLiveRequest {
    pub table_name: TableName,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct DescribeTimeToLiveResponse {
    #[serde(
        rename = "TimeToLiveDescription",
        skip_serializing_if = "Option::is_none"
    )]
    pub time_to_live_description: Option<TimeToLiveDescription>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct TimeToLiveDescription {
    #[serde(rename = "AttributeName", skip_serializing_if = "Option::is_none")]
    pub attribute_name: Option<String>,
    #[serde(rename = "TimeToLiveStatus")]
    pub time_to_live_status: TimeToLiveStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeToLiveStatus {
    Enabling,
    Enabled,
    Disabling,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateGlobalSecondaryIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateGlobalSecondaryIndexAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<DeleteGlobalSecondaryIndexAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateGlobalSecondaryIndexAction {
    pub index_name: IndexName,
    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct DeleteGlobalSecondaryIndexAction {
    pub index_name: IndexName,
}

pub struct SplitDynamoItem {
    pub key_attributes: KeyAttributes,
    pub all_attributes: HashMap<String, AttributeValue>,
    pub non_key_attributes: HashMap<String, AttributeValue>,
}

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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteRequest {
    pub key: KeyAttributes,
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

#[derive(Debug, Clone)]
pub enum PreparedBatchOperation {
    Put {
        table_name: TableName,
        table_info: StoredTableInfo,
        write_request: WriteRequest,
        key_attributes: KeyAttributes,
        non_key_attributes: HashMap<String, AttributeValue>,
        full_item: HashMap<String, AttributeValue>,
    },
    Delete {
        table_name: TableName,
        table_info: StoredTableInfo,
        write_request: WriteRequest,
        key: KeyAttributes,
        existing_item: Option<HashMap<String, AttributeValue>>,
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
    pub limit: Option<u32>,
    pub exclusive_start_key: Option<String>,
    pub scan_index_forward: Option<bool>,
    pub consistent_read: bool,
}

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
    pub condition_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_values_on_condition_check_failure: Option<String>,
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
