use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeDefinition, GlobalSecondaryIndex, IndexName, KeySchemaElement, MultiRegionConsistency,
    Projection, ReplicaDescription, StorageError, StreamRetentionDuration, StreamSpecification,
    TableName, TableStatus, TimestampSecondsFractional,
};

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

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndex>>,

    /// Used for DynamoDB compatibility on create-table calls.
    /// Local providers currently ignore this value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<BillingMode>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<OnDemandThroughput>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_policy: Option<serde_json::Value>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse_specification: Option<SseSpecification>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<Tag>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_class: Option<TableClass>,

    /// Protects the table from accidental `DeleteTable` calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_protection_enabled: Option<bool>,

    /// Aux-storage extension: table stream retention duration in hours.
    #[serde(
        rename = "AuxStreamDurationHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub aux_stream_duration_hours: Option<StreamRetentionDuration>,

    /// Aux-storage extension: default item stream retention duration in hours.
    #[serde(
        rename = "AuxDefaultItemStreamDurationHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub aux_default_item_stream_duration_hours: Option<StreamRetentionDuration>,
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
            aux_stream_duration_hours: None,
            aux_default_item_stream_duration_hours: None,
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

    #[serde(rename = "DeletionProtectionEnabled", default)]
    pub deletion_protection_enabled: bool,

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
