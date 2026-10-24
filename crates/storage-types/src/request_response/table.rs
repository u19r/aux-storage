use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use utoipa::ToSchema;

use crate::{
    AttributeDefinition, GlobalSecondaryIndex, IndexName, KeySchemaElement, MaxIndexers,
    MultiRegionConsistency, Projection, ReplicaDescription, StorageError, StorageResult,
    StoredTableInfo, StreamRetentionDuration, StreamSpecification, TableName, TableStatus,
    TimestampMillis, TimestampSecondsFractional,
};

/// Derive the stable stream identifiers returned with table responses.
pub fn latest_stream_metadata(
    table_name: &TableName,
    created_at: TimestampMillis,
    stream_specification: Option<&StreamSpecification>,
) -> (Option<String>, Option<String>) {
    if !stream_specification.is_some_and(|specification| specification.stream_enabled) {
        return (None, None);
    }

    let (stream_arn, stream_label) = stream_identity(table_name, created_at);
    (Some(stream_arn), Some(stream_label))
}

/// Construct the stable identity after the caller has established that its
/// table stream is enabled.
pub fn stream_identity(table_name: &TableName, created_at: TimestampMillis) -> (String, String) {
    let stream_label = DateTime::<Utc>::from(created_at)
        .format("%Y-%m-%dT%H:%M:%S%.3f")
        .to_string();
    let stream_arn =
        format!("arn:aws:dynamodb:us-east-1:123456789012:table/{table_name}/stream/{stream_label}");
    (stream_arn, stream_label)
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTableRequest {
    pub table_name: TableName,

    pub attribute_definitions: Vec<AttributeDefinition>,

    pub key_schema: Vec<KeySchemaElement>,

    /// Aux-storage extension: maximum ordered item indexer declarations.
    #[serde(default, skip_serializing_if = "MaxIndexers::is_zero")]
    #[builder(default)]
    pub max_indexers: MaxIndexers,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub global_secondary_indexes: Option<Vec<CreateGlobalSecondaryIndex>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub stream_specification: Option<StreamSpecification>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndex>>,

    /// Used for DynamoDB compatibility on create-table calls.
    /// Local providers currently ignore this value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<BillingMode>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub provisioned_throughput: Option<ProvisionedThroughput>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub on_demand_throughput: Option<OnDemandThroughput>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub resource_policy: Option<serde_json::Value>,

    /// Return the item preimage when the condition fails if set to `ALL_OLD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub sse_specification: Option<SseSpecification>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub tags: Option<Vec<Tag>>,

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub table_class: Option<TableClass>,

    /// Protects the table from accidental `DeleteTable` calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub deletion_protection_enabled: Option<bool>,

    /// Aux-storage extension: table stream retention duration in hours.
    #[serde(
        rename = "AuxStreamDurationHours",
        skip_serializing_if = "Option::is_none"
    )]
    #[builder(default)]
    pub aux_stream_duration_hours: Option<StreamRetentionDuration>,

    /// Aux-storage extension: default item stream retention duration in hours.
    #[serde(
        rename = "AuxDefaultItemStreamDurationHours",
        skip_serializing_if = "Option::is_none"
    )]
    #[builder(default)]
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
            max_indexers: MaxIndexers::ZERO,
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

        for (position, key_schema) in self.key_schema.iter().enumerate() {
            if self.key_schema[..position]
                .iter()
                .any(|previous| previous.attribute_name == key_schema.attribute_name)
            {
                return Err(validation_error("key_schema", "duplicate_attribute"));
            }
        }

        for key_schema in &self.key_schema {
            if !self
                .attribute_definitions
                .iter()
                .any(|definition| definition.attribute_name == key_schema.attribute_name)
            {
                return Err(validation_error("attribute_definitions", "missing_for_key"));
            }
        }
        Ok(())
    }

    pub fn validate_for_storage(&self) -> Result<(), StorageError> {
        self.validate_key_schema()?;
        if let Some(global_secondary_indexes) = &self.global_secondary_indexes
            && global_secondary_indexes.len() > 20
        {
            return Err(StorageError::validation(format!(
                "Too many global secondary indexes: {} (max 20)",
                global_secondary_indexes.len()
            )));
        }
        if let Some(global_secondary_indexes) = &self.global_secondary_indexes {
            for (position, index) in global_secondary_indexes.iter().enumerate() {
                if global_secondary_indexes[..position]
                    .iter()
                    .any(|previous| previous.index_name == index.index_name)
                {
                    return Err(StorageError::validation(format!(
                        "duplicate GSI name '{}'",
                        index.index_name
                    )));
                }
                for key in &index.key_schema {
                    if !self
                        .attribute_definitions
                        .iter()
                        .any(|definition| definition.attribute_name == key.attribute_name)
                    {
                        return Err(StorageError::validation(format!(
                            "GSI '{}' key attribute '{}' missing from attribute definitions",
                            index.index_name, key.attribute_name
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Build the one authoritative catalog image before either SQL or KV
    /// lowers a create into physical rows or keys.
    pub fn stored_table_info_at(
        &self,
        created_at: TimestampMillis,
    ) -> StorageResult<StoredTableInfo> {
        self.validate_for_storage()?;
        let table_stream_duration = self.aux_stream_duration_hours.unwrap_or_default();
        Ok(StoredTableInfo {
            table_name: self.table_name.clone(),
            table_status: TableStatus::Active,
            created_at,
            attribute_definitions: self.attribute_definitions.clone(),
            key_schema: self.key_schema.clone(),
            max_indexers: self.max_indexers,
            global_secondary_indexes: self
                .global_secondary_indexes
                .as_ref()
                .map(|indexes| indexes.iter().cloned().map(Into::into).collect()),
            table_size_bytes: 0,
            item_count: 0,
            stream_specification: self.stream_specification.clone(),
            table_stream_duration,
            default_item_stream_duration: self
                .aux_default_item_stream_duration_hours
                .unwrap_or(table_stream_duration),
            deletion_protection_enabled: self.deletion_protection_enabled.unwrap_or(false),
        })
    }
}

/// Table metadata pre-encoded for the SQL catalog columns, derived once from
/// a [`CreateTableRequest`] and shared by the sqlite, turso, and postgres
/// table-creation paths.
#[derive(Debug, Clone)]
pub struct PreparedTableMetadata {
    pub created_at: TimestampMillis,
    pub attribute_definitions_json: String,
    pub key_schema_json: String,
    pub max_indexers: MaxIndexers,
    pub global_secondary_indexes_json: Option<String>,
    pub stream_specification_json: Option<String>,
    pub deletion_protection_enabled: bool,
    pub table_stream_duration: StreamRetentionDuration,
    pub default_item_stream_duration: StreamRetentionDuration,
}

pub fn prepare_table_metadata(
    request: &CreateTableRequest,
) -> StorageResult<PreparedTableMetadata> {
    prepare_table_metadata_at(request, TimestampMillis::now())
}

pub fn prepare_table_metadata_at(
    request: &CreateTableRequest,
    created_at: TimestampMillis,
) -> StorageResult<PreparedTableMetadata> {
    let attribute_definitions_json = serde_json::to_string(&request.attribute_definitions)?;
    let key_schema_json = serde_json::to_string(&request.key_schema)?;
    let global_secondary_indexes_json = request
        .global_secondary_indexes
        .as_ref()
        .map(|indexes| {
            let storage_indexes: Vec<GlobalSecondaryIndex> =
                indexes.iter().cloned().map(Into::into).collect();
            serde_json::to_string(&storage_indexes)
        })
        .transpose()?;
    let stream_specification_json = request
        .stream_specification
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let deletion_protection_enabled = request.deletion_protection_enabled.unwrap_or(false);
    let table_stream_duration = request.aux_stream_duration_hours.unwrap_or_default();
    let default_item_stream_duration = request
        .aux_default_item_stream_duration_hours
        .unwrap_or(table_stream_duration);

    Ok(PreparedTableMetadata {
        created_at,
        attribute_definitions_json,
        key_schema_json,
        max_indexers: request.max_indexers,
        global_secondary_indexes_json,
        stream_specification_json,
        deletion_protection_enabled,
        table_stream_duration,
        default_item_stream_duration,
    })
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

    #[serde(rename = "MaxIndexers", default)]
    pub max_indexers: MaxIndexers,

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

    /// Installed regional configuration owner; absent for ordinary local
    /// tables.
    #[cfg(feature = "regional")]
    #[serde(
        rename = "AuxConfigurationPrimaryRegion",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(
        min_length = 1,
        max_length = 64,
        pattern = "^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
    )]
    pub aux_configuration_primary_region: Option<String>,

    #[serde(
        rename = "MultiRegionConsistency",
        skip_serializing_if = "Option::is_none"
    )]
    pub multi_region_consistency: Option<MultiRegionConsistency>,
}

impl TableDescription {
    #[must_use]
    pub fn from_stored_table_info(table_info: StoredTableInfo, table_status: TableStatus) -> Self {
        let indexes_are_active = matches!(&table_status, TableStatus::Active);
        let (latest_stream_arn, latest_stream_label) = latest_stream_metadata(
            &table_info.table_name,
            table_info.created_at,
            table_info.stream_specification.as_ref(),
        );
        Self {
            table_name: table_info.table_name.clone(),
            table_status,
            created_at: table_info.created_at.into(),
            attribute_definitions: table_info.attribute_definitions,
            key_schema: table_info.key_schema,
            max_indexers: table_info.max_indexers,
            table_size_bytes: table_info.table_size_bytes,
            item_count: table_info.item_count,
            table_arn: format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{}",
                table_info.table_name
            ),
            billing_mode_summary: Some(BillingModeSummary {
                billing_mode: Some(BillingMode::PayPerRequest),
                last_update_to_pay_per_request_date_time: None,
            }),
            global_secondary_indexes: table_info
                .global_secondary_indexes
                .map(|indexes| global_secondary_index_descriptions(indexes, indexes_are_active))
                .filter(|indexes| !indexes.is_empty()),
            local_secondary_indexes: None,
            provisioned_throughput: None,
            stream_specification: table_info.stream_specification,
            latest_stream_arn,
            latest_stream_label,
            deletion_protection_enabled: table_info.deletion_protection_enabled,
            replicas: None,
            #[cfg(feature = "regional")]
            aux_configuration_primary_region: None,
            multi_region_consistency: None,
        }
    }
}

fn global_secondary_index_descriptions(
    indexes: Vec<GlobalSecondaryIndex>,
    indexes_are_active: bool,
) -> Vec<GlobalSecondaryIndexDescription> {
    indexes
        .into_iter()
        .filter(|index| !index.index_name.is_ttl_index())
        .map(|index| GlobalSecondaryIndexDescription {
            index_name: index.index_name,
            key_schema: index.key_schema,
            projection: index.projection,
            index_status: indexes_are_active.then_some(IndexStatus::Active),
            backfilling: indexes_are_active.then_some(false),
            provisioned_throughput: None,
            index_size_bytes: None,
            item_count: None,
            index_arn: None,
        })
        .collect()
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
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
