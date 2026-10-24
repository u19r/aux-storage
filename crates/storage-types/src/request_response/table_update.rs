use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeDefinition, AttributeMap, BillingMode, CreateGlobalSecondaryIndex, IndexName,
    KeyAttributes, MaxIndexers, OnDemandThroughput, ProvisionedThroughput, ReplicaUpdate,
    SseSpecification, StreamRetentionDuration, StreamSpecification, TableClass, TableDescription,
    TableName,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateTableRequest {
    pub table_name: TableName,

    /// Aux-storage extension: transfer configuration ownership to an active
    /// replica. Submit to the current primary as a separate table control.
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

    /// Aux-storage extension: increase the ordered item indexer capacity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_indexers: Option<MaxIndexers>,

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

    /// Updates table deletion protection.
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

    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_class: Option<TableClass>,
}

impl UpdateTableRequest {
    #[must_use]
    pub fn new(table_name: TableName) -> Self {
        Self {
            table_name,
            #[cfg(feature = "regional")]
            aux_configuration_primary_region: None,
            max_indexers: None,
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: None,
            sse_specification: None,
            stream_specification: None,
            aux_stream_duration_hours: None,
            aux_default_item_stream_duration_hours: None,
            table_class: None,
        }
    }

    #[must_use]
    pub fn configuration_primary_region(&self) -> Option<&str> {
        #[cfg(feature = "regional")]
        {
            self.aux_configuration_primary_region.as_deref()
        }
        #[cfg(not(feature = "regional"))]
        {
            None
        }
    }
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

impl std::fmt::Display for TimeToLiveStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Enabling => "ENABLING",
            Self::Enabled => "ENABLED",
            Self::Disabling => "DISABLING",
            Self::Disabled => "DISABLED",
        })
    }
}

impl std::str::FromStr for TimeToLiveStatus {
    type Err = crate::StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ENABLING" => Ok(Self::Enabling),
            "ENABLED" => Ok(Self::Enabled),
            "DISABLING" => Ok(Self::Disabling),
            "DISABLED" => Ok(Self::Disabled),
            _ => Err(crate::StorageError::validation(format!(
                "unknown ttl policy status: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateGlobalSecondaryIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateGlobalSecondaryIndexAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<DeleteGlobalSecondaryIndexAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateGlobalSecondaryIndexAction {
    pub index_name: IndexName,
    /// Unused. Accepted for `DynamoDB` compatibility but currently ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct DeleteGlobalSecondaryIndexAction {
    pub index_name: IndexName,
}

pub struct SplitDynamoItem {
    pub key_attributes: KeyAttributes,
    pub all_attributes: AttributeMap,
    pub non_key_attributes: AttributeMap,
}
