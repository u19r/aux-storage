use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex, IndexName,
    KeyAttributes, MaxIndexers, OnDemandThroughput, ProvisionedThroughput, ReplicaUpdate,
    SseSpecification, StreamRetentionDuration, StreamSpecification, TableClass, TableDescription,
    TableName,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateTableRequest {
    pub table_name: TableName,

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
