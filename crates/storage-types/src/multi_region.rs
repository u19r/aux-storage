use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::TimestampSecondsFractional;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MultiRegionConsistency {
    Eventual,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplicaStatus {
    Creating,
    Active,
    Updating,
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct CreateReplicaAction {
    pub region_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct UpdateReplicaAction {
    pub region_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct DeleteReplicaAction {
    pub region_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReplicaUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateReplicaAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateReplicaAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<DeleteReplicaAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReplicaDescription {
    pub region_name: String,
    pub replica_status: ReplicaStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_status_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_inaccessible_date_time: Option<TimestampSecondsFractional>,
}
