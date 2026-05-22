use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeValue, KeyAttributes, StreamItemId, TableName, TimestampMillis,
    TimestampSecondsFractional,
};

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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationWriteSource {
    Local,
    Replicated,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplicationHybridLogicalClock {
    pub physical_ms: TimestampMillis,
    pub logical: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ReplicationEventMetadata {
    pub origin_region: String,
    pub origin_sequence: StreamItemId,
    pub origin_hlc: ReplicationHybridLogicalClock,
    pub origin_commit_ts: TimestampMillis,
    pub table_replica_epoch: u64,
    pub write_source: ReplicationWriteSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationMutation {
    pub table_name: TableName,
    pub key: KeyAttributes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_image: Option<HashMap<String, AttributeValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_image: Option<HashMap<String, AttributeValue>>,
    pub metadata: ReplicationEventMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationApplyRequest {
    pub source_region: String,
    pub mutations: Vec<ReplicationMutation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationApplyResponse {
    pub received_mutations: usize,
    pub applied_mutations: usize,
    pub skipped_mutations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationHeartbeatRequest {
    pub source_region: String,
    pub sent_at: TimestampMillis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_latest_commit_ts: Option<TimestampMillis>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationHeartbeatResponse {
    pub region_name: String,
    pub received_at: TimestampMillis,
    pub acknowledged_at: TimestampMillis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_applied_commit_ts: Option<TimestampMillis>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationPeerHealth {
    pub region_name: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<TimestampMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_rtt_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_offset_estimate_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_offset_uncertainty_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_staleness_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_latest_commit_ts: Option<TimestampMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_received_commit_ts: Option<TimestampMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_lag_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_queue_depth: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_auth_failure_at: Option<TimestampMillis>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationHealthResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_region: Option<String>,
    pub peers: Vec<ReplicationPeerHealth>,
}
