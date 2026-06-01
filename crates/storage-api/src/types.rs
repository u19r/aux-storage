use std::sync::Arc;

use async_trait::async_trait;
use http_error::HttpApiError;
use openraft::{
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};
use storage::DatabaseManager;
use storage_backfill::{LogicalBackfillChunk, LogicalBackfillManifest, LogicalBackfillResult};
use storage_sync::{SyncNodeId, SyncTypeConfig};

use crate::{
    batch_get_wire_response::BatchGetWireResponse,
    get_wire_response::GetWireResponse,
    health::HealthTracker,
    manager::{StorageApiManager, StorageApiManagerImpl, StorageApiManagerOptions},
    query_wire_response::QueryWireResponse,
};

#[derive(Clone)]
pub struct AppState {
    pub db_manager: Arc<DatabaseManager>,
    pub storage_manager: Arc<dyn StorageApiManager>,
    #[cfg(feature = "queue")]
    pub queue_manager: Option<Arc<queue::QueueManager>>,
    #[cfg(feature = "queue")]
    pub queue_public_base_url: String,
    #[cfg(feature = "queue")]
    pub queue_account_id: String,
    #[cfg(feature = "pubsub")]
    pub pubsub_manager: Option<Arc<pubsub::PubsubManager>>,
    pub health: Arc<HealthTracker>,
    sync_raft_rpc_handler: Option<Arc<dyn SyncRaftRpcHandler>>,
    sync_learner_join_handler: Option<Arc<dyn SyncLearnerJoinHandler>>,
    sync_internal_token: Option<Arc<str>>,
    replication_service_tokens: Vec<Arc<str>>,
}

impl AppState {
    #[must_use]
    pub fn new_with_manager_options(
        db_manager: Arc<DatabaseManager>,
        options: StorageApiManagerOptions,
    ) -> Self {
        let storage_manager = Arc::new(StorageApiManagerImpl::new_with_options(
            db_manager.clone(),
            options,
        )) as Arc<dyn StorageApiManager>;
        Self::with_manager(db_manager, storage_manager)
    }

    #[must_use]
    pub fn with_manager(
        db_manager: Arc<DatabaseManager>,
        storage_manager: Arc<dyn StorageApiManager>,
    ) -> Self {
        Self {
            db_manager,
            storage_manager,
            #[cfg(feature = "queue")]
            queue_manager: None,
            #[cfg(feature = "queue")]
            queue_public_base_url: String::new(),
            #[cfg(feature = "queue")]
            queue_account_id: "000000000000".to_string(),
            #[cfg(feature = "pubsub")]
            pubsub_manager: None,
            health: Arc::new(HealthTracker::new()),
            sync_raft_rpc_handler: None,
            sync_learner_join_handler: None,
            sync_internal_token: None,
            replication_service_tokens: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_sync_internal_token(mut self, token: impl Into<Arc<str>>) -> Self {
        self.sync_internal_token = Some(token.into());
        self
    }

    #[must_use]
    pub fn with_replication_service_tokens<I, T>(mut self, tokens: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Arc<str>>,
    {
        self.replication_service_tokens = tokens.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_sync_raft_rpc_handler(mut self, handler: Arc<dyn SyncRaftRpcHandler>) -> Self {
        self.sync_raft_rpc_handler = Some(handler);
        self
    }

    #[must_use]
    pub fn with_sync_learner_join_handler(
        mut self,
        handler: Arc<dyn SyncLearnerJoinHandler>,
    ) -> Self {
        self.sync_learner_join_handler = Some(handler);
        self
    }

    #[must_use]
    pub fn sync_internal_token(&self) -> Option<&str> {
        self.sync_internal_token.as_deref()
    }

    #[must_use]
    pub fn accepts_replication_service_token(&self, token: &str) -> bool {
        self.replication_service_tokens
            .iter()
            .any(|expected| expected.as_ref() == token)
    }

    #[must_use]
    pub fn has_replication_service_tokens(&self) -> bool {
        !self.replication_service_tokens.is_empty()
    }

    #[must_use]
    pub fn sync_raft_rpc_handler(&self) -> Option<&dyn SyncRaftRpcHandler> {
        self.sync_raft_rpc_handler.as_deref()
    }

    #[must_use]
    pub fn sync_learner_join_handler(&self) -> Option<&dyn SyncLearnerJoinHandler> {
        self.sync_learner_join_handler.as_deref()
    }

    #[cfg(any(feature = "queue", feature = "pubsub"))]
    #[must_use]
    pub fn with_messaging(
        mut self,
        #[cfg(feature = "queue")] queue_manager: Option<Arc<queue::QueueManager>>,
        #[cfg(feature = "queue")] queue_public_base_url: String,
        #[cfg(feature = "queue")] queue_account_id: String,
        #[cfg(feature = "pubsub")] pubsub_manager: Option<Arc<pubsub::PubsubManager>>,
    ) -> Self {
        #[cfg(feature = "queue")]
        {
            self.queue_manager = queue_manager;
            self.queue_public_base_url = queue_public_base_url;
            self.queue_account_id = queue_account_id;
        }
        #[cfg(feature = "pubsub")]
        {
            self.pubsub_manager = pubsub_manager;
        }
        self
    }
}

#[async_trait]
pub trait SyncRaftRpcHandler: Send + Sync + 'static {
    async fn append_entries(
        &self,
        request: AppendEntriesRequest<SyncTypeConfig>,
    ) -> Result<AppendEntriesResponse<SyncNodeId>, RaftError<SyncNodeId>>;

    async fn install_snapshot(
        &self,
        request: InstallSnapshotRequest<SyncTypeConfig>,
    ) -> Result<InstallSnapshotResponse<SyncNodeId>, RaftError<SyncNodeId, InstallSnapshotError>>;

    async fn vote(
        &self,
        request: VoteRequest<SyncNodeId>,
    ) -> Result<VoteResponse<SyncNodeId>, RaftError<SyncNodeId>>;
}

#[async_trait]
pub trait SyncLearnerJoinHandler: Send + Sync + 'static {
    async fn add_sync_learner(
        &self,
        request: SyncLearnerJoinRequest,
    ) -> Result<SyncLearnerJoinResponse, HttpApiError>;

    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> Result<SyncLearnerPromotionResponse, HttpApiError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct SyncLearnerJoinRequest {
    pub node_id: SyncNodeId,
    pub advertise_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_compatibility: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct SyncLearnerJoinResponse {
    pub node_id: SyncNodeId,
    pub log_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct SyncLearnerPromotionResponse {
    pub node_id: SyncNodeId,
    pub log_index: u64,
}

#[derive(Debug, Clone)]

pub enum Response {
    CreateTable(storage_types::CreateTableResponse),
    ListTables(storage_types::ListTablesResponse),
    DeleteTable(storage_types::DeleteTableResponse),
    DescribeTable(storage_types::DescribeTableResponse),
    PutItem(storage_types::PutItemResponse),
    GetItem(storage_types::GetItemResponse),
    GetWire(GetWireResponse),
    DeleteItem(storage_types::DeleteItemResponse),
    UpdateItem(storage_types::UpdateItemResponse),
    UpdateTable(storage_types::UpdateTableResponse),
    UpdateTimeToLive(storage_types::UpdateTimeToLiveResponse),
    Scan(storage_types::ScanResponse),
    Query(storage_types::QueryResponse),
    QueryWire(QueryWireResponse),
    BatchWriteItem(storage_types::BatchWriteItemResponse),
    BatchGetItem(storage_types::BatchGetItemResponse),
    BatchGetWire(BatchGetWireResponse),
    TransactWriteItems(storage_types::TransactWriteItemsResponse),
    TransactGetItems(storage_types::TransactGetItemsResponse),
    GetStreamRecords(storage_types::GetStreamRecordsResponse),
    ListStreams(storage_types::ListStreamsResponse),
    DescribeStream(storage_types::DescribeStreamResponse),
    GetShardIterator(storage_types::GetShardIteratorResponse),
    GetRecords(storage_types::GetRecordsResponse),
    ReplicationApply(storage_types::ReplicationApplyResponse),
    ReplicationHeartbeat(storage_types::ReplicationHeartbeatResponse),
    ReplicationHealth(storage_types::ReplicationHealthResponse),
    SyncHealth(storage_sync::SyncHealthResponse),
    DescribeTimeToLive(storage_types::DescribeTimeToLiveResponse),
    Raw(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationLogicalBackfillImportRequest {
    pub source_region: String,
    #[serde(default)]
    pub table_name: Option<String>,
    #[serde(default)]
    pub require_empty_destination: bool,
    pub manifest: LogicalBackfillManifest,
    pub chunk: LogicalBackfillChunk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationLogicalBackfillImportResponse {
    pub result: LogicalBackfillResult,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct UpdateContinuousBackupsRequest {
    pub table_name: String,
    pub point_in_time_recovery_specification: PointInTimeRecoverySpecification,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct PointInTimeRecoverySpecification {
    pub point_in_time_recovery_enabled: bool,
}
