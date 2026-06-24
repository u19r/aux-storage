use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http_error::HttpApiError;
use storage::{DatabaseManager, ReplicationMutationApplyOutcome};
use storage_provider::ListChangeIndexMarkersRequest;
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DeleteTableRequest, DescribeStreamRequest, DescribeTableRequest, DescribeTimeToLiveRequest,
    GetItemRequest, GetRecordsRequest, GetShardIteratorRequest, GetStreamRecordsRequest,
    ListStreamsRequest, ListTablesRequest, MultiRegionConsistency, PutItemRequest, QueryRequest,
    ReadSequenceProviderCapabilities, ReadSequenceRequest, ReplicaDescription,
    ReplicationApplyRequest, ReplicationHeartbeatRequest, ScanRequest, StreamSpecification,
    TableDescription, TableName, TimestampMillis, TransactGetItemsRequest,
    TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest, UpdateTimeToLiveRequest,
};

use crate::types::{
    ReplicationLogicalBackfillImportRequest, Response, UpdateContinuousBackupsRequest,
};

#[derive(Clone, Default)]
pub struct StorageApiManagerOptions {
    pub self_region: Option<String>,
    pub sync_write_proposer: Option<Arc<dyn SyncWriteProposer>>,
    pub sync_proposal_pipeline_limits: storage_sync::SyncProposalPipelineLimits,
    pub sync_read_barrier: Option<Arc<dyn SyncReadBarrier>>,
    pub sync_health_reporter: Option<Arc<dyn SyncHealthReporter>>,
    pub read_sequence_capabilities: Option<ReadSequenceProviderCapabilities>,
    #[cfg(test)]
    pub read_sequence_after_root_step_hook: Option<Arc<dyn ReadSequenceAfterRootStepHook>>,
}

impl fmt::Debug for StorageApiManagerOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_builder = formatter.debug_struct("StorageApiManagerOptions");
        debug_builder
            .field("self_region", &self.self_region)
            .field(
                "sync_write_proposer",
                &self.sync_write_proposer.as_ref().map(|_| "<configured>"),
            )
            .field(
                "sync_proposal_pipeline_limits",
                &self.sync_proposal_pipeline_limits,
            )
            .field(
                "sync_read_barrier",
                &self.sync_read_barrier.as_ref().map(|_| "<configured>"),
            )
            .field(
                "sync_health_reporter",
                &self.sync_health_reporter.as_ref().map(|_| "<configured>"),
            )
            .field(
                "read_sequence_capabilities",
                &self.read_sequence_capabilities,
            );
        #[cfg(test)]
        debug_builder.field(
            "read_sequence_after_root_step_hook",
            &self
                .read_sequence_after_root_step_hook
                .as_ref()
                .map(|_| "<configured>"),
        );
        debug_builder.finish()
    }
}

#[async_trait]
pub trait SyncWriteProposer: Send + Sync {
    async fn propose_sync_write(
        &self,
        request: storage_sync::SyncWriteProposalRequest,
    ) -> Result<storage_sync::SyncProposalResponse, HttpApiError>;
}

#[async_trait]
pub trait SyncReadBarrier: Send + Sync {
    async fn ensure_linearizable_read(&self) -> Result<(), HttpApiError>;
}

#[async_trait]
pub trait SyncHealthReporter: Send + Sync {
    async fn sync_health(&self) -> Result<storage_sync::SyncHealthResponse, HttpApiError>;
}

#[cfg(test)]
#[async_trait]
pub trait ReadSequenceAfterRootStepHook: Send + Sync {
    async fn after_root_step(&self) -> Result<(), HttpApiError>;
}

pub struct StorageApiManagerImpl {
    db: Arc<DatabaseManager>,
    self_region: Option<String>,
    pub(super) sync_write_proposer: Option<Arc<dyn SyncWriteProposer>>,
    pub(super) sync_proposal_pipeline: Arc<SyncProposalPipeline>,
    pub(super) sync_read_barrier: Option<Arc<dyn SyncReadBarrier>>,
    pub(super) sync_health_reporter: Option<Arc<dyn SyncHealthReporter>>,
    pub(super) read_sequence_capabilities: ReadSequenceProviderCapabilities,
    #[cfg(test)]
    pub(super) read_sequence_after_root_step_hook: Option<Arc<dyn ReadSequenceAfterRootStepHook>>,
}

impl StorageApiManagerImpl {
    #[must_use]
    pub fn new_with_options(db: Arc<DatabaseManager>, options: StorageApiManagerOptions) -> Self {
        let self_region = options
            .self_region
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let read_sequence_capabilities = options
            .read_sequence_capabilities
            .unwrap_or_else(|| db.read_sequence_capabilities());
        Self {
            db,
            self_region,
            sync_write_proposer: options.sync_write_proposer,
            sync_proposal_pipeline: Arc::new(SyncProposalPipeline::new(
                options.sync_proposal_pipeline_limits,
            )),
            sync_read_barrier: options.sync_read_barrier,
            sync_health_reporter: options.sync_health_reporter,
            read_sequence_capabilities,
            #[cfg(test)]
            read_sequence_after_root_step_hook: options.read_sequence_after_root_step_hook,
        }
    }

    #[must_use]
    pub fn db(&self) -> &Arc<DatabaseManager> {
        &self.db
    }

    pub(crate) fn table_arn(table_name: &TableName) -> String {
        format!("arn:aws:dynamodb:us-east-1:123456789012:table/{table_name}")
    }

    pub(crate) fn latest_stream_metadata(
        table_name: &TableName,
        created_at: TimestampMillis,
        stream_specification: Option<&StreamSpecification>,
    ) -> (Option<String>, Option<String>) {
        if !stream_specification.is_some_and(|spec| spec.stream_enabled) {
            return (None, None);
        }

        let stream_label = DateTime::<Utc>::from(created_at)
            .format("%Y-%m-%dT%H:%M:%S%.3f")
            .to_string();
        let stream_arn = format!("{}/stream/{stream_label}", Self::table_arn(table_name));
        (Some(stream_arn), Some(stream_label))
    }

    pub(crate) async fn apply_multi_region_state(
        &self,
        table_description: &mut TableDescription,
    ) -> Result<(), HttpApiError> {
        let (replicas, multi_region_consistency): (
            Option<Vec<ReplicaDescription>>,
            Option<MultiRegionConsistency>,
        ) = self
            .db()
            .get_multi_region_table_state(&table_description.table_name)
            .await?;
        table_description.replicas = replicas;
        table_description.multi_region_consistency = multi_region_consistency;
        Ok(())
    }

    pub(crate) fn replication_self_region_name(&self) -> Option<String> {
        self.self_region.clone().or_else(|| {
            std::env::var(crate::constants::STORAGE_REPLICATION_SELF_REGION_ENV)
                .ok()
                .or_else(|| std::env::var("AWS_REGION").ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
    }

    pub(crate) fn validate_replication_source_region(
        &self,
        source_region: &str,
    ) -> Result<String, HttpApiError> {
        let source_region = source_region.trim();
        if source_region.is_empty() {
            return Err(HttpApiError::validation_error(
                "SourceRegion is required for replication requests",
            ));
        }
        Ok(source_region.to_string())
    }

    pub(crate) fn is_applied_replication_outcome(
        &self,
        outcome: ReplicationMutationApplyOutcome,
    ) -> bool {
        matches!(outcome, ReplicationMutationApplyOutcome::Applied)
    }

    pub(super) async fn ensure_sync_read_barrier(
        &self,
        consistent_read: bool,
    ) -> Result<(), HttpApiError> {
        if consistent_read && let Some(barrier) = self.sync_read_barrier.as_ref() {
            barrier.ensure_linearizable_read().await?;
        }
        Ok(())
    }

    pub(super) async fn sync_health_internal(&self) -> Result<Response, HttpApiError> {
        let health = if let Some(reporter) = self.sync_health_reporter.as_ref() {
            reporter.sync_health().await?
        } else {
            storage_sync::SyncHealthResponse::disabled()
        };
        record_sync_health_metrics(&health);
        Ok(Response::SyncHealth(health))
    }
}

pub(super) struct SyncProposalPipeline {
    limits: storage_sync::SyncProposalPipelineLimits,
    in_flight: AtomicUsize,
}

impl SyncProposalPipeline {
    fn new(limits: storage_sync::SyncProposalPipelineLimits) -> Self {
        Self {
            limits,
            in_flight: AtomicUsize::new(0),
        }
    }

    pub(super) fn admit(
        &self,
        request: &storage_sync::SyncWriteRequest,
    ) -> Result<SyncProposalAdmission<'_>, HttpApiError> {
        let shape = self
            .limits
            .validate_request(request)
            .inspect_err(|_error| {
                increment_sync_write_reject("proposal_limit");
            })?;
        record_sync_proposal_shape_metrics(shape);
        let admitted = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.limits.max_queue_depth).then_some(current + 1)
            })
            .is_ok();
        if !admitted {
            record_sync_proposal_queue_depth(self.in_flight.load(Ordering::Acquire));
            increment_sync_write_reject("queue_full");
            return Err(HttpApiError::throttled_error(
                "ThrottlingException",
                storage_sync::SyncProposalPipelineQueueFull {
                    max_queue_depth: self.limits.max_queue_depth,
                }
                .to_string(),
            ));
        }
        record_sync_proposal_queue_depth(self.in_flight.load(Ordering::Acquire));
        Ok(SyncProposalAdmission { pipeline: self })
    }
}

pub(super) struct SyncProposalAdmission<'a> {
    pipeline: &'a SyncProposalPipeline,
}

impl Drop for SyncProposalAdmission<'_> {
    fn drop(&mut self) {
        let previous = self.pipeline.in_flight.fetch_sub(1, Ordering::AcqRel);
        record_sync_proposal_queue_depth(previous.saturating_sub(1));
    }
}

fn record_sync_health_metrics(health: &storage_sync::SyncHealthResponse) {
    if let Some(node_id) = health.local_node_id {
        metrics::gauge!(
            "storage.sync.raft.role",
            "node_id" => node_id.to_string(),
            "role" => health.role.as_str()
        )
        .set(1.0);
    }
    if let Some(term) = health.term {
        metrics::gauge!("storage.sync.raft.term").set(term as f64);
    }
    if let Some(commit_index) = health.commit_index {
        metrics::gauge!("storage.sync.raft.commit.index").set(commit_index as f64);
    }
    if let Some(applied_index) = health.applied_index {
        metrics::gauge!("storage.sync.raft.applied.index").set(applied_index as f64);
    }
    for peer in &health.peers {
        if let Some(match_index) = peer.match_index {
            metrics::gauge!(
                "storage.sync.raft.follower.match.index",
                "peer_node_id" => peer.node_id.to_string()
            )
            .set(match_index as f64);
        }
        if let Some(lag_entries) = peer.lag_entries {
            metrics::gauge!(
                "storage.sync.raft.follower.lag.entries",
                "peer_node_id" => peer.node_id.to_string()
            )
            .set(lag_entries as f64);
        }
    }
}

fn record_sync_proposal_shape_metrics(shape: storage_sync::SyncProposalShape) {
    metrics::histogram!("storage.sync.raft.proposal.batch.size")
        .record(shape.operation_count as f64);
    metrics::histogram!("storage.sync.raft.proposal.batch.bytes").record(shape.byte_count as f64);
}

fn record_sync_proposal_queue_depth(depth: usize) {
    metrics::gauge!("storage.sync.raft.proposal.queue.depth").set(depth as f64);
}

pub(super) fn record_sync_proposal_wait_time(duration: std::time::Duration) {
    metrics::histogram!("storage.sync.raft.proposal.wait.ms")
        .record(duration.as_secs_f64() * 1000.0);
}

fn increment_sync_write_reject(reason: &'static str) {
    metrics::counter!("storage.sync.write.reject.total", "reason" => reason).increment(1);
}

pub(super) fn record_sync_write_reject(reason: &'static str) {
    increment_sync_write_reject(reason);
}

#[async_trait]
pub trait StorageApiManager: Send + Sync {
    async fn create_table(&self, request: CreateTableRequest) -> Result<Response, HttpApiError>;
    async fn list_tables(&self, request: ListTablesRequest) -> Result<Response, HttpApiError>;
    async fn delete_table(&self, request: DeleteTableRequest) -> Result<Response, HttpApiError>;
    async fn describe_table(&self, request: DescribeTableRequest)
    -> Result<Response, HttpApiError>;
    async fn put_item(&self, request: PutItemRequest) -> Result<Response, HttpApiError>;
    async fn get_item(&self, request: GetItemRequest) -> Result<Response, HttpApiError>;
    async fn delete_item(&self, request: DeleteItemRequest) -> Result<Response, HttpApiError>;
    async fn query(&self, request: QueryRequest) -> Result<Response, HttpApiError>;
    async fn scan(&self, request: ScanRequest) -> Result<Response, HttpApiError>;
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<Response, HttpApiError>;
    async fn batch_get_item(&self, request: BatchGetItemRequest) -> Result<Response, HttpApiError>;
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn read_sequence(&self, request: ReadSequenceRequest) -> Result<Response, HttpApiError>;
    async fn update_item(&self, request: UpdateItemRequest) -> Result<Response, HttpApiError>;
    async fn update_table(&self, request: UpdateTableRequest) -> Result<Response, HttpApiError>;
    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> Result<Response, HttpApiError>;
    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> Result<Response, HttpApiError>;
    async fn update_continuous_backups(
        &self,
        request: UpdateContinuousBackupsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> Result<Response, HttpApiError>;
    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError>;
    async fn list_streams(&self, request: ListStreamsRequest) -> Result<Response, HttpApiError>;
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<Response, HttpApiError>;
    async fn get_shard_iterator(
        &self,
        request: GetShardIteratorRequest,
    ) -> Result<Response, HttpApiError>;
    async fn get_records(&self, request: GetRecordsRequest) -> Result<Response, HttpApiError>;
    async fn apply_replication(
        &self,
        request: ReplicationApplyRequest,
    ) -> Result<Response, HttpApiError>;
    async fn import_replication_logical_backfill(
        &self,
        request: ReplicationLogicalBackfillImportRequest,
    ) -> Result<Response, HttpApiError>;
    async fn heartbeat_replication(
        &self,
        request: ReplicationHeartbeatRequest,
    ) -> Result<Response, HttpApiError>;
    async fn replication_health(&self) -> Result<Response, HttpApiError>;
    async fn sync_health(&self) -> Result<Response, HttpApiError>;
    async fn clear_all_tables(&self, payload: serde_json::Value) -> Result<Response, HttpApiError>;
    async fn append_table_stream_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError>;
    async fn run_background_job(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError>;
}

#[async_trait]
impl StorageApiManager for StorageApiManagerImpl {
    async fn create_table(&self, request: CreateTableRequest) -> Result<Response, HttpApiError> {
        self.create_table_internal(request).await
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<Response, HttpApiError> {
        self.list_tables_internal(request).await
    }

    async fn delete_table(&self, request: DeleteTableRequest) -> Result<Response, HttpApiError> {
        self.delete_table_internal(request).await
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> Result<Response, HttpApiError> {
        self.describe_table_internal(request).await
    }

    async fn put_item(&self, request: PutItemRequest) -> Result<Response, HttpApiError> {
        self.put_item_internal(request).await
    }

    async fn get_item(&self, request: GetItemRequest) -> Result<Response, HttpApiError> {
        self.get_item_internal(request).await
    }

    async fn delete_item(&self, request: DeleteItemRequest) -> Result<Response, HttpApiError> {
        self.delete_item_internal(request).await
    }

    async fn query(&self, request: QueryRequest) -> Result<Response, HttpApiError> {
        self.query_internal(request).await
    }

    async fn scan(&self, request: ScanRequest) -> Result<Response, HttpApiError> {
        self.scan_internal(request).await
    }

    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<Response, HttpApiError> {
        self.batch_write_item_internal(request).await
    }

    async fn batch_get_item(&self, request: BatchGetItemRequest) -> Result<Response, HttpApiError> {
        self.batch_get_item_internal(request).await
    }

    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> Result<Response, HttpApiError> {
        self.transact_write_items_internal(request).await
    }

    async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> Result<Response, HttpApiError> {
        self.transact_get_items_internal(request).await
    }

    async fn read_sequence(&self, request: ReadSequenceRequest) -> Result<Response, HttpApiError> {
        self.read_sequence_internal(request).await
    }

    async fn update_item(&self, request: UpdateItemRequest) -> Result<Response, HttpApiError> {
        self.update_item_internal(request).await
    }

    async fn update_table(&self, request: UpdateTableRequest) -> Result<Response, HttpApiError> {
        self.update_table_internal(request).await
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> Result<Response, HttpApiError> {
        self.update_time_to_live_internal(request).await
    }

    async fn describe_time_to_live(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> Result<Response, HttpApiError> {
        self.describe_time_to_live_internal(request).await
    }

    async fn update_continuous_backups(
        &self,
        request: UpdateContinuousBackupsRequest,
    ) -> Result<Response, HttpApiError> {
        let UpdateContinuousBackupsRequest {
            table_name,
            point_in_time_recovery_specification,
        } = request;
        let _ = (
            table_name,
            point_in_time_recovery_specification.point_in_time_recovery_enabled,
        );
        Err(HttpApiError::validation_error(
            "UpdateContinuousBackups is not yet supported on the AuxFn storage compatibility \
             surface",
        ))
    }

    async fn list_change_index_markers(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> Result<Response, HttpApiError> {
        self.list_change_index_markers_internal(request).await
    }

    async fn get_stream_records(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        self.get_stream_records_internal(request).await
    }

    async fn list_streams(&self, request: ListStreamsRequest) -> Result<Response, HttpApiError> {
        self.list_streams_internal(request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<Response, HttpApiError> {
        self.describe_stream_internal(request).await
    }

    async fn get_shard_iterator(
        &self,
        request: GetShardIteratorRequest,
    ) -> Result<Response, HttpApiError> {
        self.get_shard_iterator_internal(request).await
    }

    async fn get_records(&self, request: GetRecordsRequest) -> Result<Response, HttpApiError> {
        self.get_records_internal(request).await
    }

    async fn apply_replication(
        &self,
        request: ReplicationApplyRequest,
    ) -> Result<Response, HttpApiError> {
        self.apply_replication_internal(request).await
    }

    async fn import_replication_logical_backfill(
        &self,
        request: ReplicationLogicalBackfillImportRequest,
    ) -> Result<Response, HttpApiError> {
        self.import_replication_logical_backfill_internal(request)
            .await
    }

    async fn heartbeat_replication(
        &self,
        request: ReplicationHeartbeatRequest,
    ) -> Result<Response, HttpApiError> {
        self.heartbeat_replication_internal(request).await
    }

    async fn replication_health(&self) -> Result<Response, HttpApiError> {
        self.replication_health_internal().await
    }

    async fn sync_health(&self) -> Result<Response, HttpApiError> {
        self.sync_health_internal().await
    }

    async fn clear_all_tables(&self, payload: serde_json::Value) -> Result<Response, HttpApiError> {
        self.clear_all_tables_internal(payload).await
    }

    async fn append_table_stream_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.append_table_stream_record_internal(payload).await
    }

    async fn run_background_job(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.run_background_job_internal(payload).await
    }
}
