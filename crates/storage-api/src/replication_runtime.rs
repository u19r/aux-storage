use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::http::StatusCode;
use config::{StorageReplicationConfig, StorageReplicationPeerConfig};
use http_request::{HttpClient, HttpRequestError};
use storage::{
    DatabaseManager, OutboundReplicationBatch, PeerCheckpointRecord, TableBootstrapCursorRecord,
    TableReplicationConfigRecord, increment_multi_region_auth_failure_total,
    peer_checkpoint_put_request, record_multi_region_heartbeat_rtt,
    record_multi_region_heartbeat_staleness, record_multi_region_replication_lag,
    record_multi_region_sender_queue_depth, table_bootstrap_cursor_put_request,
};
use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillChunkId, LogicalBackfillChunkSummary,
    LogicalBackfillDomain, LogicalBackfillId, LogicalBackfillManifest, LogicalBackfillResult,
    LogicalExportRequest, MultiRegionBootstrapPolicy,
};
use storage_sync::{SyncMultiRegionSenderOwnershipDecision, plan_multi_region_sender_ownership};
use storage_types::{
    ReplicaStatus, ReplicationApplyRequest, ReplicationApplyResponse, ReplicationHeartbeatRequest,
    ReplicationHeartbeatResponse, StorageEnum, StorageError, StorageResult, TableName,
    TimestampMillis,
};
use tokio::{sync::RwLock, task::JoinHandle, time::Instant};
use tracing::{debug, info, warn};

use crate::{
    manager::{SyncHealthReporter, SyncWriteProposer},
    types::{ReplicationLogicalBackfillImportRequest, ReplicationLogicalBackfillImportResponse},
};

const REPLICATION_CONTROL_PLANE_CACHE_TTL: Duration = Duration::from_secs(5);
const REPLICATION_MAX_PROGRESS_PASSES_PER_TICK: usize = 8;
const MULTI_REGION_BOOTSTRAP_LOGICAL_DOMAINS: &[LogicalBackfillDomain] =
    &[LogicalBackfillDomain::ItemRecords];
const LOGICAL_BACKFILL_COMPLETE_CURSOR: &str = "__complete__";

#[derive(Debug, Clone, Default)]
struct CachedReplicationCatalog {
    loaded_at: Option<Instant>,
    table_configs: Vec<TableReplicationConfigRecord>,
    bootstrap_cursors_by_peer: HashMap<String, Vec<TableBootstrapCursorRecord>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationPeerConfig {
    pub region_name: String,
    pub endpoint_url: String,
    pub service_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRuntimeConfig {
    pub self_region: String,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_jitter: Duration,
    pub batch_mutation_limit: usize,
    pub batch_byte_limit: usize,
    pub peers: Vec<ReplicationPeerConfig>,
}

impl ReplicationRuntimeConfig {
    pub fn from_settings(settings: &StorageReplicationConfig) -> StorageResult<Option<Self>> {
        if !settings.is_enabled() {
            return Ok(None);
        }

        let self_region = settings
            .self_region
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                StorageError::validation(
                    "jobs.storage_replication.self_region is required when replication is enabled",
                )
            })?
            .to_string();
        if settings.batch_mutation_limit == 0 || settings.batch_mutation_limit > 1_000 {
            return Err(StorageError::validation(
                "jobs.storage_replication.batch_mutation_limit must be between 1 and 1000",
            ));
        }
        if settings.batch_byte_limit == 0 {
            return Err(StorageError::validation(
                "jobs.storage_replication.batch_byte_limit must be greater than zero",
            ));
        }

        let mut peers = Vec::with_capacity(settings.peers.len());
        let mut seen_regions = HashSet::new();
        for peer in &settings.peers {
            let peer = ReplicationPeerConfig::from_settings(peer)?;
            if peer.region_name == self_region {
                return Err(StorageError::validation(
                    "jobs.storage_replication.peers must not include self_region",
                ));
            }
            if !seen_regions.insert(peer.region_name.clone()) {
                return Err(StorageError::validation(format!(
                    "jobs.storage_replication.peers contains a duplicate region '{}'",
                    peer.region_name
                )));
            }
            peers.push(peer);
        }

        Ok(Some(Self {
            self_region,
            poll_interval: Duration::from_millis(settings.poll_interval_ms),
            heartbeat_interval: Duration::from_millis(settings.heartbeat_interval_ms),
            heartbeat_jitter: Duration::from_millis(settings.heartbeat_jitter_ms),
            batch_mutation_limit: settings.batch_mutation_limit as usize,
            batch_byte_limit: settings.batch_byte_limit as usize,
            peers,
        }))
    }
}

impl ReplicationPeerConfig {
    fn from_settings(settings: &StorageReplicationPeerConfig) -> StorageResult<Self> {
        let region_name = settings.region_name.trim();
        if region_name.is_empty() {
            return Err(StorageError::validation(
                "jobs.storage_replication.peers[].region_name must not be empty",
            ));
        }
        let endpoint_url = settings.endpoint_url.trim().trim_end_matches('/');
        if endpoint_url.is_empty() {
            return Err(StorageError::validation(
                "jobs.storage_replication.peers[].endpoint_url must not be empty",
            ));
        }
        let service_token = settings.service_token.trim();
        if service_token.is_empty() {
            return Err(StorageError::validation(
                "jobs.storage_replication.peers[].service_token must not be empty",
            ));
        }

        Ok(Self {
            region_name: region_name.to_string(),
            endpoint_url: endpoint_url.to_string(),
            service_token: service_token.to_string(),
        })
    }

    fn apply_url(&self) -> String {
        format!(
            "{}{}/_internal/storage/replication/apply",
            self.endpoint_url,
            crate::constants::BASE_PATH
        )
    }

    fn heartbeat_url(&self) -> String {
        format!(
            "{}{}/_internal/storage/replication/heartbeat",
            self.endpoint_url,
            crate::constants::BASE_PATH
        )
    }

    fn logical_backfill_import_url(&self) -> String {
        format!(
            "{}{}/_internal/storage/replication/logical-backfill/import",
            self.endpoint_url,
            crate::constants::BASE_PATH
        )
    }
}

#[async_trait]
pub trait ReplicationPeerClient: Send + Sync {
    async fn apply(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationApplyRequest,
    ) -> StorageResult<ReplicationApplyResponse>;

    async fn heartbeat(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationHeartbeatRequest,
    ) -> StorageResult<ReplicationHeartbeatResponse>;

    async fn import_logical_backfill(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationLogicalBackfillImportRequest,
    ) -> StorageResult<ReplicationLogicalBackfillImportResponse>;
}

#[derive(Clone)]
pub struct HttpReplicationPeerClient {
    client: HttpClient,
}

impl HttpReplicationPeerClient {
    pub fn new() -> StorageResult<Self> {
        let client = HttpClient::new().map_err(|error| {
            StorageError::internal(&format!("build replication http client: {error}"))
        })?;
        Ok(Self { client })
    }
}

#[async_trait]
impl ReplicationPeerClient for HttpReplicationPeerClient {
    async fn apply(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationApplyRequest,
    ) -> StorageResult<ReplicationApplyResponse> {
        self.client
            .post(peer.apply_url())
            .header(
                crate::constants::STORAGE_GATEWAY_API_KEY_HEADER,
                peer.service_token.as_str(),
            )
            .json(request)
            .send()
            .await
            .map_err(map_http_request_error)?
            .error_for_status_with_body()
            .await
            .map_err(map_http_request_error)?
            .json()
            .await
            .map_err(map_http_request_error)
    }

    async fn heartbeat(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationHeartbeatRequest,
    ) -> StorageResult<ReplicationHeartbeatResponse> {
        self.client
            .post(peer.heartbeat_url())
            .header(
                crate::constants::STORAGE_GATEWAY_API_KEY_HEADER,
                peer.service_token.as_str(),
            )
            .json(request)
            .send()
            .await
            .map_err(map_http_request_error)?
            .error_for_status_with_body()
            .await
            .map_err(map_http_request_error)?
            .json()
            .await
            .map_err(map_http_request_error)
    }

    async fn import_logical_backfill(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationLogicalBackfillImportRequest,
    ) -> StorageResult<ReplicationLogicalBackfillImportResponse> {
        self.client
            .post(peer.logical_backfill_import_url())
            .header(
                crate::constants::STORAGE_GATEWAY_API_KEY_HEADER,
                peer.service_token.as_str(),
            )
            .json(request)
            .send()
            .await
            .map_err(map_http_request_error)?
            .error_for_status_with_body()
            .await
            .map_err(map_http_request_error)?
            .json()
            .await
            .map_err(map_http_request_error)
    }
}

#[derive(Clone)]
pub struct StorageReplicationRuntime<C> {
    db: Arc<DatabaseManager>,
    config: ReplicationRuntimeConfig,
    peer_client: Arc<C>,
    sync_health_reporter: Option<Arc<dyn SyncHealthReporter>>,
    sync_write_proposer: Option<Arc<dyn SyncWriteProposer>>,
    control_plane_proposal_sequence: Arc<std::sync::atomic::AtomicU64>,
    catalog_cache: Arc<RwLock<CachedReplicationCatalog>>,
}

impl<C> StorageReplicationRuntime<C>
where C: ReplicationPeerClient + 'static
{
    #[must_use]
    pub fn new(
        db: Arc<DatabaseManager>,
        config: ReplicationRuntimeConfig,
        peer_client: Arc<C>,
    ) -> Self {
        Self {
            db,
            config,
            peer_client,
            sync_health_reporter: None,
            sync_write_proposer: None,
            control_plane_proposal_sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            catalog_cache: Arc::new(RwLock::new(CachedReplicationCatalog::default())),
        }
    }

    #[must_use]
    pub fn with_sync_health_reporter(
        mut self,
        sync_health_reporter: Arc<dyn SyncHealthReporter>,
    ) -> Self {
        self.sync_health_reporter = Some(sync_health_reporter);
        self
    }

    #[must_use]
    pub fn with_sync_write_proposer(
        mut self,
        sync_write_proposer: Arc<dyn SyncWriteProposer>,
    ) -> Self {
        self.sync_write_proposer = Some(sync_write_proposer);
        self
    }

    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let runtime = Arc::new(self);
        runtime
            .config
            .peers
            .iter()
            .map(|peer| {
                let runtime = Arc::clone(&runtime);
                let peer = peer.clone();
                tokio::spawn(async move {
                    runtime.run_peer_loop(peer).await;
                })
            })
            .collect()
    }

    async fn run_peer_loop(self: Arc<Self>, peer: ReplicationPeerConfig) {
        let mut heartbeat_generation = 0u64;
        let mut next_heartbeat_at = Instant::now()
            + deterministic_jitter(
                &peer.region_name,
                heartbeat_generation,
                self.config.heartbeat_jitter,
            );
        loop {
            let heartbeat_due = Instant::now() >= next_heartbeat_at;
            let made_progress = match self
                .run_peer_burst(&peer, REPLICATION_MAX_PROGRESS_PASSES_PER_TICK)
                .await
            {
                Ok(made_progress) => made_progress,
                Err(error) => {
                    warn!(
                        peer_region = %peer.region_name,
                        error = %error,
                        "multi-region replication peer iteration failed"
                    );
                    false
                }
            };

            if heartbeat_due {
                if let Err(error) = self.send_heartbeat(&peer).await {
                    warn!(
                        peer_region = %peer.region_name,
                        error = %error,
                        "multi-region heartbeat send failed"
                    );
                }
                heartbeat_generation = heartbeat_generation.saturating_add(1);
                next_heartbeat_at = Instant::now()
                    + self.config.heartbeat_interval
                    + deterministic_jitter(
                        &peer.region_name,
                        heartbeat_generation,
                        self.config.heartbeat_jitter,
                    );
            }

            let sleep_duration = if made_progress {
                Duration::ZERO
            } else {
                self.config.poll_interval
            };
            if let Err(error) = self.emit_peer_health_metrics(&peer).await {
                warn!(
                    peer_region = %peer.region_name,
                    error = %error,
                    "multi-region replication peer metrics refresh failed"
                );
            }
            if sleep_duration.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(sleep_duration).await;
            }
        }
    }

    pub(crate) async fn run_peer_once(
        &self,
        peer: &ReplicationPeerConfig,
        send_heartbeat: bool,
    ) -> StorageResult<bool> {
        if !self.local_node_owns_outbound_replication().await? {
            return Ok(false);
        }
        let made_progress = self.run_peer_burst(peer, 1).await?;

        if send_heartbeat {
            self.send_heartbeat(peer).await?;
        }

        Ok(made_progress)
    }

    pub(crate) async fn run_peer_burst(
        &self,
        peer: &ReplicationPeerConfig,
        max_passes: usize,
    ) -> StorageResult<bool> {
        if !self.local_node_owns_outbound_replication().await? {
            return Ok(false);
        }
        let mut made_progress = false;
        for _ in 0..max_passes.max(1) {
            let progressed = if self.run_bootstrap_pass(peer).await? {
                true
            } else {
                self.run_steady_state_pass(peer).await?
            };
            made_progress |= progressed;
            if !progressed {
                break;
            }
        }
        Ok(made_progress)
    }

    async fn send_heartbeat(&self, peer: &ReplicationPeerConfig) -> StorageResult<()> {
        if !self.local_node_owns_outbound_replication().await? {
            return Ok(());
        }
        let source_latest_commit_ts = self
            .db
            .get_peer_replication_status(&peer.region_name)
            .await?
            .and_then(|status| status.last_outbound_commit_ts);
        let sent_at = TimestampMillis::now();
        let request = ReplicationHeartbeatRequest {
            source_region: self.config.self_region.clone(),
            sent_at,
            source_latest_commit_ts,
        };
        let start = Instant::now();
        let response = self.peer_client.heartbeat(peer, &request).await?;
        let response_received_at = TimestampMillis::now();
        if response.region_name != peer.region_name {
            return Err(StorageError::internal(&format!(
                "replication heartbeat expected peer '{}' but response reported '{}'",
                peer.region_name, response.region_name
            )));
        }
        let rtt_ms = start.elapsed().as_millis() as u64;
        let clock_sample = estimate_multi_region_clock_offset(
            sent_at,
            response.received_at,
            response.acknowledged_at,
            response_received_at,
        );
        self.db
            .update_peer_replication_status(&peer.region_name, |status| {
                status.last_heartbeat_rtt_ms = Some(rtt_ms);
                if let Some(sample) = clock_sample {
                    status.clock_offset_estimate_ms = Some(sample.offset_estimate_ms);
                    status.clock_offset_uncertainty_ms = Some(sample.uncertainty_ms);
                }
                status.last_remote_applied_commit_ts = response.last_applied_commit_ts;
            })
            .await?;
        record_multi_region_heartbeat_rtt(&peer.region_name, rtt_ms);
        if let Some(sample) = clock_sample {
            record_multi_region_clock_offset_sample(&peer.region_name, sample);
        }
        debug!(
            peer_region = %peer.region_name,
            destination_region = %response.region_name,
            "multi-region heartbeat acknowledged"
        );
        Ok(())
    }

    async fn run_bootstrap_pass(&self, peer: &ReplicationPeerConfig) -> StorageResult<bool> {
        let table_configs = self.cached_table_configs().await?;
        let config_by_table = table_configs
            .into_iter()
            .map(|config| (config.table_name.clone(), config))
            .collect::<HashMap<_, _>>();
        let bootstrap_cursors = self.cached_bootstrap_cursors(&peer.region_name).await?;

        for cursor in bootstrap_cursors {
            let Some(config) = config_by_table.get(&cursor.table_name) else {
                continue;
            };
            if !replica_status_for_peer(config, &peer.region_name).is_some_and(|status| {
                matches!(status, ReplicaStatus::Creating | ReplicaStatus::Updating)
            }) {
                continue;
            }
            if self
                .transfer_logical_backfill_chunk_for_cursor(peer, &cursor)
                .await?
            {
                return Ok(true);
            }

            let batch = self
                .db
                .read_outbound_replication_batch(
                    &self.config.self_region,
                    cursor.last_system_stream_cursor,
                    std::slice::from_ref(&cursor.table_name),
                    &[],
                    self.config.batch_mutation_limit,
                    self.config.batch_byte_limit,
                )
                .await?;
            if self
                .apply_batch_for_bootstrap_cursor(peer, &cursor, batch)
                .await?
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn apply_batch_for_bootstrap_cursor(
        &self,
        peer: &ReplicationPeerConfig,
        cursor: &TableBootstrapCursorRecord,
        batch: OutboundReplicationBatch,
    ) -> StorageResult<bool> {
        if batch.records.is_empty() {
            if batch.checkpoint_cursor != cursor.last_system_stream_cursor {
                self.persist_bootstrap_cursor(peer, cursor, batch.checkpoint_cursor)
                    .await?;
                return Ok(true);
            }
            if batch.reached_end {
                self.db
                    .mark_replica_active(&cursor.table_name, &peer.region_name)
                    .await?;
                self.db
                    .delete_table_bootstrap_cursor(&cursor.table_name, &peer.region_name)
                    .await?;
                self.invalidate_catalog_cache().await;
                info!(
                    peer_region = %peer.region_name,
                    table_name = %cursor.table_name,
                    "multi-region bootstrap catchup completed"
                );
                return Ok(true);
            }
            return Ok(false);
        }

        self.apply_batch(peer, &batch).await?;
        self.persist_bootstrap_cursor(peer, cursor, batch.checkpoint_cursor)
            .await?;
        if batch.reached_end {
            self.db
                .mark_replica_active(&cursor.table_name, &peer.region_name)
                .await?;
            self.db
                .delete_table_bootstrap_cursor(&cursor.table_name, &peer.region_name)
                .await?;
            self.invalidate_catalog_cache().await;
            info!(
                peer_region = %peer.region_name,
                table_name = %cursor.table_name,
                "multi-region bootstrap catchup completed"
            );
        }
        Ok(true)
    }

    async fn persist_bootstrap_cursor(
        &self,
        _peer: &ReplicationPeerConfig,
        cursor: &TableBootstrapCursorRecord,
        checkpoint_cursor: Option<storage_types::StreamItemId>,
    ) -> StorageResult<()> {
        self.persist_table_bootstrap_cursor(&TableBootstrapCursorRecord {
            table_name: cursor.table_name.clone(),
            peer_region: cursor.peer_region.clone(),
            protected_stream_cursor: cursor
                .protected_stream_cursor
                .or(cursor.last_system_stream_cursor),
            last_system_stream_cursor: checkpoint_cursor,
            activation_cursor: next_activation_cursor(checkpoint_cursor, cursor),
            session_started_at: cursor.session_started_at.or(Some(cursor.updated_at)),
            logical_backfill_manifest_id: cursor.logical_backfill_manifest_id.clone(),
            logical_backfill_domain: cursor.logical_backfill_domain.clone(),
            logical_backfill_cursor: cursor.logical_backfill_cursor.clone(),
            updated_at: TimestampMillis::now(),
        })
        .await?;
        self.invalidate_catalog_cache().await;
        Ok(())
    }

    pub(crate) async fn transfer_logical_backfill_chunk_for_cursor(
        &self,
        peer: &ReplicationPeerConfig,
        cursor: &TableBootstrapCursorRecord,
    ) -> StorageResult<bool> {
        if cursor.logical_backfill_cursor.as_deref() == Some(LOGICAL_BACKFILL_COMPLETE_CURSOR) {
            return Ok(false);
        }
        let manifest = logical_bootstrap_manifest(peer, cursor)?;
        let domain = logical_bootstrap_domain(cursor)?;
        let page = self
            .db
            .export_logical_backfill_page(LogicalExportRequest {
                manifest_id: manifest.id.clone(),
                domain,
                table_name: Some(cursor.table_name.to_string()),
                cursor: cursor.logical_backfill_cursor.clone(),
                limit: self
                    .config
                    .batch_mutation_limit
                    .try_into()
                    .unwrap_or(u32::MAX),
            })
            .await?;
        let chunk = LogicalBackfillChunk {
            summary: LogicalBackfillChunkSummary {
                id: LogicalBackfillChunkId::new(logical_chunk_id(cursor, domain))
                    .map_err(|error| StorageError::validation(error.to_string()))?,
                domain,
                record_count: page.records.len().try_into().map_err(|_| {
                    StorageError::internal("logical backfill chunk record count overflow")
                })?,
                checksum: page.checksum,
            },
            records: page.records,
        };
        let request = ReplicationLogicalBackfillImportRequest {
            source_region: self.config.self_region.clone(),
            table_name: Some(cursor.table_name.as_ref().to_string()),
            require_empty_destination: cursor.logical_backfill_cursor.is_none(),
            manifest: manifest.clone(),
            chunk,
        };
        let response = self
            .peer_client
            .import_logical_backfill(peer, &request)
            .await?;
        match response.result {
            LogicalBackfillResult::ChunkImported | LogicalBackfillResult::DuplicateChunkIgnored => {
            }
            unexpected => {
                return Err(StorageError::internal(&format!(
                    "replication peer '{}' returned unexpected logical backfill result \
                     {unexpected:?}",
                    peer.region_name
                )));
            }
        }
        let next_cursor = page
            .next_cursor
            .or_else(|| Some(LOGICAL_BACKFILL_COMPLETE_CURSOR.to_string()));
        self.persist_logical_bootstrap_checkpoint(peer, cursor, &manifest, domain, next_cursor)
            .await?;
        Ok(true)
    }

    async fn persist_logical_bootstrap_checkpoint(
        &self,
        _peer: &ReplicationPeerConfig,
        cursor: &TableBootstrapCursorRecord,
        manifest: &LogicalBackfillManifest,
        domain: LogicalBackfillDomain,
        logical_cursor: Option<String>,
    ) -> StorageResult<()> {
        self.persist_table_bootstrap_cursor(&TableBootstrapCursorRecord {
            table_name: cursor.table_name.clone(),
            peer_region: cursor.peer_region.clone(),
            protected_stream_cursor: cursor
                .protected_stream_cursor
                .or(cursor.last_system_stream_cursor),
            last_system_stream_cursor: cursor.last_system_stream_cursor,
            activation_cursor: cursor.activation_cursor,
            session_started_at: cursor.session_started_at.or(Some(cursor.updated_at)),
            logical_backfill_manifest_id: Some(manifest.id.as_str().to_string()),
            logical_backfill_domain: Some(logical_domain_key(domain).to_string()),
            logical_backfill_cursor: logical_cursor,
            updated_at: TimestampMillis::now(),
        })
        .await?;
        self.invalidate_catalog_cache().await;
        Ok(())
    }

    async fn run_steady_state_pass(&self, peer: &ReplicationPeerConfig) -> StorageResult<bool> {
        let table_configs = self.cached_table_configs().await?;
        let bootstrap_cursors = self.cached_bootstrap_cursors(&peer.region_name).await?;
        let config_by_table = table_configs
            .iter()
            .map(|config| (&config.table_name, config))
            .collect::<HashMap<_, _>>();
        let excluded_tables = bootstrap_cursors
            .iter()
            .filter(|cursor| {
                config_by_table
                    .get(&cursor.table_name)
                    .is_some_and(|config| {
                        replica_status_for_peer(config, &peer.region_name).is_some_and(|status| {
                            matches!(status, ReplicaStatus::Creating | ReplicaStatus::Updating)
                        })
                    })
            })
            .map(|cursor| cursor.table_name.clone())
            .collect::<Vec<_>>();
        let included_tables = steady_state_tables_for_peer(&table_configs, &peer.region_name)
            .into_iter()
            .collect::<Vec<_>>();
        if included_tables.is_empty() {
            return Ok(false);
        }

        let checkpoint = self.db.get_peer_checkpoint(&peer.region_name).await?;
        let batch = self
            .db
            .read_outbound_replication_batch(
                &self.config.self_region,
                checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.last_system_stream_cursor),
                &included_tables,
                &excluded_tables,
                self.config.batch_mutation_limit,
                self.config.batch_byte_limit,
            )
            .await?;

        if batch.records.is_empty() {
            // Empty steady-state scans may have advanced only across control-plane
            // or remote-origin stream pointers. Do not durably move the outbound
            // checkpoint unless at least one local mutation was sent; some backend
            // stream ids are not safe to treat as a future-write high watermark
            // after an empty pre-poll.
            return Ok(false);
        }

        self.apply_batch(peer, &batch).await?;
        self.persist_peer_checkpoint(peer, batch.checkpoint_cursor)
            .await?;
        let last_commit_ts = batch
            .records
            .last()
            .map(|record| record.mutation.metadata.origin_commit_ts);
        self.record_outbound_apply_success(peer, last_commit_ts)
            .await?;
        Ok(true)
    }

    async fn apply_batch(
        &self,
        peer: &ReplicationPeerConfig,
        batch: &OutboundReplicationBatch,
    ) -> StorageResult<()> {
        self.update_sender_queue_depth(peer, batch.records.len() as u64)
            .await?;
        let request = ReplicationApplyRequest {
            source_region: self.config.self_region.clone(),
            mutations: batch
                .records
                .iter()
                .map(|record| record.mutation.clone())
                .collect(),
        };
        let response = match self.peer_client.apply(peer, &request).await {
            Ok(response) => response,
            Err(error) => {
                self.record_outbound_apply_failure(peer, is_auth_error(&error))
                    .await?;
                return Err(error);
            }
        };
        self.update_sender_queue_depth(peer, 0).await?;
        if response.received_mutations != request.mutations.len() {
            return Err(StorageError::internal(&format!(
                "replication peer '{}' acknowledged {} mutations but {} were sent",
                peer.region_name,
                response.received_mutations,
                request.mutations.len()
            )));
        }
        Ok(())
    }

    pub(crate) async fn persist_peer_checkpoint(
        &self,
        peer: &ReplicationPeerConfig,
        checkpoint_cursor: Option<storage_types::StreamItemId>,
    ) -> StorageResult<()> {
        let record = PeerCheckpointRecord {
            peer_region: peer.region_name.clone(),
            last_system_stream_cursor: checkpoint_cursor,
            updated_at: TimestampMillis::now(),
        };
        if let Some(proposer) = self.sync_write_proposer.as_ref() {
            let request = peer_checkpoint_put_request(&record)?;
            proposer
                .propose_sync_write(storage_sync::SyncWriteProposalRequest::new(
                    self.next_control_plane_proposal_id("peer_checkpoint")?,
                    storage_sync::SyncWriteRequest::PutItem(request),
                ))
                .await
                .map_err(|error| StorageError::internal(&format!("{error:?}")))?;
            Ok(())
        } else {
            self.db.put_peer_checkpoint(&record).await
        }
    }

    async fn persist_table_bootstrap_cursor(
        &self,
        record: &TableBootstrapCursorRecord,
    ) -> StorageResult<()> {
        if let Some(proposer) = self.sync_write_proposer.as_ref() {
            let request = table_bootstrap_cursor_put_request(record)?;
            proposer
                .propose_sync_write(storage_sync::SyncWriteProposalRequest::new(
                    self.next_control_plane_proposal_id("bootstrap_cursor")?,
                    storage_sync::SyncWriteRequest::PutItem(request),
                ))
                .await
                .map_err(|error| StorageError::internal(&format!("{error:?}")))?;
            Ok(())
        } else {
            self.db.put_table_bootstrap_cursor(record).await
        }
    }

    async fn update_sender_queue_depth(
        &self,
        peer: &ReplicationPeerConfig,
        sender_queue_depth: u64,
    ) -> StorageResult<()> {
        self.db
            .update_peer_replication_status(&peer.region_name, |status| {
                status.sender_queue_depth = Some(sender_queue_depth);
            })
            .await?;
        record_multi_region_sender_queue_depth(&peer.region_name, sender_queue_depth);
        Ok(())
    }

    async fn record_outbound_apply_success(
        &self,
        peer: &ReplicationPeerConfig,
        last_commit_ts: Option<TimestampMillis>,
    ) -> StorageResult<()> {
        self.db
            .update_peer_replication_status(&peer.region_name, |status| {
                status.last_outbound_apply_at = Some(TimestampMillis::now());
                status.last_outbound_commit_ts = last_commit_ts;
                status.sender_queue_depth = Some(0);
            })
            .await?;
        record_multi_region_sender_queue_depth(&peer.region_name, 0);
        Ok(())
    }

    async fn record_outbound_apply_failure(
        &self,
        peer: &ReplicationPeerConfig,
        auth_failure: bool,
    ) -> StorageResult<()> {
        self.db
            .update_peer_replication_status(&peer.region_name, |status| {
                status.sender_queue_depth = Some(0);
                if auth_failure {
                    status.last_auth_failure_at = Some(TimestampMillis::now());
                }
            })
            .await?;
        record_multi_region_sender_queue_depth(&peer.region_name, 0);
        if auth_failure {
            increment_multi_region_auth_failure_total(&peer.region_name);
        }
        Ok(())
    }

    async fn emit_peer_health_metrics(&self, peer: &ReplicationPeerConfig) -> StorageResult<()> {
        let Some(status) = self
            .db
            .get_peer_replication_status(&peer.region_name)
            .await?
        else {
            return Ok(());
        };
        let now = TimestampMillis::now().timestamp_millis();
        if let Some(last_heartbeat_at) = status.last_inbound_heartbeat_at {
            let staleness_ms = now.saturating_sub(last_heartbeat_at.timestamp_millis()) as u64;
            record_multi_region_heartbeat_staleness(&peer.region_name, staleness_ms);
        }
        let source_latest_commit_ts = status
            .last_received_source_commit_ts
            .or(status.last_outbound_commit_ts);
        let applied_commit_ts = status
            .last_received_commit_ts
            .or(status.last_remote_applied_commit_ts);
        if let (Some(source_latest_commit_ts), Some(applied_commit_ts)) =
            (source_latest_commit_ts, applied_commit_ts)
        {
            let lag_ms = source_latest_commit_ts
                .timestamp_millis()
                .saturating_sub(applied_commit_ts.timestamp_millis())
                as u64;
            record_multi_region_replication_lag(&peer.region_name, lag_ms);
        }
        if let Some(checkpoint) = self.db.get_peer_checkpoint(&peer.region_name).await? {
            let checkpoint_age_ms =
                now.saturating_sub(checkpoint.updated_at.timestamp_millis()) as u64;
            record_sync_multi_region_checkpoint_lag(&peer.region_name, checkpoint_age_ms);
        }
        Ok(())
    }

    async fn cached_table_configs(&self) -> StorageResult<Vec<TableReplicationConfigRecord>> {
        let cache = self.refresh_catalog_if_stale().await?;
        Ok(cache.table_configs.clone())
    }

    async fn cached_bootstrap_cursors(
        &self,
        peer_region: &str,
    ) -> StorageResult<Vec<TableBootstrapCursorRecord>> {
        let cache = self.refresh_catalog_if_stale().await?;
        Ok(cache
            .bootstrap_cursors_by_peer
            .get(peer_region)
            .cloned()
            .unwrap_or_default())
    }

    async fn refresh_catalog_if_stale(&self) -> StorageResult<CachedReplicationCatalog> {
        {
            let cache = self.catalog_cache.read().await;
            if cache
                .loaded_at
                .is_some_and(|loaded_at| loaded_at.elapsed() < REPLICATION_CONTROL_PLANE_CACHE_TTL)
            {
                return Ok(cache.clone());
            }
        }

        let table_configs = self.db.list_table_replication_configs().await?;
        let bootstrap_cursors = self.db.list_table_bootstrap_cursors().await?;
        let mut bootstrap_cursors_by_peer: HashMap<String, Vec<TableBootstrapCursorRecord>> =
            HashMap::new();
        for cursor in bootstrap_cursors {
            bootstrap_cursors_by_peer
                .entry(cursor.peer_region.clone())
                .or_default()
                .push(cursor);
        }
        for cursors in bootstrap_cursors_by_peer.values_mut() {
            cursors.sort_by(|left, right| left.table_name.as_ref().cmp(right.table_name.as_ref()));
        }

        let refreshed = CachedReplicationCatalog {
            loaded_at: Some(Instant::now()),
            table_configs,
            bootstrap_cursors_by_peer,
        };
        let mut cache = self.catalog_cache.write().await;
        *cache = refreshed.clone();
        Ok(refreshed)
    }

    async fn local_node_owns_outbound_replication(&self) -> StorageResult<bool> {
        let Some(reporter) = self.sync_health_reporter.as_ref() else {
            return Ok(true);
        };
        let health = reporter
            .sync_health()
            .await
            .map_err(|error| StorageError::internal(&format!("{error:?}")))?;
        let owns_sender = matches!(
            plan_multi_region_sender_ownership(&health.role),
            SyncMultiRegionSenderOwnershipDecision::OwnsSender
        );
        record_sync_multi_region_sender_owner(&health.role, owns_sender);
        Ok(owns_sender)
    }

    fn next_control_plane_proposal_id(
        &self,
        operation: &str,
    ) -> StorageResult<storage_sync::SyncProposalId> {
        let sequence = self
            .control_plane_proposal_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        storage_sync::SyncProposalId::new(format!(
            "multi-region-{operation}-{}-{sequence}",
            self.config.self_region
        ))
        .map_err(|error| StorageError::validation(error.to_string()))
    }

    async fn invalidate_catalog_cache(&self) {
        let mut cache = self.catalog_cache.write().await;
        cache.loaded_at = None;
        cache.table_configs.clear();
        cache.bootstrap_cursors_by_peer.clear();
    }
}

fn next_activation_cursor(
    checkpoint_cursor: Option<storage_types::StreamItemId>,
    cursor: &TableBootstrapCursorRecord,
) -> Option<storage_types::StreamItemId> {
    checkpoint_cursor
        .or(cursor.activation_cursor)
        .or(cursor.protected_stream_cursor)
        .or(cursor.last_system_stream_cursor)
}

fn logical_bootstrap_manifest(
    peer: &ReplicationPeerConfig,
    cursor: &TableBootstrapCursorRecord,
) -> StorageResult<LogicalBackfillManifest> {
    let manifest_id = cursor
        .logical_backfill_manifest_id
        .clone()
        .unwrap_or_else(|| format!("bootstrap#{}#{}", cursor.table_name, peer.region_name));
    let mut manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new(manifest_id)
            .map_err(|error| StorageError::validation(error.to_string()))?,
        &MultiRegionBootstrapPolicy,
        "storage-api",
        "storage-api",
        MULTI_REGION_BOOTSTRAP_LOGICAL_DOMAINS.to_vec(),
    );
    manifest.protected_stream_cursor = cursor.protected_stream_cursor.map(|value| value.to_hex());
    manifest.source_log_boundary = cursor.activation_cursor.map(|value| value.to_hex());
    Ok(manifest)
}

fn logical_bootstrap_domain(
    cursor: &TableBootstrapCursorRecord,
) -> StorageResult<LogicalBackfillDomain> {
    match cursor.logical_backfill_domain.as_deref() {
        None | Some("item_records") => Ok(LogicalBackfillDomain::ItemRecords),
        Some(unsupported) => Err(StorageError::validation(format!(
            "unsupported multi-region bootstrap logical backfill domain '{unsupported}'"
        ))),
    }
}

fn logical_domain_key(domain: LogicalBackfillDomain) -> &'static str {
    match domain {
        LogicalBackfillDomain::ItemRecords => "item_records",
        LogicalBackfillDomain::TableMetadata => "table_metadata",
        LogicalBackfillDomain::Tombstones => "tombstones",
        LogicalBackfillDomain::DurableRevisions => "durable_revisions",
        LogicalBackfillDomain::StreamRecords => "stream_records",
        LogicalBackfillDomain::TtlRecords => "ttl_records",
        LogicalBackfillDomain::GsiRecords => "gsi_records",
        LogicalBackfillDomain::StorageControlPlane => "storage_control_plane",
        LogicalBackfillDomain::BackgroundJobs => "background_jobs",
        LogicalBackfillDomain::SyncControlPlane => "sync_control_plane",
    }
}

fn logical_chunk_id(cursor: &TableBootstrapCursorRecord, domain: LogicalBackfillDomain) -> String {
    let checkpoint = cursor.logical_backfill_cursor.as_deref().unwrap_or("begin");
    format!(
        "{}#{}#{}#{}",
        cursor.table_name,
        cursor.peer_region,
        logical_domain_key(domain),
        checkpoint
    )
}

#[derive(Clone, Copy)]
pub(crate) struct MultiRegionClockOffsetSample {
    pub(crate) offset_estimate_ms: i64,
    pub(crate) uncertainty_ms: u64,
}

pub(crate) fn estimate_multi_region_clock_offset(
    sent_at: TimestampMillis,
    peer_received_at: TimestampMillis,
    peer_acknowledged_at: TimestampMillis,
    response_received_at: TimestampMillis,
) -> Option<MultiRegionClockOffsetSample> {
    let t0 = sent_at.timestamp_millis();
    let t1 = peer_received_at.timestamp_millis();
    let t2 = peer_acknowledged_at.timestamp_millis();
    let t3 = response_received_at.timestamp_millis();
    if t2 < t1 || t3 < t0 {
        return None;
    }

    let offset_estimate_ms = ((t1 - t0) + (t2 - t3)) / 2;
    let remote_processing_ms = t2 - t1;
    let local_round_trip_ms = t3 - t0;
    if local_round_trip_ms < remote_processing_ms {
        return None;
    }
    let network_delay_ms = local_round_trip_ms - remote_processing_ms;
    Some(MultiRegionClockOffsetSample {
        offset_estimate_ms,
        uncertainty_ms: u64::try_from((network_delay_ms + 1) / 2).unwrap_or(u64::MAX),
    })
}

fn record_multi_region_clock_offset_sample(
    peer_region: &str,
    sample: MultiRegionClockOffsetSample,
) {
    metrics::gauge!(
        "storage.multi.region.clock.offset.estimate.ms",
        "peer_region" => peer_region.to_string()
    )
    .set(sample.offset_estimate_ms as f64);
    metrics::gauge!(
        "storage.multi.region.clock.offset.abs.ms",
        "peer_region" => peer_region.to_string()
    )
    .set(sample.offset_estimate_ms.unsigned_abs() as f64);
    metrics::gauge!(
        "storage.multi.region.clock.offset.uncertainty.ms",
        "peer_region" => peer_region.to_string()
    )
    .set(sample.uncertainty_ms as f64);
}

fn record_sync_multi_region_sender_owner(role: &storage_sync::SyncRaftRole, owns_sender: bool) {
    metrics::gauge!(
        "storage.sync.multi_region.sender.owner",
        "role" => role.as_str()
    )
    .set(u8::from(owns_sender) as f64);
}

fn record_sync_multi_region_checkpoint_lag(peer_region: &str, lag_ms: u64) {
    metrics::gauge!(
        "storage.sync.multi_region.checkpoint.lag.ms",
        "peer_region" => peer_region.to_string()
    )
    .set(lag_ms as f64);
}

fn steady_state_tables_for_peer(
    table_configs: &[TableReplicationConfigRecord],
    peer_region: &str,
) -> HashSet<TableName> {
    table_configs
        .iter()
        .filter(|config| {
            replica_status_for_peer(config, peer_region).is_some_and(|status| {
                matches!(
                    status,
                    ReplicaStatus::Active | ReplicaStatus::Creating | ReplicaStatus::Updating
                )
            })
        })
        .map(|config| config.table_name.clone())
        .collect()
}

fn replica_status_for_peer(
    config: &TableReplicationConfigRecord,
    peer_region: &str,
) -> Option<ReplicaStatus> {
    config
        .replicas
        .iter()
        .find(|replica| replica.region_name == peer_region)
        .map(|replica| replica.replica_status.clone())
}

fn deterministic_jitter(peer_region: &str, generation: u64, max_jitter: Duration) -> Duration {
    let max_jitter_ms = max_jitter.as_millis();
    if max_jitter_ms == 0 {
        return Duration::ZERO;
    }

    let mut hasher = DefaultHasher::new();
    peer_region.hash(&mut hasher);
    generation.hash(&mut hasher);
    let jitter_ms = hasher.finish() % (max_jitter_ms as u64 + 1);
    Duration::from_millis(jitter_ms)
}

fn map_http_request_error(error: HttpRequestError) -> StorageError {
    match error {
        HttpRequestError::HttpStatus { status, body } if status == StatusCode::UNAUTHORIZED => {
            StorageError::Base(StorageEnum::Authentication {
                message: body
                    .unwrap_or_else(|| "replication peer rejected credentials".to_string()),
            })
        }
        HttpRequestError::HttpStatus { status, body } if status == StatusCode::FORBIDDEN => {
            StorageError::Base(StorageEnum::AccessDenied {
                message: body.unwrap_or_else(|| "replication peer denied access".to_string()),
            })
        }
        other => StorageError::internal(&other.to_string()),
    }
}

fn is_auth_error(error: &StorageError) -> bool {
    matches!(
        error.as_ref(),
        StorageEnum::Authentication { .. }
            | StorageEnum::AccessDenied { .. }
            | StorageEnum::MissingAuthenticationToken
    )
}
