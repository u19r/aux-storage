use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use config::{StorageReplicationConfig, StorageReplicationPeerConfig};
use http_error::HttpApiError;
use storage::{DatabaseManager, DeleteItemInput, PutItemInput, TableBootstrapCursorRecord, Tables};
use storage_sync::{SyncHealthResponse, SyncRaftRole};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, ReplicaStatus, ReplicationApplyRequest, ReplicationApplyResponse,
    ReplicationHeartbeatRequest, ReplicationHeartbeatResponse, StorageResult, StreamSpecification,
    StreamViewType, TableName, TimestampMillis,
};

use crate::{
    manager::{SyncHealthReporter, SyncWriteProposer},
    replication_logical_import::enforce_logical_backfill_import_preflight,
    replication_runtime::{
        ReplicationPeerClient, ReplicationPeerConfig, ReplicationRuntimeConfig,
        StorageReplicationRuntime, estimate_multi_region_clock_offset,
    },
    types::{ReplicationLogicalBackfillImportRequest, ReplicationLogicalBackfillImportResponse},
};

#[test]
fn heartbeat_clock_offset_estimate_uses_ntp_four_timestamp_formula() {
    let sample = estimate_multi_region_clock_offset(
        TimestampMillis::from_timestamp(1_000),
        TimestampMillis::from_timestamp(1_030),
        TimestampMillis::from_timestamp(1_035),
        TimestampMillis::from_timestamp(1_050),
    )
    .expect("valid clock sample");

    assert_eq!(sample.offset_estimate_ms, 7);
    assert_eq!(sample.uncertainty_ms, 23);
}

#[derive(Clone)]
struct RecordingPeerClient {
    destination: Arc<DatabaseManager>,
    fail_next_apply: Arc<AtomicBool>,
    fail_next_apply_with_auth: Arc<AtomicBool>,
    fail_next_logical_import_after_apply: Arc<AtomicBool>,
    apply_calls: Arc<AtomicUsize>,
    heartbeat_calls: Arc<AtomicUsize>,
    heartbeat_last_applied_commit_ts: Option<TimestampMillis>,
    heartbeat_region_name: Option<String>,
}

impl RecordingPeerClient {
    fn new(destination: Arc<DatabaseManager>) -> Self {
        Self {
            destination,
            fail_next_apply: Arc::new(AtomicBool::new(false)),
            fail_next_apply_with_auth: Arc::new(AtomicBool::new(false)),
            fail_next_logical_import_after_apply: Arc::new(AtomicBool::new(false)),
            apply_calls: Arc::new(AtomicUsize::new(0)),
            heartbeat_calls: Arc::new(AtomicUsize::new(0)),
            heartbeat_last_applied_commit_ts: None,
            heartbeat_region_name: None,
        }
    }

    fn with_heartbeat_last_applied_commit_ts(
        mut self,
        heartbeat_last_applied_commit_ts: TimestampMillis,
    ) -> Self {
        self.heartbeat_last_applied_commit_ts = Some(heartbeat_last_applied_commit_ts);
        self
    }

    fn with_heartbeat_region_name(mut self, heartbeat_region_name: impl Into<String>) -> Self {
        self.heartbeat_region_name = Some(heartbeat_region_name.into());
        self
    }
}

#[async_trait]
impl ReplicationPeerClient for RecordingPeerClient {
    async fn apply(
        &self,
        _peer: &ReplicationPeerConfig,
        request: &ReplicationApplyRequest,
    ) -> StorageResult<ReplicationApplyResponse> {
        if self.fail_next_apply_with_auth.swap(false, Ordering::SeqCst) {
            return Err(storage_types::StorageError::Base(
                storage_types::StorageEnum::Authentication {
                    message: "synthetic auth failure".to_string(),
                },
            ));
        }
        if self.fail_next_apply.swap(false, Ordering::SeqCst) {
            return Err(storage_types::StorageError::internal(
                "synthetic replication apply failure",
            ));
        }
        self.apply_calls.fetch_add(1, Ordering::SeqCst);

        let outcomes = self
            .destination
            .apply_replication_mutations_with_outcomes(request.mutations.clone())
            .await?;
        let applied_mutations = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, storage::ReplicationMutationApplyOutcome::Applied))
            .count();
        let skipped_mutations = outcomes.len().saturating_sub(applied_mutations);

        Ok(ReplicationApplyResponse {
            received_mutations: applied_mutations + skipped_mutations,
            applied_mutations,
            skipped_mutations,
        })
    }

    async fn heartbeat(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationHeartbeatRequest,
    ) -> StorageResult<ReplicationHeartbeatResponse> {
        self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReplicationHeartbeatResponse {
            region_name: self
                .heartbeat_region_name
                .clone()
                .unwrap_or_else(|| peer.region_name.clone()),
            received_at: request.sent_at + 4,
            acknowledged_at: request.sent_at + 4,
            last_applied_commit_ts: self.heartbeat_last_applied_commit_ts,
        })
    }

    async fn import_logical_backfill(
        &self,
        _peer: &ReplicationPeerConfig,
        request: &ReplicationLogicalBackfillImportRequest,
    ) -> StorageResult<ReplicationLogicalBackfillImportResponse> {
        enforce_logical_backfill_import_preflight(self.destination.as_ref(), request).await?;
        let result = self
            .destination
            .import_logical_backfill_chunk(&request.manifest, request.chunk.clone())
            .await?;
        if self
            .fail_next_logical_import_after_apply
            .swap(false, Ordering::SeqCst)
        {
            return Err(storage_types::StorageError::internal(
                "synthetic logical import checkpoint failure",
            ));
        }
        Ok(ReplicationLogicalBackfillImportResponse { result })
    }
}

#[derive(Debug)]
struct StaticSyncHealthReporter {
    role: SyncRaftRole,
}

#[async_trait]
impl SyncHealthReporter for StaticSyncHealthReporter {
    async fn sync_health(&self) -> Result<SyncHealthResponse, HttpApiError> {
        let mut health = SyncHealthResponse::disabled();
        health.role = self.role.clone();
        Ok(health)
    }
}

#[derive(Debug, Default)]
struct RecordingSyncWriteProposer {
    requests: Mutex<Vec<storage_sync::SyncWriteProposalRequest>>,
}

impl RecordingSyncWriteProposer {
    fn requests(&self) -> Vec<storage_sync::SyncWriteProposalRequest> {
        self.requests.lock().expect("requests mutex").clone()
    }
}

#[async_trait]
impl SyncWriteProposer for RecordingSyncWriteProposer {
    async fn propose_sync_write(
        &self,
        request: storage_sync::SyncWriteProposalRequest,
    ) -> Result<storage_sync::SyncProposalResponse, HttpApiError> {
        let proposal_id = request.proposal_id.clone();
        self.requests.lock().expect("requests mutex").push(request);
        Ok(storage_sync::SyncProposalResponse::new(
            proposal_id,
            vec![storage_sync::SyncMutationResponse::default()],
        ))
    }
}

fn runtime_config(peer_region: &str) -> (ReplicationRuntimeConfig, ReplicationPeerConfig) {
    let peer = ReplicationPeerConfig {
        region_name: peer_region.to_string(),
        endpoint_url: "http://replica.internal".to_string(),
        service_token: "token".to_string(),
    };
    let config = ReplicationRuntimeConfig {
        self_region: "region-a".to_string(),
        poll_interval: std::time::Duration::from_millis(10),
        heartbeat_interval: std::time::Duration::from_secs(10),
        heartbeat_jitter: std::time::Duration::ZERO,
        batch_mutation_limit: 1_000,
        batch_byte_limit: 512 * 1024,
        peers: vec![peer.clone()],
    };
    (config, peer)
}

async fn create_test_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            BillingMode::PayPerRequest,
        )
        .with_stream_specification(Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        })),
    )
    .await
    .expect("create test table");
}

async fn put_test_item(db: &DatabaseManager, table_name: &TableName, value: &str) {
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("pk1".to_string())),
                ("sk".to_string(), AttributeValue::S("sk1".to_string())),
                ("value".to_string(), AttributeValue::S(value.to_string())),
            ]))
            .build(),
    )
    .await
    .expect("put test item");
}

fn item_key() -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("pk1".to_string())),
        ("sk".to_string(), AttributeValue::S("sk1".to_string())),
    ])
}

fn bootstrap_cursor(table_name: &TableName) -> TableBootstrapCursorRecord {
    TableBootstrapCursorRecord {
        table_name: table_name.clone(),
        peer_region: "region-b".to_string(),
        protected_stream_cursor: None,
        last_system_stream_cursor: None,
        activation_cursor: None,
        session_started_at: Some(TimestampMillis::now()),
        logical_backfill_manifest_id: None,
        logical_backfill_domain: None,
        logical_backfill_cursor: None,
        updated_at: TimestampMillis::now(),
    }
}

#[tokio::test]
async fn sync_runtime_persists_peer_checkpoint_through_sync_proposer() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    Tables::create_sys_storage_replication_table(source.as_ref())
        .await
        .expect("ensure control-plane table");
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let peer_client = Arc::new(RecordingPeerClient::new(destination));
    let sync_proposer = Arc::new(RecordingSyncWriteProposer::default());
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client)
        .with_sync_write_proposer(sync_proposer.clone());

    runtime
        .persist_peer_checkpoint(&peer, None)
        .await
        .expect("persist checkpoint through sync proposer");

    let requests = sync_proposer.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        matches!(
            requests[0].request,
            storage_sync::SyncWriteRequest::PutItem(_)
        ),
        "checkpoint persistence should enter the sync log as a PutItem"
    );
    assert!(
        source
            .get_peer_checkpoint("region-b")
            .await
            .expect("load local checkpoint")
            .is_none(),
        "sync mode checkpoint writes must not bypass the sync proposer with a local write"
    );
}

#[tokio::test]
async fn sync_follower_runtime_does_not_run_outbound_sender() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_sync_follower");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");
    put_test_item(source.as_ref(), &table_name, "follower-should-not-send").await;

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client.clone())
        .with_sync_health_reporter(Arc::new(StaticSyncHealthReporter {
            role: SyncRaftRole::Follower,
        }));

    let made_progress = runtime
        .run_peer_once(&peer, true)
        .await
        .expect("run sync follower replication pass");

    assert!(
        !made_progress,
        "sync followers must not run outbound multi-region senders"
    );
    assert_eq!(
        peer_client.heartbeat_calls.load(Ordering::SeqCst),
        0,
        "sync followers must not send replication heartbeats"
    );
    assert_eq!(
        peer_client.apply_calls.load(Ordering::SeqCst),
        0,
        "sync followers must not send replication batches"
    );
    assert!(
        destination
            .get_item_map(table_name.clone(), item_key())
            .await
            .expect("read destination item")
            .is_none(),
        "sync followers must not apply outbound replication batches"
    );
    assert!(
        source
            .get_peer_checkpoint("region-b")
            .await
            .expect("load checkpoint")
            .is_none(),
        "sync followers must not advance outbound checkpoints"
    );
}

#[tokio::test]
async fn sync_leader_runtime_runs_outbound_sender() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_sync_leader");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client.clone())
        .with_sync_health_reporter(Arc::new(StaticSyncHealthReporter {
            role: SyncRaftRole::Leader,
        }));

    runtime
        .run_peer_once(&peer, true)
        .await
        .expect("run sync leader replication pass");

    assert_eq!(
        peer_client.heartbeat_calls.load(Ordering::SeqCst),
        1,
        "sync leaders should send outbound replication heartbeats"
    );
}

#[tokio::test]
async fn sync_new_leader_resumes_from_replicated_sender_checkpoint() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_sync_failover_checkpoint");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");
    put_test_item(
        source.as_ref(),
        &table_name,
        "already-replicated-before-failover",
    )
    .await;
    let replicated_cursor = source
        .latest_system_stream_cursor()
        .await
        .expect("read latest system cursor");
    source
        .put_peer_checkpoint(&storage::PeerCheckpointRecord {
            peer_region: "region-b".to_string(),
            last_system_stream_cursor: replicated_cursor,
            updated_at: TimestampMillis::now(),
        })
        .await
        .expect("seed replicated checkpoint");

    let peer_client = Arc::new(RecordingPeerClient::new(destination));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client.clone())
        .with_sync_health_reporter(Arc::new(StaticSyncHealthReporter {
            role: SyncRaftRole::Leader,
        }));

    let made_progress = runtime
        .run_peer_once(&peer, false)
        .await
        .expect("new leader replication pass");

    assert!(
        !made_progress,
        "new leader should not replay records at or before the replicated checkpoint"
    );
    assert_eq!(
        peer_client.apply_calls.load(Ordering::SeqCst),
        0,
        "new leader must not duplicate already-checkpointed outbound batches"
    );
}

#[test]
fn replication_runtime_config_from_settings_returns_none_when_disabled() {
    let settings = StorageReplicationConfig::default();

    let config = ReplicationRuntimeConfig::from_settings(&settings).expect("disabled config");

    assert!(config.is_none());
}

#[test]
fn replication_runtime_config_from_settings_trims_and_normalizes_peer_values() {
    let settings = StorageReplicationConfig {
        enabled: true,
        self_region: Some("  region-a  ".to_string()),
        poll_interval_ms: 250,
        heartbeat_interval_ms: 1_500,
        heartbeat_jitter_ms: 125,
        batch_mutation_limit: 25,
        batch_byte_limit: 8_192,
        peers: vec![StorageReplicationPeerConfig {
            region_name: " region-b ".to_string(),
            endpoint_url: " https://replica.internal/ ".to_string(),
            service_token: " token-123 ".to_string(),
        }],
    };

    let config = ReplicationRuntimeConfig::from_settings(&settings)
        .expect("enabled config")
        .expect("runtime config");

    assert_eq!(config.self_region, "region-a");
    assert_eq!(config.poll_interval, std::time::Duration::from_millis(250));
    assert_eq!(
        config.heartbeat_interval,
        std::time::Duration::from_millis(1_500)
    );
    assert_eq!(
        config.heartbeat_jitter,
        std::time::Duration::from_millis(125)
    );
    assert_eq!(config.batch_mutation_limit, 25);
    assert_eq!(config.batch_byte_limit, 8_192);
    assert_eq!(config.peers.len(), 1);
    assert_eq!(config.peers[0].region_name, "region-b");
    assert_eq!(config.peers[0].endpoint_url, "https://replica.internal");
    assert_eq!(config.peers[0].service_token, "token-123");
}

#[test]
fn replication_runtime_config_from_settings_rejects_self_peers_and_duplicate_regions() {
    let self_peer_settings = StorageReplicationConfig {
        enabled: true,
        self_region: Some("region-a".to_string()),
        poll_interval_ms: 250,
        heartbeat_interval_ms: 1_000,
        heartbeat_jitter_ms: 100,
        batch_mutation_limit: 10,
        batch_byte_limit: 4_096,
        peers: vec![StorageReplicationPeerConfig {
            region_name: "region-a".to_string(),
            endpoint_url: "https://replica-a.internal".to_string(),
            service_token: "token-a".to_string(),
        }],
    };
    let duplicate_peer_settings = StorageReplicationConfig {
        enabled: true,
        self_region: Some("region-a".to_string()),
        poll_interval_ms: 250,
        heartbeat_interval_ms: 1_000,
        heartbeat_jitter_ms: 100,
        batch_mutation_limit: 10,
        batch_byte_limit: 4_096,
        peers: vec![
            StorageReplicationPeerConfig {
                region_name: "region-b".to_string(),
                endpoint_url: "https://replica-b.internal".to_string(),
                service_token: "token-b".to_string(),
            },
            StorageReplicationPeerConfig {
                region_name: "region-b".to_string(),
                endpoint_url: "https://replica-b-2.internal".to_string(),
                service_token: "token-b-2".to_string(),
            },
        ],
    };

    let self_peer_error = ReplicationRuntimeConfig::from_settings(&self_peer_settings)
        .expect_err("self peer should be rejected");
    let duplicate_error = ReplicationRuntimeConfig::from_settings(&duplicate_peer_settings)
        .expect_err("duplicate peer regions should be rejected");

    assert!(
        self_peer_error
            .to_string()
            .contains("must not include self_region")
    );
    assert!(
        duplicate_error
            .to_string()
            .contains("contains a duplicate region 'region-b'")
    );
}

#[tokio::test]
async fn steady_state_runtime_replicates_local_origin_mutations() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_steady_state");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");

    put_test_item(source.as_ref(), &table_name, "steady").await;

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);
    let made_progress = runtime
        .run_peer_once(&peer, false)
        .await
        .expect("run steady-state replication pass");

    assert!(made_progress, "steady-state pass should replicate data");
    let replicated = destination
        .get_item_map(table_name.clone(), item_key())
        .await
        .expect("read replicated item")
        .expect("replicated item should exist");
    assert_eq!(
        replicated
            .get("value")
            .expect("value attr")
            .inner_str()
            .expect("string value"),
        "steady"
    );

    let checkpoint = source
        .get_peer_checkpoint("region-b")
        .await
        .expect("load checkpoint")
        .expect("checkpoint should exist");
    assert!(
        checkpoint.last_system_stream_cursor.is_some(),
        "checkpoint should advance after a successful apply"
    );
}

#[tokio::test]
async fn steady_state_runtime_burst_drains_multiple_batches_without_sleeping_between_passes() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_burst_drain");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");

    for value in ["v1", "v2", "v3"] {
        source
            .put_item(
                PutItemInput::builder()
                    .table_name(table_name.clone())
                    .item(HashMap::from([
                        ("pk".to_string(), AttributeValue::S(value.to_string())),
                        ("sk".to_string(), AttributeValue::S(value.to_string())),
                        ("value".to_string(), AttributeValue::S(value.to_string())),
                    ]))
                    .build(),
            )
            .await
            .expect("put test item");
    }

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (mut config, peer) = runtime_config("region-b");
    config.batch_mutation_limit = 1;
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client.clone());

    let made_progress = runtime
        .run_peer_burst(&peer, 8)
        .await
        .expect("run steady-state burst");

    assert!(made_progress, "burst should make progress");
    assert_eq!(
        peer_client.heartbeat_calls.load(Ordering::SeqCst),
        0,
        "burst helper should not emit heartbeats"
    );
    for value in ["v1", "v2", "v3"] {
        let replicated = destination
            .get_item_map(
                table_name.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S(value.to_string())),
                    ("sk".to_string(), AttributeValue::S(value.to_string())),
                ]),
            )
            .await
            .expect("read replicated item");
        assert!(
            replicated.is_some(),
            "burst should drain all pending single-item batches"
        );
    }
}

#[tokio::test]
async fn steady_state_runtime_preserves_checkpoint_on_apply_failure() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_failure_resume");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");
    put_test_item(source.as_ref(), &table_name, "retry").await;

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    peer_client.fail_next_apply.store(true, Ordering::SeqCst);
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client.clone());

    let first_result = runtime.run_peer_once(&peer, false).await;
    assert!(first_result.is_err(), "first pass should fail");
    assert!(
        source
            .get_peer_checkpoint("region-b")
            .await
            .expect("load checkpoint")
            .is_none(),
        "checkpoint must not advance when apply fails"
    );

    let second_result = runtime
        .run_peer_once(&peer, false)
        .await
        .expect("second pass should succeed");
    assert!(second_result, "retry pass should make progress");
    let destination_item = destination
        .get_item_map(table_name.clone(), item_key())
        .await
        .expect("load destination item");
    assert!(
        destination_item.is_some(),
        "item should replicate after retry"
    );
}

#[tokio::test]
async fn bootstrap_cursor_runtime_promotes_replica_after_catchup() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_bootstrap");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .put_table_bootstrap_cursor(&TableBootstrapCursorRecord {
            table_name: table_name.clone(),
            peer_region: "region-b".to_string(),
            protected_stream_cursor: None,
            last_system_stream_cursor: None,
            activation_cursor: None,
            session_started_at: Some(TimestampMillis::now()),
            logical_backfill_manifest_id: None,
            logical_backfill_domain: None,
            logical_backfill_cursor: None,
            updated_at: TimestampMillis::now(),
        })
        .await
        .expect("seed bootstrap cursor");
    put_test_item(source.as_ref(), &table_name, "bootstrap").await;

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);
    let logical_progress = runtime
        .run_peer_once(&peer, false)
        .await
        .expect("run logical bootstrap pass");
    assert!(logical_progress, "logical bootstrap should make progress");
    let intermediate_cursor = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load intermediate bootstrap cursor")
        .expect("cursor remains until stream drain");
    assert_eq!(
        intermediate_cursor.logical_backfill_cursor.as_deref(),
        Some("__complete__")
    );
    let intermediate_config = source
        .get_table_replication_config(&table_name)
        .await
        .expect("load intermediate table config")
        .expect("table config should exist");
    let intermediate_status = intermediate_config
        .replicas
        .iter()
        .find(|replica| replica.region_name == "region-b")
        .map(|replica| replica.replica_status.clone());
    assert_eq!(intermediate_status, Some(ReplicaStatus::Creating));

    let made_progress = runtime
        .run_peer_once(&peer, false)
        .await
        .expect("run stream drain bootstrap pass");

    assert!(made_progress, "bootstrap pass should replay and promote");
    assert!(
        source
            .get_table_bootstrap_cursor(&table_name, "region-b")
            .await
            .expect("load bootstrap cursor")
            .is_none(),
        "bootstrap cursor should be deleted after catchup"
    );
    let config = source
        .get_table_replication_config(&table_name)
        .await
        .expect("load table config")
        .expect("table config should exist");
    let replica_status = config
        .replicas
        .iter()
        .find(|replica| replica.region_name == "region-b")
        .map(|replica| replica.replica_status.clone());
    assert_eq!(replica_status, Some(ReplicaStatus::Active));
}

#[tokio::test]
async fn logical_backfill_chunk_transfer_imports_page_and_persists_checkpoint() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_logical_bootstrap");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    put_test_item(source.as_ref(), &table_name, "logical").await;

    let now = TimestampMillis::now();
    let cursor = TableBootstrapCursorRecord {
        table_name: table_name.clone(),
        peer_region: "region-b".to_string(),
        protected_stream_cursor: None,
        last_system_stream_cursor: None,
        activation_cursor: None,
        session_started_at: Some(now),
        logical_backfill_manifest_id: None,
        logical_backfill_domain: None,
        logical_backfill_cursor: None,
        updated_at: now,
    };
    source
        .put_table_bootstrap_cursor(&cursor)
        .await
        .expect("seed bootstrap cursor");
    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    let made_progress = runtime
        .transfer_logical_backfill_chunk_for_cursor(&peer, &cursor)
        .await
        .expect("transfer logical chunk");

    assert!(made_progress);
    let imported = destination
        .get_item_map(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("pk1".to_string())),
                ("sk".to_string(), AttributeValue::S("sk1".to_string())),
            ]),
        )
        .await
        .expect("read imported item")
        .expect("item imported");
    assert_eq!(
        imported.get("value"),
        Some(&AttributeValue::S("logical".to_string()))
    );
    let checkpoint = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load checkpoint")
        .expect("checkpoint remains");
    assert!(checkpoint.logical_backfill_manifest_id.is_some());
    assert_eq!(
        checkpoint.logical_backfill_domain.as_deref(),
        Some("item_records")
    );
    assert_eq!(
        checkpoint.logical_backfill_cursor.as_deref(),
        Some("__complete__")
    );
}

#[tokio::test]
async fn logical_backfill_empty_table_persists_completion_checkpoint() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_logical_empty_bootstrap");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;

    let now = TimestampMillis::now();
    let cursor = TableBootstrapCursorRecord {
        table_name: table_name.clone(),
        peer_region: "region-b".to_string(),
        protected_stream_cursor: None,
        last_system_stream_cursor: None,
        activation_cursor: None,
        session_started_at: Some(now),
        logical_backfill_manifest_id: None,
        logical_backfill_domain: None,
        logical_backfill_cursor: None,
        updated_at: now,
    };
    source
        .put_table_bootstrap_cursor(&cursor)
        .await
        .expect("seed bootstrap cursor");
    let peer_client = Arc::new(RecordingPeerClient::new(destination));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    assert!(
        runtime
            .transfer_logical_backfill_chunk_for_cursor(&peer, &cursor)
            .await
            .expect("transfer empty logical chunk")
    );
    let checkpoint = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load checkpoint")
        .expect("checkpoint remains");
    assert_eq!(
        checkpoint.logical_backfill_cursor.as_deref(),
        Some("__complete__")
    );
}

#[tokio::test]
async fn logical_backfill_rejects_non_empty_destination_before_preflight() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_logical_non_empty_reject");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    put_test_item(source.as_ref(), &table_name, "source").await;
    put_test_item(destination.as_ref(), &table_name, "pre-existing").await;

    let cursor = bootstrap_cursor(&table_name);
    source
        .put_table_bootstrap_cursor(&cursor)
        .await
        .expect("seed bootstrap cursor");
    let peer_client = Arc::new(RecordingPeerClient::new(destination));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    let error = runtime
        .transfer_logical_backfill_chunk_for_cursor(&peer, &cursor)
        .await
        .expect_err("non-empty destination should reject bootstrap preflight");
    assert!(
        error
            .to_string()
            .contains("logical bootstrap destination is not empty"),
        "unexpected error: {error:?}"
    );
    let checkpoint = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load source cursor")
        .expect("cursor remains");
    assert!(
        checkpoint.logical_backfill_cursor.is_none(),
        "source must not checkpoint a rejected destination"
    );
}

#[tokio::test]
async fn logical_backfill_retry_after_partial_import_uses_destination_preflight_marker() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_logical_partial_retry");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    put_test_item(source.as_ref(), &table_name, "logical").await;

    let cursor = bootstrap_cursor(&table_name);
    source
        .put_table_bootstrap_cursor(&cursor)
        .await
        .expect("seed bootstrap cursor");
    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    peer_client
        .fail_next_logical_import_after_apply
        .store(true, Ordering::SeqCst);
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    let first = runtime
        .transfer_logical_backfill_chunk_for_cursor(&peer, &cursor)
        .await;
    assert!(
        first.is_err(),
        "first import simulates lost source checkpoint"
    );
    let source_checkpoint = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load source cursor")
        .expect("cursor remains");
    assert!(
        source_checkpoint.logical_backfill_cursor.is_none(),
        "source checkpoint should remain unadvanced after synthetic failure"
    );

    let retried = runtime
        .transfer_logical_backfill_chunk_for_cursor(&peer, &source_checkpoint)
        .await
        .expect("retry after partial import should use destination preflight marker");
    assert!(retried);
    let checkpoint = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load checkpoint")
        .expect("checkpoint remains");
    assert_eq!(
        checkpoint.logical_backfill_cursor.as_deref(),
        Some("__complete__")
    );
    let imported = destination
        .get_item_map(table_name, item_key())
        .await
        .expect("read destination item")
        .expect("item imported");
    assert_eq!(
        imported.get("value"),
        Some(&AttributeValue::S("logical".to_string()))
    );
}

#[tokio::test]
async fn bootstrap_after_existing_source_history_uses_logical_backfill() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_bootstrap_trimmed_source");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    put_test_item(source.as_ref(), &table_name, "before-replica").await;
    let existing_history_cursor = source
        .latest_system_stream_cursor()
        .await
        .expect("read source cursor")
        .expect("source stream cursor");

    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    let bootstrap_cursor = source
        .get_table_bootstrap_cursor(&table_name, "region-b")
        .await
        .expect("load bootstrap cursor")
        .expect("bootstrap cursor");
    assert_eq!(
        bootstrap_cursor.last_system_stream_cursor,
        Some(existing_history_cursor),
        "bootstrap starts after pre-existing source stream history"
    );

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);
    runtime
        .run_peer_burst(&peer, 2)
        .await
        .expect("bootstrap after existing source history");

    let imported = destination
        .get_item_map(
            table_name,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("pk1".to_string())),
                ("sk".to_string(), AttributeValue::S("sk1".to_string())),
            ]),
        )
        .await
        .expect("read imported item")
        .expect("logical backfill imports item that stream catchup starts after");
    assert_eq!(
        imported.get("value"),
        Some(&AttributeValue::S("before-replica".to_string()))
    );
}

#[tokio::test]
async fn bootstrap_stream_drain_applies_source_write_after_logical_scan() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_bootstrap_during_put");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    put_test_item(source.as_ref(), &table_name, "before-bootstrap").await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);
    runtime
        .run_peer_once(&peer, false)
        .await
        .expect("logical bootstrap pass");
    put_test_item(source.as_ref(), &table_name, "during-bootstrap").await;
    runtime
        .run_peer_once(&peer, false)
        .await
        .expect("stream drain pass");

    let imported = destination
        .get_item_map(table_name, item_key())
        .await
        .expect("read destination item")
        .expect("destination item");
    assert_eq!(
        imported.get("value"),
        Some(&AttributeValue::S("during-bootstrap".to_string()))
    );
}

#[tokio::test]
async fn bootstrap_stream_drain_applies_source_delete_after_logical_scan() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_bootstrap_during_delete");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    put_test_item(source.as_ref(), &table_name, "before-bootstrap").await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");

    let peer_client = Arc::new(RecordingPeerClient::new(destination.clone()));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);
    runtime
        .run_peer_once(&peer, false)
        .await
        .expect("logical bootstrap pass");
    source
        .delete_item(
            DeleteItemInput::builder()
                .table_name(table_name.clone())
                .key(item_key())
                .build(),
        )
        .await
        .expect("delete during bootstrap");
    runtime
        .run_peer_once(&peer, false)
        .await
        .expect("stream drain pass");

    let imported = destination
        .get_item_map(table_name, item_key())
        .await
        .expect("read destination item");
    assert!(imported.is_none(), "delete during bootstrap should win");
}

#[tokio::test]
async fn runtime_sends_heartbeat_when_requested() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    Tables::create_sys_storage_replication_table(source.as_ref())
        .await
        .expect("ensure control-plane table");
    let peer_client = Arc::new(RecordingPeerClient::new(destination));
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client.clone());

    let made_progress = runtime
        .run_peer_once(&peer, true)
        .await
        .expect("run heartbeat-only pass");

    assert!(
        !made_progress,
        "heartbeat-only pass should not report replication progress without replicated tables"
    );
    assert_eq!(peer_client.heartbeat_calls.load(Ordering::SeqCst), 1);
    let status = source
        .get_peer_replication_status("region-b")
        .await
        .expect("load peer status")
        .expect("peer status should exist");
    assert_eq!(status.last_heartbeat_rtt_ms, Some(0));
    assert!(status.clock_offset_estimate_ms.is_some());
    assert!(status.clock_offset_uncertainty_ms.is_some());
}

#[tokio::test]
async fn runtime_heartbeat_tracks_peer_applied_watermark() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    Tables::create_sys_storage_replication_table(source.as_ref())
        .await
        .expect("ensure control-plane table");
    let peer_client = Arc::new(
        RecordingPeerClient::new(Arc::new(
            DatabaseManager::new_for_test()
                .await
                .expect("destination db"),
        ))
        .with_heartbeat_last_applied_commit_ts(TimestampMillis::from_timestamp(2_000_000_000_000)),
    );
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    runtime
        .run_peer_once(&peer, true)
        .await
        .expect("run heartbeat-only pass");

    let status = source
        .get_peer_replication_status("region-b")
        .await
        .expect("load peer status")
        .expect("peer status should exist");
    assert_eq!(
        status.last_remote_applied_commit_ts,
        Some(TimestampMillis::from_timestamp(2_000_000_000_000))
    );
}

#[tokio::test]
async fn runtime_heartbeat_rejects_responses_that_claim_a_different_peer_region() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    Tables::create_sys_storage_replication_table(source.as_ref())
        .await
        .expect("ensure control-plane table");
    let peer_client = Arc::new(
        RecordingPeerClient::new(Arc::new(
            DatabaseManager::new_for_test()
                .await
                .expect("destination db"),
        ))
        .with_heartbeat_region_name("region-c")
        .with_heartbeat_last_applied_commit_ts(TimestampMillis::from_timestamp(2_000_000_000_000)),
    );
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    let result = runtime.run_peer_once(&peer, true).await;
    assert!(
        result.is_err(),
        "heartbeat should fail closed when a peer claims the wrong region"
    );

    let status = source
        .get_peer_replication_status("region-b")
        .await
        .expect("load peer status");
    assert!(
        status.is_none(),
        "mismatched heartbeat identity must not poison the intended peer status row"
    );
}

#[tokio::test]
async fn steady_state_runtime_records_auth_failures() {
    let source = Arc::new(DatabaseManager::new_for_test().await.expect("source db"));
    let destination = Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("destination db"),
    );
    let table_name = TableName::new("replication_runtime_auth_failure");
    create_test_table(source.as_ref(), &table_name).await;
    create_test_table(destination.as_ref(), &table_name).await;
    source
        .apply_replica_updates(
            &table_name,
            &[storage_types::ReplicaUpdate {
                create: Some(storage_types::CreateReplicaAction {
                    region_name: "region-b".to_string(),
                }),
                update: None,
                delete: None,
            }],
        )
        .await
        .expect("create replica config");
    source
        .mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");
    put_test_item(source.as_ref(), &table_name, "retry").await;

    let peer_client = Arc::new(RecordingPeerClient::new(destination));
    peer_client
        .fail_next_apply_with_auth
        .store(true, Ordering::SeqCst);
    let (config, peer) = runtime_config("region-b");
    let runtime = StorageReplicationRuntime::new(source.clone(), config, peer_client);

    let result = runtime.run_peer_once(&peer, false).await;
    assert!(result.is_err(), "auth failure should fail the pass");

    let status = source
        .get_peer_replication_status("region-b")
        .await
        .expect("load peer status")
        .expect("peer status should exist");
    assert!(status.last_auth_failure_at.is_some());
}
