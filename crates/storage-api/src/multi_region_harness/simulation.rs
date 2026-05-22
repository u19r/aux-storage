use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use storage::{DatabaseManager, DeleteItemInput, PutItemInput, TableBootstrapCursorRecord};
use storage_types::{
    CreateReplicaAction, ReplicaUpdate, StorageError, StorageResult, TableName, TimestampMillis,
};

use crate::{
    multi_region_harness::{
        simulation_network::{
            LinkFaultProfile, SimulationNetworkState, lock_simulation_network, sleep_if_needed,
        },
        simulation_peer::SimulationPeerClient,
        simulation_storage::{build_region_databases, create_stream_table, item, item_key},
    },
    replication_runtime::{ReplicationPeerConfig, ReplicationRuntimeConfig},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationStorageBackend {
    Sqlite,
    Turso,
    Postgres,
    Rocksdb,
    Foundationdb,
}

impl SimulationStorageBackend {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Turso => "turso",
            Self::Postgres => "postgres",
            Self::Rocksdb => "rocksdb",
            Self::Foundationdb => "foundationdb",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimulationHarnessConfig {
    pub region_names: Vec<String>,
    pub single_node_sync_regions: Vec<String>,
    pub storage_backend: SimulationStorageBackend,
    pub region_storage_backends: Vec<SimulationStorageBackend>,
    pub sqlite_database_dir: Option<PathBuf>,
    pub postgres_dsn_template: Option<String>,
    pub postgres_max_pool_size: usize,
    pub postgres_tls: bool,
    pub foundationdb_cluster_file: Option<String>,
    pub foundationdb_subspace_prefix: Option<String>,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_jitter: Duration,
    pub batch_mutation_limit: usize,
    pub batch_byte_limit: usize,
    pub link_latency: Duration,
    pub link_latency_jitter: Duration,
    pub heartbeat_latency: Duration,
    pub heartbeat_latency_jitter: Duration,
    pub drop_probability_per_10k: u16,
    pub duplicate_probability_per_10k: u16,
    pub queue_probability_per_10k: u16,
    pub emulated_clock_skew_ms: u64,
    pub seed: u64,
}

impl Default for SimulationHarnessConfig {
    fn default() -> Self {
        Self {
            region_names: vec!["region-a".to_string(), "region-b".to_string()],
            single_node_sync_regions: Vec::new(),
            storage_backend: SimulationStorageBackend::Sqlite,
            region_storage_backends: Vec::new(),
            sqlite_database_dir: None,
            postgres_dsn_template: None,
            postgres_max_pool_size: 16,
            postgres_tls: false,
            foundationdb_cluster_file: None,
            foundationdb_subspace_prefix: None,
            poll_interval: Duration::from_millis(5),
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_jitter: Duration::ZERO,
            batch_mutation_limit: 1_000,
            batch_byte_limit: 512 * 1024,
            link_latency: Duration::ZERO,
            link_latency_jitter: Duration::ZERO,
            heartbeat_latency: Duration::ZERO,
            heartbeat_latency_jitter: Duration::ZERO,
            drop_probability_per_10k: 0,
            duplicate_probability_per_10k: 0,
            queue_probability_per_10k: 0,
            emulated_clock_skew_ms: 0,
            seed: 1,
        }
    }
}

#[derive(Clone)]
pub(super) struct SimulationRegion {
    pub(super) db: Arc<DatabaseManager>,
    pub(super) config: ReplicationRuntimeConfig,
    pub(super) client: Arc<SimulationPeerClient>,
    pub(super) sync_role: Option<storage_sync::SyncRaftRole>,
}

#[derive(Clone)]
pub struct SimulationHarness {
    pub(super) regions: HashMap<String, SimulationRegion>,
    pub(super) network: Arc<Mutex<SimulationNetworkState>>,
    pub(super) region_order: Vec<String>,
    emulated_clock_skew_ms: u64,
}

impl SimulationHarness {
    pub async fn new(config: SimulationHarnessConfig) -> StorageResult<Self> {
        let region_dbs = build_region_databases(&config).await?;
        Ok(Self::from_databases(region_dbs, config))
    }

    pub fn from_databases(
        region_dbs: HashMap<String, Arc<DatabaseManager>>,
        config: SimulationHarnessConfig,
    ) -> Self {
        let mut region_names = region_dbs.keys().cloned().collect::<Vec<_>>();
        region_names.sort();

        let network = Arc::new(Mutex::new(SimulationNetworkState::with_seed(config.seed)));
        let mut regions = HashMap::new();
        for origin_region in &region_names {
            let mut peers = Vec::new();
            for peer_region in &region_names {
                if peer_region == origin_region {
                    continue;
                }
                let service_token = format!("{origin_region}-token-v1");
                peers.push(ReplicationPeerConfig {
                    region_name: peer_region.clone(),
                    endpoint_url: "http://simulation".to_string(),
                    service_token: service_token.clone(),
                });
                let mut lock = lock_simulation_network(&network);
                let link = lock
                    .links
                    .entry((origin_region.clone(), peer_region.clone()))
                    .or_default();
                link.accepted_tokens.insert(service_token);
                link.profile = LinkFaultProfile {
                    apply_latency: config.link_latency,
                    apply_latency_jitter: config.link_latency_jitter,
                    heartbeat_latency: config.heartbeat_latency,
                    heartbeat_latency_jitter: config.heartbeat_latency_jitter,
                    drop_probability_per_10k: config.drop_probability_per_10k,
                    duplicate_probability_per_10k: config.duplicate_probability_per_10k,
                    queue_probability_per_10k: config.queue_probability_per_10k,
                };
            }

            let client = Arc::new(SimulationPeerClient {
                origin_region: origin_region.clone(),
                regions: region_dbs.clone(),
                network: Arc::clone(&network),
            });
            regions.insert(
                origin_region.clone(),
                SimulationRegion {
                    db: region_dbs.get(origin_region).cloned().unwrap_or_else(|| {
                        unreachable!("region names are derived from region_dbs")
                    }),
                    config: ReplicationRuntimeConfig {
                        self_region: origin_region.clone(),
                        poll_interval: config.poll_interval,
                        heartbeat_interval: config.heartbeat_interval,
                        heartbeat_jitter: config.heartbeat_jitter,
                        batch_mutation_limit: config.batch_mutation_limit,
                        batch_byte_limit: config.batch_byte_limit,
                        peers,
                    },
                    client,
                    sync_role: config
                        .single_node_sync_regions
                        .iter()
                        .any(|sync_region| sync_region == origin_region)
                        .then_some(storage_sync::SyncRaftRole::Follower),
                },
            );
        }

        Self {
            regions,
            network,
            region_order: region_names,
            emulated_clock_skew_ms: config.emulated_clock_skew_ms,
        }
    }

    #[must_use]
    pub fn region_names(&self) -> &[String] {
        &self.region_order
    }

    #[must_use]
    pub fn emulated_clock_skew_ms(&self) -> u64 {
        self.emulated_clock_skew_ms
    }

    pub async fn create_global_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.create_stream_table_in_all_regions(table_name).await?;
        for (region_name, region) in &self.regions {
            let updates = self
                .region_order
                .iter()
                .filter(|peer_region| *peer_region != region_name)
                .map(|peer_region| ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: peer_region.clone(),
                    }),
                    update: None,
                    delete: None,
                })
                .collect::<Vec<_>>();
            region
                .db
                .apply_replica_updates(table_name, &updates)
                .await?;
            for peer_region in &self.region_order {
                if peer_region == region_name {
                    continue;
                }
                region
                    .db
                    .mark_replica_active(table_name, peer_region)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn create_stream_table_in_all_regions(
        &self,
        table_name: &TableName,
    ) -> StorageResult<()> {
        for region in self.regions.values() {
            create_stream_table(region.db.as_ref(), table_name).await?;
        }
        Ok(())
    }

    pub async fn create_bootstrap_replica(
        &self,
        source_region: &str,
        peer_region: &str,
        table_name: &TableName,
    ) -> StorageResult<()> {
        let source = self.region(source_region)?;
        source
            .db
            .apply_replica_updates(
                table_name,
                &[ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: peer_region.to_string(),
                    }),
                    update: None,
                    delete: None,
                }],
            )
            .await?;
        source
            .db
            .put_table_bootstrap_cursor(&TableBootstrapCursorRecord {
                table_name: table_name.clone(),
                peer_region: peer_region.to_string(),
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
    }

    pub async fn put_item_value(
        &self,
        region_name: &str,
        table_name: &TableName,
        pk: &str,
        sk: &str,
        value: &str,
        padded_payload_bytes: usize,
    ) -> StorageResult<()> {
        let region = self.region(region_name)?;
        sleep_if_needed(self.region_write_bias(region_name)).await;
        region
            .db
            .put_item(
                PutItemInput::builder()
                    .table_name(table_name.clone())
                    .item(item(pk, sk, value, padded_payload_bytes))
                    .build(),
            )
            .await
            .map(|_| ())
    }

    pub async fn delete_item(
        &self,
        region_name: &str,
        table_name: &TableName,
        pk: &str,
        sk: &str,
    ) -> StorageResult<()> {
        let region = self.region(region_name)?;
        sleep_if_needed(self.region_write_bias(region_name)).await;
        region
            .db
            .delete_item(
                DeleteItemInput::builder()
                    .table_name(table_name.clone())
                    .key(item_key(pk, sk))
                    .build(),
            )
            .await
            .map(|_| ())
    }

    pub async fn get_item_value(
        &self,
        region_name: &str,
        table_name: &TableName,
        pk: &str,
        sk: &str,
    ) -> StorageResult<Option<String>> {
        let region = self.region(region_name)?;
        let item = region
            .db
            .get_item_map(table_name.clone(), item_key(pk, sk))
            .await?;
        Ok(item.and_then(|attrs| {
            attrs
                .get("value")
                .and_then(|value| value.inner_str().ok())
                .map(ToString::to_string)
        }))
    }

    pub async fn get_item_origin_commit_ts(
        &self,
        region_name: &str,
        table_name: &TableName,
        pk: &str,
        sk: &str,
    ) -> StorageResult<Option<TimestampMillis>> {
        let region = self.region(region_name)?;
        let metadata = region
            .db
            .get_latest_item_replication_metadata(table_name, &item_key(pk, sk))
            .await?;
        Ok(metadata.map(|metadata| metadata.origin_commit_ts))
    }

    pub async fn all_regions_match_value(
        &self,
        table_name: &TableName,
        pk: &str,
        sk: &str,
        expected_value: Option<&str>,
    ) -> StorageResult<bool> {
        for region_name in &self.region_order {
            let actual = self.get_item_value(region_name, table_name, pk, sk).await?;
            if actual.as_deref() != expected_value {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn region(&self, region_name: &str) -> StorageResult<&SimulationRegion> {
        self.regions
            .get(region_name)
            .ok_or_else(|| StorageError::validation(format!("region '{region_name}' not found")))
    }

    fn region_write_bias(&self, region_name: &str) -> Duration {
        let index = self
            .region_order
            .iter()
            .position(|candidate| candidate == region_name)
            .unwrap_or_default() as u64;
        Duration::from_millis(index.saturating_mul(self.emulated_clock_skew_ms))
    }
}
