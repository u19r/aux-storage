#[cfg(any(test, feature = "sqlite"))]
use std::{collections::BTreeMap, path::Path};
use std::{sync::Arc, time::Duration};

use config::StorageSyncReplicationConfig;
#[cfg(any(test, feature = "sqlite"))]
use openraft::{Config as OpenRaftConfig, SnapshotPolicy};
#[cfg(feature = "sqlite")]
use sql::{SQLiteStorageProvider, SqliteSyncRaftLogStore};
use storage::DatabaseManager;
#[cfg(any(test, feature = "sqlite"))]
use storage_provider::StorageBackend;
use storage_provider::StorageConfig;
#[cfg(any(test, feature = "sqlite"))]
use storage_sync::{SyncNode, SyncRaftNetworkFactory, SyncRaftRuntime};
use storage_types::{StorageError, StorageResult};

use crate::SyncRaftRuntimeAdapter;
#[cfg(any(test, feature = "sqlite"))]
use crate::{HttpSyncRaftRpcClient, SyncLearnerJoinRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(test, feature = "sqlite"))]
pub(crate) enum SyncRaftStartupDecision {
    Allow,
    SqliteFeatureRequired,
    LocalBackendRequired,
    SqliteRaftPathRequired,
    SyncDataDirRequired,
}

pub async fn build_sync_raft_runtime_adapter(
    db: Arc<DatabaseManager>,
    sync: &StorageSyncReplicationConfig,
    storage_config: &StorageConfig,
) -> StorageResult<Option<Arc<SyncRaftRuntimeAdapter>>> {
    if !sync.enabled {
        return Ok(None);
    }

    #[cfg(feature = "sqlite")]
    {
        validate_sync_raft_startup(storage_config, sync)?;

        let node_id = required_node_id(sync)?;
        let rpc_client = HttpSyncRaftRpcClient::from_config(sync)?;
        let network = SyncRaftNetworkFactory::new(Arc::new(rpc_client.clone()));
        let provider = sqlite_raft_provider(sync, storage_config, node_id).await?;
        let log_store = SqliteSyncRaftLogStore::new(provider);
        let runtime = SyncRaftRuntime::new(
            node_id,
            Arc::new(openraft_config(sync)?),
            network,
            log_store,
            db.clone(),
            sync.advertise_url.clone(),
            Some("sqlite".to_string()),
        )
        .await?;
        if sync.join_as_learner {
            request_learner_join(sync, storage_config, &rpc_client).await?;
        } else {
            runtime.initialize_if_needed(initial_members(sync)?).await?;
        }
        Ok(Some(Arc::new(
            SyncRaftRuntimeAdapter::new_with_coalescing_window(
                db,
                runtime,
                sync_proposal_coalescing_window(sync),
            ),
        )))
    }

    #[cfg(not(feature = "sqlite"))]
    {
        let _ = (db, sync, storage_config);
        Err(StorageError::unsupported(
            "features.storage_sync_replication requires a storage-api binary built with sqlite \
             support",
        ))
    }
}

#[cfg(any(test, feature = "sqlite"))]
pub(crate) fn plan_sync_raft_startup(
    storage_config: &StorageConfig,
    sync: &StorageSyncReplicationConfig,
) -> SyncRaftStartupDecision {
    plan_sync_raft_startup_with_sqlite_feature(storage_config, sync, cfg!(feature = "sqlite"))
}

#[cfg(any(test, feature = "sqlite"))]
pub(crate) fn plan_sync_raft_startup_with_sqlite_feature(
    storage_config: &StorageConfig,
    sync: &StorageSyncReplicationConfig,
    sqlite_feature_enabled: bool,
) -> SyncRaftStartupDecision {
    if !sqlite_feature_enabled {
        return SyncRaftStartupDecision::SqliteFeatureRequired;
    }
    if matches!(storage_config.backend_type, StorageBackend::Remote) {
        return SyncRaftStartupDecision::LocalBackendRequired;
    }
    if matches!(storage_config.backend_type, StorageBackend::SQLite) {
        return if sync_data_dir_configured(sync) || storage_connection_configured(storage_config) {
            SyncRaftStartupDecision::Allow
        } else {
            SyncRaftStartupDecision::SqliteRaftPathRequired
        };
    }
    if sync_data_dir_configured(sync) {
        SyncRaftStartupDecision::Allow
    } else {
        SyncRaftStartupDecision::SyncDataDirRequired
    }
}

#[cfg(any(test, feature = "sqlite"))]
fn validate_sync_raft_startup(
    storage_config: &StorageConfig,
    sync: &StorageSyncReplicationConfig,
) -> StorageResult<()> {
    match plan_sync_raft_startup(storage_config, sync) {
        SyncRaftStartupDecision::Allow => Ok(()),
        SyncRaftStartupDecision::SqliteFeatureRequired => Err(StorageError::unsupported(
            "features.storage_sync_replication requires a storage-api binary built with sqlite \
             support for the durable sync Raft log store",
        )),
        SyncRaftStartupDecision::LocalBackendRequired => Err(StorageError::unsupported(
            "features.storage_sync_replication requires a local storage backend",
        )),
        SyncRaftStartupDecision::SqliteRaftPathRequired => Err(StorageError::validation(
            "features.storage_sync_replication.data_dir is required when sqlite connection_string \
             is unavailable",
        )),
        SyncRaftStartupDecision::SyncDataDirRequired => Err(StorageError::validation(
            "features.storage_sync_replication.data_dir is required when the storage backend is \
             not sqlite, because the sync Raft log uses a separate durable sqlite database",
        )),
    }
}

#[cfg(any(test, feature = "sqlite"))]
fn required_node_id(sync: &StorageSyncReplicationConfig) -> StorageResult<u64> {
    sync.node_id.ok_or_else(|| {
        StorageError::validation(
            "features.storage_sync_replication.node_id is required when enabled",
        )
    })
}

#[cfg(any(test, feature = "sqlite"))]
fn openraft_config(sync: &StorageSyncReplicationConfig) -> StorageResult<OpenRaftConfig> {
    OpenRaftConfig {
        cluster_name: "aux-storage-sync".to_string(),
        election_timeout_min: sync.election_timeout_ms,
        election_timeout_max: sync.election_timeout_ms.saturating_mul(2),
        heartbeat_interval: sync.heartbeat_interval_ms,
        snapshot_policy: SnapshotPolicy::Never,
        ..OpenRaftConfig::default()
    }
    .validate()
    .map_err(|error| StorageError::validation(format!("invalid sync raft config: {error}")))
}

#[cfg(any(test, feature = "sqlite"))]
fn sync_proposal_coalescing_window(sync: &StorageSyncReplicationConfig) -> Duration {
    Duration::from_micros(sync.proposal_coalescing_window_us)
}

#[cfg(any(test, feature = "sqlite"))]
fn initial_members(sync: &StorageSyncReplicationConfig) -> StorageResult<BTreeMap<u64, SyncNode>> {
    let node_id = required_node_id(sync)?;
    let advertise_url = sync
        .advertise_url
        .as_deref()
        .ok_or_else(|| {
            StorageError::validation(
                "features.storage_sync_replication.advertise_url is required when enabled",
            )
        })?
        .trim();
    if advertise_url.is_empty() {
        return Err(StorageError::validation(
            "features.storage_sync_replication.advertise_url is required when enabled",
        ));
    }

    let mut members = BTreeMap::from([(node_id, SyncNode::new(advertise_url))]);
    for peer in &sync.peers {
        members.insert(peer.node_id, SyncNode::new(peer.endpoint_url.trim()));
    }
    Ok(members)
}

#[cfg(any(test, feature = "sqlite"))]
async fn request_learner_join(
    sync: &StorageSyncReplicationConfig,
    storage_config: &StorageConfig,
    client: &HttpSyncRaftRpcClient,
) -> StorageResult<()> {
    let node_id = required_node_id(sync)?;
    let advertise_url = required_advertise_url(sync)?;
    let target = learner_join_peer_node_id(sync)?;
    let request = SyncLearnerJoinRequest {
        node_id,
        advertise_url: advertise_url.to_string(),
        backend_compatibility: Some(sync_backend_name(storage_config).to_string()),
    };
    client.request_learner_join(target, &request).await?;
    Ok(())
}

#[cfg(any(test, feature = "sqlite"))]
fn sync_backend_name(storage_config: &StorageConfig) -> &'static str {
    match storage_config.backend_type {
        StorageBackend::SQLite => "sqlite",
        StorageBackend::Postgres => "postgres",
        StorageBackend::Turso => "turso",
        StorageBackend::RocksDB => "rocksdb",
        StorageBackend::FoundationDb => "foundationdb",
        StorageBackend::Remote => "remote",
    }
}

#[cfg(any(test, feature = "sqlite"))]
fn sync_data_dir_configured(sync: &StorageSyncReplicationConfig) -> bool {
    sync.data_dir
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

#[cfg(any(test, feature = "sqlite"))]
fn storage_connection_configured(storage_config: &StorageConfig) -> bool {
    storage_config
        .connection_string
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

#[cfg(any(test, feature = "sqlite"))]
fn learner_join_peer_node_id(sync: &StorageSyncReplicationConfig) -> StorageResult<u64> {
    if let Some(node_id) = sync.learner_join_peer_node_id {
        return Ok(node_id);
    }
    sync.peers.first().map(|peer| peer.node_id).ok_or_else(|| {
        StorageError::validation(
            "features.storage_sync_replication.peers must contain at least one bootstrap peer \
             when join_as_learner is true",
        )
    })
}

#[cfg(any(test, feature = "sqlite"))]
fn required_advertise_url(sync: &StorageSyncReplicationConfig) -> StorageResult<&str> {
    let advertise_url = sync
        .advertise_url
        .as_deref()
        .ok_or_else(|| {
            StorageError::validation(
                "features.storage_sync_replication.advertise_url is required when enabled",
            )
        })?
        .trim();
    if advertise_url.is_empty() {
        return Err(StorageError::validation(
            "features.storage_sync_replication.advertise_url is required when enabled",
        ));
    }
    Ok(advertise_url)
}

#[cfg(feature = "sqlite")]
async fn sqlite_raft_provider(
    sync: &StorageSyncReplicationConfig,
    storage_config: &StorageConfig,
    node_id: u64,
) -> StorageResult<SQLiteStorageProvider> {
    let path = sqlite_raft_path(sync, storage_config, node_id)?;
    let mut settings = storage_config.sqlite.clone().unwrap_or_default();
    settings.force_file_backed_database = true;
    SQLiteStorageProvider::new_with_settings(&path, settings).await
}

#[cfg(feature = "sqlite")]
fn sqlite_raft_path(
    sync: &StorageSyncReplicationConfig,
    storage_config: &StorageConfig,
    node_id: u64,
) -> StorageResult<String> {
    if let Some(data_dir) = sync
        .data_dir
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(Path::new(data_dir)
            .join(format!("sync-raft-node-{node_id}.db"))
            .to_string_lossy()
            .into_owned());
    }

    if !matches!(storage_config.backend_type, StorageBackend::SQLite) {
        return Err(StorageError::validation(
            "features.storage_sync_replication.data_dir is required when the storage backend is \
             not sqlite, because the sync Raft log uses a separate durable sqlite database",
        ));
    }

    storage_config.connection_string.clone().ok_or_else(|| {
        StorageError::validation(
            "features.storage_sync_replication.data_dir is required when sqlite connection_string \
             is unavailable",
        )
    })
}

#[cfg(test)]
pub(crate) fn sync_raft_test_members(
    sync: &StorageSyncReplicationConfig,
) -> StorageResult<BTreeMap<u64, SyncNode>> {
    initial_members(sync)
}

#[cfg(test)]
pub(crate) fn sync_raft_test_learner_join_peer_node_id(
    sync: &StorageSyncReplicationConfig,
) -> StorageResult<u64> {
    learner_join_peer_node_id(sync)
}

#[cfg(test)]
pub(crate) fn sync_raft_test_openraft_config(
    sync: &StorageSyncReplicationConfig,
) -> StorageResult<OpenRaftConfig> {
    openraft_config(sync)
}

#[cfg(test)]
pub(crate) fn sync_raft_test_proposal_coalescing_window(
    sync: &StorageSyncReplicationConfig,
) -> Duration {
    sync_proposal_coalescing_window(sync)
}

#[cfg(test)]
#[cfg(feature = "sqlite")]
pub(crate) fn sync_raft_test_sqlite_path(
    sync: &StorageSyncReplicationConfig,
    storage_config: &StorageConfig,
    node_id: u64,
) -> StorageResult<String> {
    sqlite_raft_path(sync, storage_config, node_id)
}
