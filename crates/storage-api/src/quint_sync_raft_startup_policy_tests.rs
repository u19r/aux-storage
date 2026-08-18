#![allow(non_snake_case)]

use config::StorageSyncReplicationConfig;
use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_provider::{StorageBackend, StorageConfig};

use crate::sync_replication_startup::{
    SyncRaftStartupDecision, plan_sync_raft_startup_with_sqlite_feature,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct StartupInput {
    #[serde(rename = "sqliteFeatureEnabled")]
    sqlite_feature_enabled: bool,
    #[serde(rename = "storageBackend")]
    storage_backend: String,
    #[serde(rename = "dataDirConfigured")]
    data_dir_configured: bool,
    #[serde(rename = "storageConnectionConfigured")]
    storage_connection_configured: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncRaftStartupPolicyState {
    #[serde(rename = "lastInput")]
    last_input: StartupInput,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncRaftStartupPolicyDriver> for SyncRaftStartupPolicyState {
    fn from_driver(driver: &SyncRaftStartupPolicyDriver) -> Result<Self> {
        Ok(Self {
            last_input: driver.last_input.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncRaftStartupPolicyDriver {
    last_input: StartupInput,
    last_decision: String,
}

impl Default for SyncRaftStartupPolicyDriver {
    fn default() -> Self {
        Self {
            last_input: StartupInput {
                sqlite_feature_enabled: false,
                storage_backend: String::new(),
                data_dir_configured: false,
                storage_connection_configured: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncRaftStartupPolicyDriver {
    type State = SyncRaftStartupPolicyState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                sqliteFeatureEnabled: bool,
                storageBackend: String,
                dataDirConfigured: bool,
                storageConnectionConfigured: bool,
            ) => {
                self.check(StartupInput {
                    sqlite_feature_enabled: sqliteFeatureEnabled,
                    storage_backend: storageBackend,
                    data_dir_configured: dataDirConfigured,
                    storage_connection_configured: storageConnectionConfigured,
                });
            },
            step(
                sqliteFeatureEnabled: bool?,
                storageBackend: String?,
                dataDirConfigured: bool?,
                storageConnectionConfigured: bool?,
            ) => {
                if let (
                    Some(sqlite_feature_enabled),
                    Some(storage_backend),
                    Some(data_dir_configured),
                    Some(storage_connection_configured),
                ) = (
                    sqliteFeatureEnabled,
                    storageBackend,
                    dataDirConfigured,
                    storageConnectionConfigured,
                ) {
                    self.check(StartupInput {
                        sqlite_feature_enabled,
                        storage_backend,
                        data_dir_configured,
                        storage_connection_configured,
                    });
                }
            },
        })
    }
}

impl SyncRaftStartupPolicyDriver {
    fn check(&mut self, input: StartupInput) {
        self.last_decision = decision_name(plan_sync_raft_startup_with_sqlite_feature(
            &storage_config(&input),
            &sync_config(&input),
            input.sqlite_feature_enabled,
        ))
        .to_string();
        self.last_input = input;
    }
}

fn storage_config(input: &StartupInput) -> StorageConfig {
    StorageConfig {
        backend_type: backend(&input.storage_backend),
        connection_string: input
            .storage_connection_configured
            .then(|| "storage.db".to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

fn sync_config(input: &StartupInput) -> StorageSyncReplicationConfig {
    StorageSyncReplicationConfig {
        data_dir: input
            .data_dir_configured
            .then(|| "run-artifacts/storage-api-data/sync".to_string()),
        ..StorageSyncReplicationConfig::default()
    }
}

fn backend(name: &str) -> StorageBackend {
    match name {
        "sqlite" => StorageBackend::SQLite,
        "postgres" => StorageBackend::Postgres,
        "turso" => StorageBackend::Turso,
        "rocksdb" => StorageBackend::RocksDB,
        "foundationdb" => StorageBackend::FoundationDb,
        _ => StorageBackend::Remote,
    }
}

fn decision_name(decision: SyncRaftStartupDecision) -> &'static str {
    match decision {
        SyncRaftStartupDecision::Allow => "allow",
        SyncRaftStartupDecision::SqliteFeatureRequired => "sqlite_feature_required",
        SyncRaftStartupDecision::LocalBackendRequired => "local_backend_required",
        SyncRaftStartupDecision::SqliteRaftPathRequired => "sqlite_raft_path_required",
        SyncRaftStartupDecision::SyncDataDirRequired => "sync_data_dir_required",
    }
}

#[quint_run(
    spec = "../../quint/sync_raft_startup_policy_mbt.qnt",
    max_samples = 96,
    max_steps = 8,
    seed = "0x5a1709"
)]
fn sync_raft_startup_policy_mbt_matches_rust_boundary() -> impl Driver {
    SyncRaftStartupPolicyDriver::default()
}
