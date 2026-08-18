use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use storage::{DatabaseManager, DatabaseManagerRuntimeOptions};
use storage_provider::{
    FoundationDbSettings, PostgresSettings, RocksdbSettings, SqliteSettings, TursoSettings,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, StorageError, StorageResult, StreamSpecification, StreamViewType,
    TableName,
};

use crate::{
    StorageBackend, StorageConfig, multi_region_harness::simulation::SimulationHarnessConfig,
};

pub(super) async fn create_stream_table(
    db: &DatabaseManager,
    table_name: &TableName,
) -> StorageResult<()> {
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
}

pub(super) fn item(
    pk: &str,
    sk: &str,
    value: &str,
    padded_payload_bytes: usize,
) -> HashMap<String, AttributeValue> {
    let mut item = item_key(pk, sk);
    item.insert("value".to_string(), AttributeValue::S(value.to_string()));
    if padded_payload_bytes > value.len() {
        item.insert(
            "padding".to_string(),
            AttributeValue::S("x".repeat(padded_payload_bytes - value.len())),
        );
    }
    item
}

pub(super) fn item_key(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
    ])
}

pub(super) async fn build_region_databases(
    config: &SimulationHarnessConfig,
) -> StorageResult<HashMap<String, Arc<DatabaseManager>>> {
    let sqlite_database_dir = config
        .sqlite_database_dir
        .clone()
        .unwrap_or_else(|| default_database_dir(config.storage_backend.as_str(), config.seed));
    if config
        .region_names
        .iter()
        .enumerate()
        .any(|(index, _)| uses_local_database_dir(storage_backend_for_region(config, index)))
    {
        std::fs::create_dir_all(&sqlite_database_dir).map_err(|error| {
            StorageError::internal(&format!(
                "create harness database directory '{}': {error}",
                sqlite_database_dir.display()
            ))
        })?;
    }

    let mut region_dbs = HashMap::new();
    for (region_index, region_name) in config.region_names.iter().enumerate() {
        let sync_enabled = config
            .single_node_sync_regions
            .iter()
            .any(|sync_region| sync_region == region_name);
        let manager = build_region_database(
            config,
            &sqlite_database_dir,
            region_name,
            region_index,
            sync_enabled,
        )
        .await?;
        region_dbs.insert(region_name.clone(), Arc::new(manager));
    }
    Ok(region_dbs)
}

async fn build_region_database(
    config: &SimulationHarnessConfig,
    database_dir: &Path,
    region_name: &str,
    region_index: usize,
    single_node_sync_enabled: bool,
) -> StorageResult<DatabaseManager> {
    DatabaseManager::new_with_config_and_runtime_options(
        storage_config_for_region(config, database_dir, region_name, region_index)?,
        harness_database_runtime_options(single_node_sync_enabled),
    )
    .await
}

fn storage_config_for_region(
    config: &SimulationHarnessConfig,
    database_dir: &Path,
    region_name: &str,
    region_index: usize,
) -> StorageResult<StorageConfig> {
    let backend = storage_backend_for_region(config, region_index);
    match backend {
        super::simulation::SimulationStorageBackend::Sqlite => Ok(StorageConfig {
            backend_type: StorageBackend::SQLite,
            connection_string: Some(
                database_dir
                    .join(format!("{region_name}.sqlite3"))
                    .display()
                    .to_string(),
            ),
            file_path: None,
            sqlite: Some(SqliteSettings {
                force_file_backed_database: true,
                ..SqliteSettings::default()
            }),
            turso: None,
            postgres: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        }),
        super::simulation::SimulationStorageBackend::Turso => Ok(StorageConfig {
            backend_type: StorageBackend::Turso,
            connection_string: Some(
                database_dir
                    .join(format!("{region_name}.turso.db"))
                    .display()
                    .to_string(),
            ),
            file_path: None,
            sqlite: None,
            turso: Some(TursoSettings::default()),
            postgres: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        }),
        super::simulation::SimulationStorageBackend::Rocksdb => Ok(StorageConfig {
            backend_type: StorageBackend::RocksDB,
            connection_string: Some(
                database_dir
                    .join(format!("{region_name}.rocksdb"))
                    .display()
                    .to_string(),
            ),
            file_path: None,
            sqlite: None,
            turso: None,
            postgres: None,
            rocksdb: Some(RocksdbSettings::default()),
            foundationdb: None,
            remote: None,
        }),
        super::simulation::SimulationStorageBackend::Postgres => {
            let template = config.postgres_dsn_template.as_ref().ok_or_else(|| {
                StorageError::validation(
                    "multi-region postgres harness requires a postgres DSN template",
                )
            })?;
            let dsn = template
                .replace("{region}", region_name)
                .replace("{region_name}", region_name)
                .replace("{node_id}", &(region_index + 1).to_string());
            Ok(StorageConfig {
                backend_type: StorageBackend::Postgres,
                connection_string: Some(dsn.clone()),
                file_path: None,
                sqlite: None,
                turso: None,
                postgres: Some(PostgresSettings {
                    dsn,
                    max_pool_size: config.postgres_max_pool_size,
                    background_max_pool_size: 4,
                    tls: config.postgres_tls,
                    immediate_gsi_consistency: false,
                }),
                rocksdb: None,
                foundationdb: None,
                remote: None,
            })
        }
        super::simulation::SimulationStorageBackend::Foundationdb => {
            let subspace_prefix = config
                .foundationdb_subspace_prefix
                .as_ref()
                .map(|prefix| format!("{prefix}-{region_name}"));
            Ok(StorageConfig {
                backend_type: StorageBackend::FoundationDb,
                connection_string: None,
                file_path: None,
                sqlite: None,
                turso: None,
                postgres: None,
                rocksdb: None,
                foundationdb: Some(FoundationDbSettings {
                    cluster_file: config.foundationdb_cluster_file.clone(),
                    tenant_name: None,
                    subspace_prefix,
                    cache_read_version_ms: 0,
                    immediate_gsi_consistency: false,
                }),
                remote: None,
            })
        }
    }
}

fn storage_backend_for_region(
    config: &SimulationHarnessConfig,
    region_index: usize,
) -> super::simulation::SimulationStorageBackend {
    config
        .region_storage_backends
        .get(region_index)
        .copied()
        .unwrap_or(config.storage_backend)
}

fn harness_database_runtime_options(
    single_node_sync_enabled: bool,
) -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions {
        enable_database_jobs: false,
        enable_background_refresh: false,
        enable_background_watchers: false,
        enable_single_node_sync_mode: single_node_sync_enabled,
        ..DatabaseManagerRuntimeOptions::default()
    }
}

fn uses_local_database_dir(backend: super::simulation::SimulationStorageBackend) -> bool {
    matches!(
        backend,
        super::simulation::SimulationStorageBackend::Sqlite
            | super::simulation::SimulationStorageBackend::Turso
            | super::simulation::SimulationStorageBackend::Rocksdb
    )
}

fn default_database_dir(backend: &str, seed: u64) -> PathBuf {
    let started_at_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let run_id = format!("{}-{}-{}", std::process::id(), seed, started_at_nanos);
    workspace_root()
        .join("run-artifacts/storage-api-multi-region")
        .join(backend)
        .join(run_id)
}

fn workspace_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| manifest_dir.to_path_buf(), Path::to_path_buf)
}
