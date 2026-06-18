use std::sync::Arc;
#[cfg(feature = "foundationdb")]
use std::time::Duration;

#[cfg(any(
    feature = "foundationdb",
    feature = "rocksdb",
    feature = "sqlite",
    feature = "turso",
    feature = "postgres"
))]
use bg_job_store::SysJobLockStore;
#[cfg(any(
    feature = "foundationdb",
    feature = "rocksdb",
    feature = "sqlite",
    feature = "turso",
    feature = "postgres"
))]
use bg_jobs::{JobManager, default_worker_id};
#[cfg(feature = "rocksdb")]
use kv::RocksDbKvStore;
#[cfg(any(feature = "foundationdb", feature = "rocksdb"))]
use kv::SortedKvDbStorageProvider;
#[cfg(feature = "foundationdb")]
use kv::{FoundationDbConfig, FoundationDbKvStore};
#[cfg(feature = "postgres")]
use sql::PostgresStorageProvider;
#[cfg(feature = "sqlite")]
use sql::SQLiteStorageProvider;
#[cfg(feature = "turso")]
use sql::TursoStorageProvider;
use storage_common::DatabaseJobIntervals;
use storage_provider::{StorageBackend, StorageConfig};
#[cfg(feature = "remote")]
use storage_remote::RemoteStorageProvider;
use storage_types::{StorageError, StorageResult};

#[cfg(feature = "foundationdb")]
use crate::constants::FOUNDATIONDB_STARTUP_REACHABILITY_TIMEOUT_SECS;
use crate::newtypes::DatabaseTrait;

pub struct StorageProviderBundle {
    pub database: Box<dyn DatabaseTrait>,
    pub queue: Option<Arc<dyn queue_provider::QueueProvider>>,
    pub pubsub: Option<Arc<dyn pubsub_provider::PubsubProvider>>,
}

/// Factory function to create storage providers
pub async fn create_storage_provider(
    config: StorageConfig,
    enable_database_jobs: bool,
) -> StorageResult<Box<dyn DatabaseTrait>> {
    Ok(create_storage_provider_bundle(
        config,
        enable_database_jobs,
        DatabaseJobIntervals::default(),
    )
    .await?
    .database)
}

pub async fn create_storage_provider_bundle(
    config: StorageConfig,
    enable_database_jobs: bool,
    database_job_intervals: DatabaseJobIntervals,
) -> StorageResult<StorageProviderBundle> {
    #[cfg(not(any(
        feature = "foundationdb",
        feature = "postgres",
        feature = "rocksdb",
        feature = "sqlite"
    )))]
    let _ = enable_database_jobs;
    #[cfg(not(any(
        feature = "foundationdb",
        feature = "postgres",
        feature = "rocksdb",
        feature = "sqlite"
    )))]
    let _ = database_job_intervals;
    tracing::info!(
        "Creating storage provider for backend: {:?}",
        config.backend_type
    );
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite => {
            let db_path = config
                .connection_string
                .unwrap_or_else(|| "main.db".to_string());
            let provider = SQLiteStorageProvider::new_with_settings(
                &db_path,
                config.sqlite.clone().unwrap_or_default(),
            )
            .await?;
            let use_in_memory_lock = use_in_memory_job_lock_for_path(&db_path);
            let job_manager = if use_in_memory_lock {
                JobManager::new_for_test()
            } else {
                let lock_backend: Arc<dyn storage_provider::StorageProvider> =
                    Arc::new(provider.clone());
                let lock_store =
                    Arc::new(SysJobLockStore::new(lock_backend, default_worker_id()).await?);
                JobManager::new(lock_store)
            };
            let provider = provider
                .with_job_manager(job_manager)
                .with_database_jobs_enabled(enable_database_jobs)
                .with_database_job_intervals(database_job_intervals);
            Ok(StorageProviderBundle {
                database: Box::new(provider.clone()),
                queue: Some(Arc::new(provider.clone())),
                pubsub: Some(Arc::new(provider)),
            })
        }
        #[cfg(feature = "turso")]
        StorageBackend::Turso => {
            let db_path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_string());
            let provider = TursoStorageProvider::new(&db_path)
                .await?
                .with_immediate_gsi_consistency(
                    config
                        .turso
                        .clone()
                        .unwrap_or_default()
                        .immediate_gsi_consistency,
                );
            let use_in_memory_lock = use_in_memory_job_lock_for_path(&db_path);
            let job_manager = if use_in_memory_lock {
                JobManager::new_for_test()
            } else {
                let lock_backend: Arc<dyn storage_provider::StorageProvider> =
                    Arc::new(provider.clone());
                let lock_store =
                    Arc::new(SysJobLockStore::new(lock_backend, default_worker_id()).await?);
                JobManager::new(lock_store)
            };
            let provider = provider.with_job_manager(job_manager);
            Ok(StorageProviderBundle {
                database: Box::new(provider.clone()),
                queue: Some(Arc::new(provider.clone())),
                pubsub: Some(Arc::new(provider)),
            })
        }
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres => {
            let settings = config.postgres.clone().ok_or_else(|| {
                StorageError::validation(
                    "postgres backend requires postgres settings with dsn and pool size",
                )
            })?;
            let provider = PostgresStorageProvider::new_with_tls(
                &settings.dsn,
                settings.max_pool_size,
                settings.background_max_pool_size,
                settings.tls,
            )
            .await?
            .with_immediate_gsi_consistency(settings.immediate_gsi_consistency);
            let lock_backend: Arc<dyn storage_provider::StorageProvider> =
                Arc::new(provider.clone());
            let lock_store =
                Arc::new(SysJobLockStore::new(lock_backend, default_worker_id()).await?);
            let job_manager = JobManager::new(lock_store);
            let provider = provider
                .with_job_manager(job_manager)
                .with_database_job_intervals(database_job_intervals);
            Ok(StorageProviderBundle {
                database: Box::new(provider.clone()),
                queue: Some(Arc::new(provider.clone())),
                pubsub: Some(Arc::new(provider)),
            })
        }

        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB => {
            let db_path = config
                .connection_string
                .or(config.file_path)
                .unwrap_or_else(|| {
                    std::env::temp_dir()
                        .join("rocksdb")
                        .to_string_lossy()
                        .to_string()
                });
            let rocks_db = RocksDbKvStore::new(db_path.into())?;
            let provider = SortedKvDbStorageProvider::new(rocks_db).with_immediate_gsi_consistency(
                config
                    .rocksdb
                    .clone()
                    .unwrap_or_default()
                    .immediate_gsi_consistency,
            );
            let lock_backend: Arc<dyn storage_provider::StorageProvider> =
                Arc::new(provider.clone());
            let lock_store =
                Arc::new(SysJobLockStore::new(lock_backend, default_worker_id()).await?);
            let job_manager = JobManager::new(lock_store);
            let provider = provider
                .with_job_manager(job_manager)
                .with_database_jobs_enabled(enable_database_jobs)
                .with_database_job_intervals(database_job_intervals);

            Ok(StorageProviderBundle {
                database: Box::new(provider.clone()),
                queue: Some(Arc::new(provider.clone())),
                pubsub: Some(Arc::new(provider)),
            })
        }
        #[cfg(feature = "foundationdb")]
        StorageBackend::FoundationDb => {
            let fdb_config = build_foundationdb_config(&config);
            let store = FoundationDbKvStore::connect(fdb_config)?;
            store
                .check_reachable(Duration::from_secs(
                    FOUNDATIONDB_STARTUP_REACHABILITY_TIMEOUT_SECS,
                ))
                .await?;
            let provider = SortedKvDbStorageProvider::new(store).with_immediate_gsi_consistency(
                config
                    .foundationdb
                    .clone()
                    .unwrap_or_default()
                    .immediate_gsi_consistency,
            );
            let lock_backend: Arc<dyn storage_provider::StorageProvider> =
                Arc::new(provider.clone());
            let lock_store =
                Arc::new(SysJobLockStore::new(lock_backend, default_worker_id()).await?);
            let job_manager = JobManager::new(lock_store);
            let provider = provider
                .with_job_manager(job_manager)
                .with_database_jobs_enabled(enable_database_jobs)
                .with_database_job_intervals(database_job_intervals);
            Ok(StorageProviderBundle {
                database: Box::new(provider.clone()),
                queue: Some(Arc::new(provider.clone())),
                pubsub: Some(Arc::new(provider)),
            })
        }
        #[cfg(feature = "remote")]
        StorageBackend::Remote => {
            let settings = config.remote.clone().ok_or_else(|| {
                StorageError::validation("remote backend requires remote configuration settings")
            })?;
            let provider = RemoteStorageProvider::new(settings).await?;
            Ok(StorageProviderBundle {
                database: Box::new(provider),
                queue: None,
                pubsub: None,
            })
        }
        #[cfg(not(feature = "remote"))]
        StorageBackend::Remote => Err(StorageError::validation(
            "remote storage backend is not enabled in this build".to_string(),
        )),
        #[cfg(not(feature = "sqlite"))]
        StorageBackend::SQLite => Err(StorageError::validation(
            "SQLite storage backend is not enabled in this build".to_string(),
        )),
        #[cfg(not(feature = "turso"))]
        StorageBackend::Turso => Err(StorageError::validation(
            "Turso storage backend is not enabled in this build".to_string(),
        )),
        #[cfg(not(feature = "postgres"))]
        StorageBackend::Postgres => Err(StorageError::validation(
            "Postgres storage backend is not enabled in this build".to_string(),
        )),
        #[cfg(not(feature = "rocksdb"))]
        StorageBackend::RocksDB => Err(StorageError::validation(
            "RocksDB storage backend is not enabled in this build".to_string(),
        )),
        #[cfg(not(feature = "foundationdb"))]
        StorageBackend::FoundationDb => Err(StorageError::validation(
            "FoundationDB storage backend is not enabled in this build".to_string(),
        )),
    }
}

#[cfg(any(test, feature = "sqlite", feature = "turso"))]
pub(crate) fn use_in_memory_job_lock_for_path(db_path: &str) -> bool {
    db_path == ":memory:"
}

#[cfg(feature = "foundationdb")]
fn build_foundationdb_config(config: &StorageConfig) -> FoundationDbConfig {
    let settings = config.foundationdb.clone().unwrap_or_default();
    let mut fdb_config = FoundationDbConfig::default();
    if let Some(cluster_file_path) = settings.cluster_file {
        fdb_config.cluster_file_path = Some(cluster_file_path);
    }
    if let Some(prefix) = settings.subspace_prefix {
        fdb_config.subspace_prefix = Some(prefix.into_bytes());
    }
    if let Some(tenant) = settings.tenant_name {
        fdb_config.tenant_name = Some(tenant.into_bytes());
    }
    fdb_config.cache_read_version_ms = settings.cache_read_version_ms;
    fdb_config.immediate_gsi_consistency = settings.immediate_gsi_consistency;
    fdb_config
}
