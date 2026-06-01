#[cfg(feature = "rocksdb")]
use kv::RocksDbKvStore;
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
use kv::SortedKvDbStorageProvider;
#[cfg(feature = "foundationdb")]
use kv::{FoundationDbConfig, FoundationDbKvStore};
use queue_provider::{QueueBackend, QueueConfig, QueueError, QueueProvider, QueueResult};
#[cfg(feature = "remote")]
use queue_remote::RemoteQueueProvider;
#[cfg(feature = "postgres")]
use sql::PostgresStorageProvider;
#[cfg(feature = "sqlite")]
use sql::SQLiteStorageProvider;
#[cfg(feature = "turso")]
use sql::TursoStorageProvider;

/// Factory function to create queue providers
pub async fn create_queue_provider(config: QueueConfig) -> QueueResult<Box<dyn QueueProvider>> {
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        QueueBackend::SQLite => {
            let db_path = config
                .connection_string
                .unwrap_or_else(|| "main.db".to_string());
            let provider = SQLiteStorageProvider::new(&db_path).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "sqlite"))]
        QueueBackend::SQLite => Err(QueueError::internal(
            queue_provider::QueueInternalKind::SQLiteBackendDisabled,
        )),
        #[cfg(feature = "turso")]
        QueueBackend::Turso => {
            let db_path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_string());
            let provider = TursoStorageProvider::new(&db_path).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "turso"))]
        QueueBackend::Turso => Err(QueueError::internal(
            queue_provider::QueueInternalKind::TursoBackendDisabled,
        )),

        #[cfg(feature = "rocksdb")]
        QueueBackend::RocksDB => {
            let db_path = config.connection_string.unwrap_or_else(|| {
                std::env::temp_dir()
                    .join("rocksdb")
                    .to_string_lossy()
                    .to_string()
            });
            let rocks_db = RocksDbKvStore::new(db_path.into())?;
            let provider = SortedKvDbStorageProvider::new(rocks_db);

            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "rocksdb"))]
        QueueBackend::RocksDB => Err(QueueError::internal_with_detail(
            queue_provider::QueueInternalKind::RocksDbBackendDisabled,
            "rocksdb queue backend is not enabled in this build",
        )),
        #[cfg(feature = "foundationdb")]
        QueueBackend::FoundationDb => {
            let provider = build_foundationdb_provider(&config)?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "foundationdb"))]
        QueueBackend::FoundationDb => Err(QueueError::internal(
            queue_provider::QueueInternalKind::FoundationDbBackendDisabled,
        )),
        #[cfg(feature = "postgres")]
        QueueBackend::Postgres => {
            let settings = config.postgres.clone().ok_or_else(|| {
                QueueError::from(storage_types::StorageError::validation(
                    "postgres queue backend requires postgres settings with dsn and pool size",
                ))
            })?;
            let provider = PostgresStorageProvider::new_with_tls(
                &settings.dsn,
                settings.max_pool_size,
                settings.background_max_pool_size,
                settings.tls,
            )
            .await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "postgres"))]
        QueueBackend::Postgres => Err(QueueError::internal_with_detail(
            queue_provider::QueueInternalKind::PostgresBackendDisabled,
            "postgres queue backend is not enabled in this build",
        )),
        #[cfg(feature = "remote")]
        QueueBackend::Remote => {
            let settings = config.remote.clone().ok_or_else(|| {
                QueueError::from(storage_types::StorageError::validation(
                    "remote queue backend requires remote configuration settings",
                ))
            })?;
            let provider = RemoteQueueProvider::new(settings).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "remote"))]
        QueueBackend::Remote => Err(QueueError::internal(
            queue_provider::QueueInternalKind::RemoteBackendNotImplemented,
        )),
    }
}

#[cfg(feature = "foundationdb")]
fn build_foundationdb_provider(
    config: &QueueConfig,
) -> QueueResult<SortedKvDbStorageProvider<FoundationDbKvStore>> {
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
    fdb_config.report_conflicting_keys = settings.report_conflicting_keys;
    let store = FoundationDbKvStore::connect(fdb_config)?;
    Ok(SortedKvDbStorageProvider::new(store))
}
