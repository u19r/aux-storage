//! Construct one initialized family store for the standalone queue service.

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
use kv_backend::KvStore;
use queue_provider::{QueueBackend, QueueConfig, QueueError, QueueOperations, QueueResult};
#[cfg(feature = "remote")]
use queue_remote::RemoteQueueProvider;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use sql_backend::SqlStore;

/// Select the configured driver, initialize its physical format, and publish
/// it.
pub async fn create_queue_provider(config: QueueConfig) -> QueueResult<Box<dyn QueueOperations>> {
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        QueueBackend::SQLite => {
            let path = config
                .connection_string
                .unwrap_or_else(|| "main.db".to_owned());
            let driver = sql_backend::sqlite::SqliteBackend::initialize(path).await?;
            Ok(Box::new(SqlStore::initialize_schema(driver).await?))
        }
        #[cfg(not(feature = "sqlite"))]
        QueueBackend::SQLite => Err(QueueError::internal(
            queue_provider::QueueInternalKind::SQLiteBackendDisabled,
        )),
        #[cfg(feature = "turso")]
        QueueBackend::Turso => {
            let path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_owned());
            let driver = sql_backend::turso::TursoBackend::initialize(&path).await?;
            Ok(Box::new(SqlStore::initialize_schema(driver).await?))
        }
        #[cfg(not(feature = "turso"))]
        QueueBackend::Turso => Err(QueueError::internal(
            queue_provider::QueueInternalKind::TursoBackendDisabled,
        )),
        #[cfg(feature = "postgres")]
        QueueBackend::Postgres => {
            let settings = config.postgres.ok_or_else(|| {
                QueueError::from(storage_types::StorageError::validation(
                    "postgres queue backend requires postgres settings with dsn and pool size",
                ))
            })?;
            let driver = sql_backend::postgres::PostgresBackend::initialize(
                &settings.dsn,
                settings.max_pool_size,
                settings.tls,
            )
            .await?;
            Ok(Box::new(SqlStore::initialize_schema(driver).await?))
        }
        #[cfg(not(feature = "postgres"))]
        QueueBackend::Postgres => Err(QueueError::internal_with_detail(
            queue_provider::QueueInternalKind::PostgresBackendDisabled,
            "postgres queue backend is not enabled in this build",
        )),
        #[cfg(feature = "rocksdb")]
        QueueBackend::RocksDB => {
            let path = config.connection_string.unwrap_or_else(|| {
                std::env::temp_dir()
                    .join("rocksdb")
                    .to_string_lossy()
                    .into_owned()
            });
            let driver = kv_backend::rocksdb::RocksDbBackend::initialize(path.into())?;
            Ok(Box::new(KvStore::initialize_format(driver).await?))
        }
        #[cfg(not(feature = "rocksdb"))]
        QueueBackend::RocksDB => Err(QueueError::internal_with_detail(
            queue_provider::QueueInternalKind::RocksDbBackendDisabled,
            "rocksdb queue backend is not enabled in this build",
        )),
        #[cfg(feature = "foundationdb")]
        QueueBackend::FoundationDb => {
            let settings = config.foundationdb.unwrap_or_default();
            let prefix = settings.subspace_prefix.as_deref().map(str::as_bytes);
            let tenant = settings.tenant_name.as_deref().map(str::as_bytes);
            let driver = kv_backend::foundationdb::FoundationDbBackend::initialize(
                kv_backend::foundationdb::FoundationDbOptions {
                    cluster_file: settings.cluster_file.as_deref(),
                    subspace_prefix: prefix,
                    tenant_name: tenant,
                    cache_read_version_ms: settings.cache_read_version_ms,
                    transaction_byte_limit: settings.transaction_byte_limit,
                    report_conflicting_keys: settings.report_conflicting_keys,
                },
            )
            .await?;
            Ok(Box::new(KvStore::initialize_format(driver).await?))
        }
        #[cfg(not(feature = "foundationdb"))]
        QueueBackend::FoundationDb => Err(QueueError::internal(
            queue_provider::QueueInternalKind::FoundationDbBackendDisabled,
        )),
        #[cfg(feature = "remote")]
        QueueBackend::Remote => {
            let settings = config.remote.ok_or_else(|| {
                QueueError::from(storage_types::StorageError::validation(
                    "remote queue backend requires remote configuration settings",
                ))
            })?;
            Ok(Box::new(RemoteQueueProvider::new(settings).await?))
        }
        #[cfg(not(feature = "remote"))]
        QueueBackend::Remote => Err(QueueError::internal(
            queue_provider::QueueInternalKind::RemoteBackendNotImplemented,
        )),
    }
}
