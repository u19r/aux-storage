//! Construct one initialized family store for the standalone stream service.

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
use kv_backend::KvStore;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use sql_backend::SqlStore;
use storage_provider::{StorageBackend, StorageConfig};
use stream_provider::{StreamError, StreamOperations, StreamResult};

/// Select the configured driver, initialize its physical format, and publish
/// it.
pub async fn create_stream_provider(
    config: StorageConfig,
) -> StreamResult<Box<dyn StreamOperations>> {
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite => {
            let path = config
                .connection_string
                .unwrap_or_else(|| "main.db".to_owned());
            let driver = sql_backend::sqlite::SqliteBackend::initialize(path).await?;
            Ok(Box::new(SqlStore::initialize_schema(driver).await?))
        }
        #[cfg(not(feature = "sqlite"))]
        StorageBackend::SQLite => Err(StreamError::internal(
            "sqlite stream backend is not enabled in this build",
        )),
        #[cfg(feature = "turso")]
        StorageBackend::Turso => {
            let path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_owned());
            let driver = sql_backend::turso::TursoBackend::initialize(&path).await?;
            Ok(Box::new(SqlStore::initialize_schema(driver).await?))
        }
        #[cfg(not(feature = "turso"))]
        StorageBackend::Turso => Err(StreamError::internal(
            "turso stream backend is not enabled in this build",
        )),
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres => {
            let dsn = config
                .connection_string
                .ok_or_else(|| StreamError::internal("postgres stream backend missing dsn"))?;
            let (pool_size, tls) = config.postgres.as_ref().map_or((16, true), |settings| {
                (settings.max_pool_size, settings.tls)
            });
            let driver =
                sql_backend::postgres::PostgresBackend::initialize(&dsn, pool_size, tls).await?;
            Ok(Box::new(SqlStore::initialize_schema(driver).await?))
        }
        #[cfg(not(feature = "postgres"))]
        StorageBackend::Postgres => Err(StreamError::internal(
            "postgres stream backend is not enabled in this build",
        )),
        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB => {
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
        StorageBackend::RocksDB => Err(StreamError::internal(
            "rocksdb stream backend is not enabled in this build",
        )),
        #[cfg(feature = "foundationdb")]
        StorageBackend::FoundationDb => {
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
                    report_conflicting_keys: false,
                },
            )
            .await?;
            Ok(Box::new(KvStore::initialize_format(driver).await?))
        }
        #[cfg(not(feature = "foundationdb"))]
        StorageBackend::FoundationDb => Err(StreamError::internal(
            "foundationdb stream backend is not enabled in this build",
        )),
        StorageBackend::Remote => Err(StreamError::internal(
            "remote stream backend is not implemented",
        )),
    }
}
