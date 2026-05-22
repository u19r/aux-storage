#[cfg(feature = "rocksdb")]
use kv::RocksDbKvStore;
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
use kv::SortedKvDbStorageProvider;
#[cfg(feature = "foundationdb")]
use kv::{FoundationDbConfig, FoundationDbKvStore};
#[cfg(feature = "postgres")]
use sql::PostgresStorageProvider;
#[cfg(feature = "sqlite")]
use sql::SQLiteStorageProvider;
#[cfg(feature = "turso")]
use sql::TursoStorageProvider;
use storage_provider::{StorageBackend, StorageConfig};
use stream_provider::{StreamError, StreamProvider, StreamResult};

/// Factory function to create stream providers
pub async fn create_stream_provider(
    config: StorageConfig,
) -> StreamResult<Box<dyn StreamProvider>> {
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite => {
            let db_path = config
                .connection_string
                .unwrap_or_else(|| "main.db".to_string());
            let provider = SQLiteStorageProvider::new(&db_path).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "sqlite"))]
        StorageBackend::SQLite => Err(StreamError::internal(
            "sqlite stream backend is not enabled in this build",
        )),
        #[cfg(feature = "turso")]
        StorageBackend::Turso => {
            let db_path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_string());
            let provider = TursoStorageProvider::new(&db_path).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "turso"))]
        StorageBackend::Turso => Err(StreamError::internal(
            "turso stream backend is not enabled in this build",
        )),
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres => {
            let dsn = config
                .connection_string
                .clone()
                .ok_or_else(|| StreamError::internal("postgres stream backend missing dsn"))?;
            let (pool_size, tls_enabled) = config
                .postgres
                .as_ref()
                .map_or((16, true), |cfg| (cfg.max_pool_size, cfg.tls));
            let provider =
                PostgresStorageProvider::new_with_tls(&dsn, pool_size, tls_enabled).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "postgres"))]
        StorageBackend::Postgres => Err(StreamError::internal(
            "postgres stream backend is not enabled in this build",
        )),

        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB => {
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
        StorageBackend::RocksDB => Err(StreamError::internal(
            "rocksdb stream backend is not enabled in this build",
        )),
        #[cfg(feature = "foundationdb")]
        StorageBackend::FoundationDb => {
            let provider = build_foundationdb_provider(&config)?;
            Ok(Box::new(provider))
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

#[cfg(feature = "foundationdb")]
fn build_foundationdb_provider(
    config: &StorageConfig,
) -> StreamResult<SortedKvDbStorageProvider<FoundationDbKvStore>> {
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
    let store = FoundationDbKvStore::connect(fdb_config)?;
    Ok(SortedKvDbStorageProvider::new(store))
}
