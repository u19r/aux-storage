#[cfg(feature = "remote")]
use std::fmt;
#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "postgres",
    feature = "rocksdb",
    feature = "foundationdb",
    feature = "remote"
))]
use std::sync::Arc;
#[cfg(feature = "remote")]
use std::time::Duration;

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
use kv_backend::KvStore;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
use sql_backend::SqlStore;
#[cfg(feature = "remote")]
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings, RemoteTimeoutOverrides};
use storage_provider::{StorageBackend, StorageConfig};
#[cfg(feature = "remote")]
use storage_remote::RemoteStorageProvider;
use storage_types::{StorageError, StorageResult};

#[cfg(feature = "remote")]
use super::models::RemoteScheduledJobStoreConfig;
use super::models::StorageProviderBundle;
#[cfg(feature = "remote")]
use crate::newtypes::DatabaseTrait;

#[cfg(feature = "remote")]
const MAX_REMOTE_SCHEDULED_JOB_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "remote")]
impl RemoteScheduledJobStoreConfig {
    pub fn new<I, S>(endpoint_urls: I, request_timeout: Duration) -> StorageResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let endpoint_urls = endpoint_urls
            .into_iter()
            .map(|endpoint| endpoint.into().trim().to_string())
            .collect::<Vec<_>>();
        storage_remote::validate_scheduled_job_http_endpoints(&endpoint_urls)?;
        if request_timeout.is_zero() || request_timeout > MAX_REMOTE_SCHEDULED_JOB_REQUEST_TIMEOUT {
            return Err(StorageError::validation(
                "remote scheduled-job request timeout is outside its supported bound",
            ));
        }
        Ok(Self {
            endpoint_urls,
            request_timeout,
        })
    }
}

#[cfg(feature = "remote")]
impl fmt::Debug for RemoteScheduledJobStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteScheduledJobStoreConfig")
            .field("endpoint_count", &self.endpoint_urls.len())
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

#[cfg(feature = "remote")]
pub async fn connect_remote_scheduled_job_store(
    config: RemoteScheduledJobStoreConfig,
) -> StorageResult<bg_job_store::ScheduledJobStore> {
    let timeout_ms = u64::try_from(config.request_timeout.as_millis())
        .map_err(|_| StorageError::validation("scheduled-job timeout exceeds remote limit"))?;
    let settings = RemoteStorageSettings {
        endpoint_urls: config.endpoint_urls,
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: Some(RemoteTimeoutOverrides {
            connect_timeout_ms: Some(timeout_ms),
            request_timeout_ms: Some(timeout_ms),
        }),
    };
    storage_remote::connect_scheduled_job_store(settings).await
}

pub(crate) async fn create_storage_provider_bundle(
    config: StorageConfig,
    _runtime_options: &crate::DatabaseManagerRuntimeOptions,
    _connection_id: &str,
) -> StorageResult<StorageProviderBundle> {
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite => {
            let path = config
                .connection_string
                .or(config.file_path)
                .unwrap_or_else(|| "main.db".to_owned());
            let driver = sql_backend::sqlite::SqliteBackend::initialize(path).await?;
            let store = Arc::new(SqlStore::initialize_schema(driver).await?);
            super::assembly::prepare_native_bundle(store, _runtime_options, _connection_id).await
        }
        #[cfg(feature = "turso")]
        StorageBackend::Turso => {
            let path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_owned());
            let driver = sql_backend::turso::TursoBackend::initialize(&path).await?;
            let store = Arc::new(SqlStore::initialize_schema(driver).await?);
            super::assembly::prepare_native_bundle(store, _runtime_options, _connection_id).await
        }
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres => {
            let settings = config.postgres.ok_or_else(|| {
                StorageError::validation(
                    "postgres backend requires postgres settings with dsn and pool size",
                )
            })?;
            let driver = sql_backend::postgres::PostgresBackend::initialize(
                &settings.dsn,
                settings.max_pool_size,
                settings.tls,
            )
            .await?;
            let store = Arc::new(SqlStore::initialize_schema(driver).await?);
            super::assembly::prepare_native_bundle(store, _runtime_options, _connection_id).await
        }
        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB => {
            let path = config
                .connection_string
                .or(config.file_path)
                .unwrap_or_else(|| {
                    std::env::temp_dir()
                        .join("rocksdb")
                        .to_string_lossy()
                        .into_owned()
                });
            let driver = kv_backend::rocksdb::RocksDbBackend::initialize(path.into())?;
            let store = Arc::new(KvStore::initialize_format(driver).await?);
            super::assembly::prepare_native_bundle(store, _runtime_options, _connection_id).await
        }
        #[cfg(feature = "foundationdb")]
        StorageBackend::FoundationDb => {
            let settings = config.foundationdb.unwrap_or_default();
            let driver = kv_backend::foundationdb::FoundationDbBackend::initialize(
                kv_backend::foundationdb::FoundationDbOptions {
                    cluster_file: settings.cluster_file.as_deref(),
                    subspace_prefix: settings.subspace_prefix.as_deref().map(str::as_bytes),
                    tenant_name: settings.tenant_name.as_deref().map(str::as_bytes),
                    cache_read_version_ms: settings.cache_read_version_ms,
                    transaction_byte_limit: settings.transaction_byte_limit,
                    report_conflicting_keys: false,
                },
            )
            .await?;
            let store = Arc::new(KvStore::initialize_format(driver).await?);
            super::assembly::prepare_native_bundle(store, _runtime_options, _connection_id).await
        }
        #[cfg(feature = "remote")]
        StorageBackend::Remote => {
            let settings = config.remote.ok_or_else(|| {
                StorageError::validation("remote backend requires remote configuration settings")
            })?;
            let scheduled_job_store =
                storage_remote::build_scheduled_job_store(settings.clone()).await?;
            let provider = Arc::new(RemoteStorageProvider::new(settings).await?);
            let database: Arc<dyn DatabaseTrait> = provider.clone();
            Ok(StorageProviderBundle {
                native_attempt_capacity: None,
                #[cfg(feature = "regional")]
                regional_services: None,
                database: Some(database),
                dynamo: provider.clone(),
                stream: None,

                ttl_sweep_repository: None,
                system_stream_trim_repository: None,
                table_reclaim_repository: None,
                gsi_lifecycle_repository: provider,
                queue: None,
                pubsub: None,
                delivery_source: None,
                maintenance: None,
                maintenance_source: None,
                scheduled_job_store,
            })
        }
        #[cfg(not(feature = "remote"))]
        StorageBackend::Remote => Err(StorageError::validation(
            "remote storage backend is not enabled in this build",
        )),
        #[cfg(not(feature = "sqlite"))]
        StorageBackend::SQLite => Err(StorageError::validation(
            "SQLite storage backend is not enabled in this build",
        )),
        #[cfg(not(feature = "turso"))]
        StorageBackend::Turso => Err(StorageError::validation(
            "Turso storage backend is not enabled in this build",
        )),
        #[cfg(not(feature = "postgres"))]
        StorageBackend::Postgres => Err(StorageError::validation(
            "Postgres storage backend is not enabled in this build",
        )),
        #[cfg(not(feature = "rocksdb"))]
        StorageBackend::RocksDB => Err(StorageError::validation(
            "RocksDB storage backend is not enabled in this build",
        )),
        #[cfg(not(feature = "foundationdb"))]
        StorageBackend::FoundationDb => Err(StorageError::validation(
            "FoundationDB storage backend is not enabled in this build",
        )),
    }
}
