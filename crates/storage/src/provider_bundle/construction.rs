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

use super::models::InitializedStore;
#[cfg(feature = "remote")]
use super::models::RemoteScheduledJobStoreConfig;
#[cfg(feature = "remote")]
use super::models::StoreKind;
#[cfg(feature = "remote")]
const MAX_REMOTE_SCHEDULED_JOB_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "remote")]
const MIN_INTERNAL_SERVICE_TOKEN_LEN: usize = 32;

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
            internal_service_token: None,
        })
    }

    /// Sends the token as `x-aux-internal-token` on every request, for an
    /// endpoint behind the storage owner's internal listener.
    pub fn with_internal_service_token(mut self, token: impl Into<String>) -> StorageResult<Self> {
        let token = token.into().trim().to_string();
        if token.len() < MIN_INTERNAL_SERVICE_TOKEN_LEN
            || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(StorageError::validation(
                "internal service token must be at least 32 printable characters",
            ));
        }
        self.internal_service_token = Some(token);
        Ok(self)
    }
}

#[cfg(feature = "remote")]
impl fmt::Debug for RemoteScheduledJobStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteScheduledJobStoreConfig")
            .field("endpoint_count", &self.endpoint_urls.len())
            .field("request_timeout", &self.request_timeout)
            .field(
                "has_internal_service_token",
                &self.internal_service_token.is_some(),
            )
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
            connect_timeout_ms: Some(
                u64::try_from(
                    storage_remote::connect_timeout_for(config.request_timeout).as_millis(),
                )
                .unwrap_or(timeout_ms),
            ),
            request_timeout_ms: Some(timeout_ms),
        }),
    };
    storage_remote::connect_scheduled_job_store_with_token(
        settings,
        config.internal_service_token.as_deref(),
    )
    .await
}

pub(crate) async fn open_store(
    config: StorageConfig,
    _runtime_options: &crate::DatabaseManagerRuntimeOptions,
    _connection_id: &str,
) -> StorageResult<InitializedStore> {
    #[cfg(any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    ))]
    let initial_stream_lineages = config
        .initial_stream_lineages
        .unwrap_or_else(|| config.backend_type.default_initial_stream_lineages());
    match config.backend_type {
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite => {
            let path = config.local_path().unwrap_or("main.db").to_owned();
            let driver = sql_backend::sqlite::SqliteBackend::initialize(
                path,
                _runtime_options.attempt_deadlines,
            )
            .await?;
            let store = Arc::new(
                SqlStore::initialize_schema(
                    driver,
                    initial_stream_lineages,
                    _runtime_options
                        .regional_connections
                        .contains_key(_connection_id),
                )
                .await?,
            );
            Ok(InitializedStore::native(store))
        }
        #[cfg(feature = "turso")]
        StorageBackend::Turso => {
            let path = config
                .connection_string
                .unwrap_or_else(|| "main.turso.db".to_owned());
            let driver = sql_backend::turso::TursoBackend::initialize(
                &path,
                _runtime_options.attempt_deadlines,
            )
            .await?;
            let store = Arc::new(
                SqlStore::initialize_schema(
                    driver,
                    initial_stream_lineages,
                    _runtime_options
                        .regional_connections
                        .contains_key(_connection_id),
                )
                .await?,
            );
            Ok(InitializedStore::native(store))
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
                _runtime_options.attempt_deadlines,
            )
            .await?;
            let store = Arc::new(
                SqlStore::initialize_schema(
                    driver,
                    initial_stream_lineages,
                    _runtime_options
                        .regional_connections
                        .contains_key(_connection_id),
                )
                .await?,
            );
            Ok(InitializedStore::native(store))
        }
        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB => {
            let path = config.local_path().map_or_else(
                || {
                    std::env::temp_dir()
                        .join("rocksdb")
                        .to_string_lossy()
                        .into_owned()
                },
                str::to_owned,
            );
            let writer_queue_capacity = config.rocksdb.unwrap_or_default().writer_queue_capacity;
            let driver = kv_backend::rocksdb::RocksDbBackend::initialize(
                path.into(),
                _runtime_options.attempt_deadlines,
                writer_queue_capacity,
            )?;
            let store =
                Arc::new(KvStore::initialize_format(driver, initial_stream_lineages).await?);
            Ok(InitializedStore::native(store))
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
                    reply_byte_limit: settings.reply_byte_limit,
                    transaction_byte_limit: settings.transaction_byte_limit,
                    report_conflicting_keys: false,
                    attempt_deadlines: _runtime_options.attempt_deadlines,
                },
            )
            .await?;
            let store =
                Arc::new(KvStore::initialize_format(driver, initial_stream_lineages).await?);
            Ok(InitializedStore::native(store))
        }
        #[cfg(feature = "remote")]
        StorageBackend::Remote => {
            let settings = config.remote.ok_or_else(|| {
                StorageError::validation("remote backend requires remote configuration settings")
            })?;
            let scheduled_job_store =
                storage_remote::build_scheduled_job_store(settings.clone()).await?;
            let provider = Arc::new(RemoteStorageProvider::new(settings).await?);
            Ok(InitializedStore {
                kind: StoreKind::Remote {
                    provider,
                    jobs: scheduled_job_store,
                },
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
