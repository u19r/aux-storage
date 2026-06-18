use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bg_jobs::JobManager;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rustls::{ClientConfig, RootCertStore};
use storage_common::{DatabaseJobIntervals, GsiPropagationGovernor};
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_postgres::{Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::Span;

use super::sql_statements;

#[derive(Clone)]
pub struct PostgresStorageProvider {
    pub(super) pool: Arc<Pool>,
    pub(super) background_pool: Arc<Pool>,
    pub(super) background_work_limit: Arc<Semaphore>,
    pub(super) foreground_write_limit: Arc<Semaphore>,
    pub(crate) job_manager: JobManager,
    pub(crate) table_info_cache: Arc<RwLock<HashMap<TableName, Arc<StoredTableInfo>>>>,
    pub(crate) ttl_config_cache: Arc<RwLock<HashMap<TableName, CachedTtlConfig>>>,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) database_job_intervals: DatabaseJobIntervals,
    pub(crate) gsi_propagation_governor: Arc<GsiPropagationGovernor>,
}

const TTL_CONFIG_CACHE_TTL: Duration = Duration::from_secs(60);
pub(crate) const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;
pub(crate) const POSTGRES_MAX_CONFLICT_RETRIES: u32 = 8;
pub(crate) const POSTGRES_BASE_BACKOFF_MS: u64 = 5;
const POSTGRES_FOREGROUND_POOL_LANE: &str = "foreground";
const POSTGRES_BACKGROUND_POOL_LANE: &str = "background";

#[derive(Clone)]
pub(crate) struct CachedTtlConfig {
    config: Option<storage_common::ttl::TtlConfigRecord>,
    cached_at: Instant,
}

impl CachedTtlConfig {
    pub(crate) fn new(config: Option<storage_common::ttl::TtlConfigRecord>) -> Self {
        Self {
            config,
            cached_at: Instant::now(),
        }
    }

    pub(crate) fn is_fresh(&self) -> bool {
        self.cached_at.elapsed() < TTL_CONFIG_CACHE_TTL
    }

    pub(crate) fn config(&self) -> Option<storage_common::ttl::TtlConfigRecord> {
        self.config.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KeyColumnBinding {
    pub(super) column: String,
    pub(super) attribute_type: storage_types::KeyAttributeType,
    pub(super) value: String,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderedKeyColumn {
    pub(super) column: String,
    pub(super) attribute_type: storage_types::KeyAttributeType,
}

pub(crate) fn record_read(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_returned", items as u64);
    span.record("bytes_read", bytes as u64);
}

pub(crate) fn record_write(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_updated", items as u64);
    span.record("bytes_written", bytes as u64);
}

impl PostgresStorageProvider {
    pub(crate) async fn acquire_client(
        &self,
        operation: &'static str,
    ) -> StorageResult<deadpool_postgres::Client> {
        self.acquire_client_from_pool(operation, POSTGRES_FOREGROUND_POOL_LANE, &self.pool)
            .await
    }

    pub(crate) async fn acquire_background_client(
        &self,
        operation: &'static str,
    ) -> StorageResult<deadpool_postgres::Client> {
        self.acquire_client_from_pool(
            operation,
            POSTGRES_BACKGROUND_POOL_LANE,
            &self.background_pool,
        )
        .await
    }

    async fn acquire_client_from_pool(
        &self,
        operation: &'static str,
        lane: &'static str,
        pool: &Pool,
    ) -> StorageResult<deadpool_postgres::Client> {
        let _ = (operation, lane);
        pool.get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)
    }

    pub(crate) async fn acquire_background_work_permit(
        &self,
    ) -> StorageResult<OwnedSemaphorePermit> {
        self.background_work_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| {
                StorageError::internal(&format!("postgres background work limit closed: {err}"))
            })
    }

    pub(crate) async fn acquire_foreground_write_permit(
        &self,
        operation: &'static str,
    ) -> StorageResult<OwnedSemaphorePermit> {
        let _ = operation;
        self.foreground_write_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| {
                StorageError::internal(&format!("postgres foreground write limit closed: {err}"))
            })
    }

    pub(crate) async fn begin_transaction<'a>(
        &self,
        client: &'a mut deadpool_postgres::Client,
        operation: &'static str,
        context: &'static str,
    ) -> StorageResult<deadpool_postgres::Transaction<'a>> {
        let _ = operation;
        client
            .transaction()
            .await
            .map_err(|err| Self::map_postgres_write_error(context, err))
    }

    pub(crate) fn connection_hold_timer(
        &self,
        operation: &'static str,
    ) -> PostgresConnectionHoldTimer {
        let _ = operation;
        PostgresConnectionHoldTimer
    }

    pub(crate) fn transaction_hold_timer(
        &self,
        operation: &'static str,
    ) -> PostgresTransactionHoldTimer {
        let _ = operation;
        PostgresTransactionHoldTimer
    }

    pub(crate) fn record_transaction_phase(
        &self,
        operation: &'static str,
        phase: &'static str,
        elapsed: Duration,
    ) {
        let _ = (operation, phase, elapsed);
    }

    pub async fn new(connection_string: &str, max_pool_size: usize) -> StorageResult<Self> {
        Self::new_with_tls(
            connection_string,
            max_pool_size,
            default_background_pool_size(max_pool_size),
            true,
        )
        .await
    }

    pub async fn new_with_tls(
        connection_string: &str,
        max_pool_size: usize,
        background_max_pool_size: usize,
        tls_enabled: bool,
    ) -> StorageResult<Self> {
        let config: Config = connection_string
            .parse()
            .map_err(|err| StorageError::validation(format!("invalid postgres dsn: {err}")))?;

        let pool = build_pool(config.clone(), max_pool_size, tls_enabled)?;
        let background_pool = build_pool(config, background_max_pool_size, tls_enabled)?;

        {
            let client = pool.get().await.map_err(|err| {
                StorageError::internal(&format!("failed to connect to postgres: {err}"))
            })?;
            client
                .simple_query(sql_statements::select_one())
                .await
                .map_err(|err| StorageError::internal(&format!("postgres ping failed: {err}")))?;
        }

        Ok(Self {
            pool: Arc::new(pool),
            background_pool: Arc::new(background_pool),
            background_work_limit: Arc::new(Semaphore::new(background_max_pool_size)),
            foreground_write_limit: Arc::new(Semaphore::new(default_foreground_write_permits(
                max_pool_size,
            ))),
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_config_cache: Arc::new(RwLock::new(HashMap::new())),
            immediate_gsi_consistency: false,
            database_job_intervals: DatabaseJobIntervals::default(),
            gsi_propagation_governor: Arc::new(GsiPropagationGovernor::default()),
        })
    }

    #[must_use]
    pub fn with_pool(pool: Pool) -> Self {
        let max_pool_size = pool.status().max_size;
        let pool = Arc::new(pool);
        Self {
            pool: pool.clone(),
            background_pool: pool,
            background_work_limit: Arc::new(Semaphore::new(1)),
            foreground_write_limit: Arc::new(Semaphore::new(default_foreground_write_permits(
                max_pool_size,
            ))),
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_config_cache: Arc::new(RwLock::new(HashMap::new())),
            immediate_gsi_consistency: false,
            database_job_intervals: DatabaseJobIntervals::default(),
            gsi_propagation_governor: Arc::new(GsiPropagationGovernor::default()),
        }
    }

    #[must_use]
    pub fn with_job_manager(mut self, job_manager: JobManager) -> Self {
        self.job_manager = job_manager;
        self
    }

    #[must_use]
    pub fn with_immediate_gsi_consistency(mut self, enabled: bool) -> Self {
        self.immediate_gsi_consistency = enabled;
        self
    }

    #[must_use]
    pub fn with_database_job_intervals(mut self, intervals: DatabaseJobIntervals) -> Self {
        self.database_job_intervals = intervals;
        self
    }
}

pub(crate) struct PostgresConnectionHoldTimer;

pub(crate) struct PostgresTransactionHoldTimer;

fn default_background_pool_size(max_pool_size: usize) -> usize {
    (max_pool_size / 4).clamp(1, 4)
}

fn default_foreground_write_permits(max_pool_size: usize) -> usize {
    max_pool_size.max(1)
}

fn build_pool(config: Config, max_pool_size: usize, tls_enabled: bool) -> StorageResult<Pool> {
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };

    if tls_enabled {
        let tls_connector = build_tls_connector()?;
        let manager = Manager::from_config(config, tls_connector, mgr_config);
        Pool::builder(manager)
            .max_size(max_pool_size)
            .build()
            .map_err(|err| StorageError::internal(&format!("failed to build postgres pool: {err}")))
    } else {
        let manager = Manager::from_config(config, NoTls, mgr_config);
        Pool::builder(manager)
            .max_size(max_pool_size)
            .build()
            .map_err(|err| StorageError::internal(&format!("failed to build postgres pool: {err}")))
    }
}

fn build_tls_connector() -> StorageResult<MakeRustlsConnect> {
    let mut roots = RootCertStore::empty();
    let certs = rustls_native_certs::load_native_certs();
    if certs.errors.is_empty() && certs.certs.is_empty() {
        return Err(StorageError::internal(
            "no system root certificates were discovered for postgres TLS",
        ));
    }

    for cert in certs.certs {
        roots.add(cert).map_err(|err| {
            StorageError::internal(&format!(
                "failed to register a system root certificate for postgres TLS: {err}"
            ))
        })?;
    }

    let client_config =
        ClientConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|err| {
                StorageError::internal(&format!(
                    "failed to configure rustls protocol versions for postgres TLS: {err}"
                ))
            })?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(MakeRustlsConnect::new(client_config))
}
