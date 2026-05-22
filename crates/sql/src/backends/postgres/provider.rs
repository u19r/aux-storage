use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bg_jobs::JobManager;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rustls::{ClientConfig, RootCertStore};
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};
use tokio::sync::RwLock;
use tokio_postgres::{Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::Span;

use super::sql_statements;

#[derive(Clone)]
pub struct PostgresStorageProvider {
    pub(super) pool: Arc<Pool>,
    pub(crate) job_manager: JobManager,
    pub(crate) table_info_cache: Arc<RwLock<HashMap<TableName, Arc<StoredTableInfo>>>>,
    pub(crate) ttl_config_cache: Arc<RwLock<HashMap<TableName, CachedTtlConfig>>>,
    pub(crate) immediate_gsi_consistency: bool,
}

const TTL_CONFIG_CACHE_TTL: Duration = Duration::from_secs(60);
pub(crate) const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;
pub(crate) const POSTGRES_MAX_CONFLICT_RETRIES: u32 = 8;
pub(crate) const POSTGRES_BASE_BACKOFF_MS: u64 = 5;

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
    pub async fn new(connection_string: &str, max_pool_size: usize) -> StorageResult<Self> {
        Self::new_with_tls(connection_string, max_pool_size, true).await
    }

    pub async fn new_with_tls(
        connection_string: &str,
        max_pool_size: usize,
        tls_enabled: bool,
    ) -> StorageResult<Self> {
        let config: Config = connection_string
            .parse()
            .map_err(|err| StorageError::validation(format!("invalid postgres dsn: {err}")))?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let pool = if tls_enabled {
            let tls_connector = build_tls_connector()?;
            let manager = Manager::from_config(config.clone(), tls_connector, mgr_config);
            Pool::builder(manager)
                .max_size(max_pool_size)
                .build()
                .map_err(|err| {
                    StorageError::internal(&format!("failed to build postgres pool: {err}"))
                })?
        } else {
            let manager = Manager::from_config(config, NoTls, mgr_config);
            Pool::builder(manager)
                .max_size(max_pool_size)
                .build()
                .map_err(|err| {
                    StorageError::internal(&format!("failed to build postgres pool: {err}"))
                })?
        };

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
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_config_cache: Arc::new(RwLock::new(HashMap::new())),
            immediate_gsi_consistency: false,
        })
    }

    #[must_use]
    pub fn with_pool(pool: Pool) -> Self {
        Self {
            pool: Arc::new(pool),
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_config_cache: Arc::new(RwLock::new(HashMap::new())),
            immediate_gsi_consistency: false,
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
