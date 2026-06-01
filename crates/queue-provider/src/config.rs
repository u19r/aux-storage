use storage_types::{StorageError, StorageResult};

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub backend_type: QueueBackend,
    pub connection_string: Option<String>,
    pub file_path: Option<String>,
    pub postgres: Option<PostgresSettings>,
    pub foundationdb: Option<FoundationDbSettings>,
    pub remote: Option<RemoteQueueSettings>,
}

#[derive(Debug, Clone)]
pub enum QueueBackend {
    SQLite,
    Turso,
    Postgres,
    RocksDB,
    FoundationDb,
    Remote,
}

#[derive(Debug, Clone)]
pub struct PostgresSettings {
    pub dsn: String,
    pub max_pool_size: usize,
    pub background_max_pool_size: usize,
    pub tls: bool,
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            max_pool_size: default_postgres_max_pool_size(),
            background_max_pool_size: 4,
            tls: true,
        }
    }
}

fn default_postgres_max_pool_size() -> usize {
    std::thread::available_parallelism()
        .map_or(20, |cores| usize::from(cores) + 8)
        .max(20)
}

#[derive(Debug, Clone, Default)]
pub struct FoundationDbSettings {
    pub cluster_file: Option<String>,
    pub tenant_name: Option<String>,
    pub subspace_prefix: Option<String>,
    pub cache_read_version_ms: u16,
    pub report_conflicting_keys: bool,
}

#[derive(Debug, Clone)]
pub struct RemoteQueueSettings {
    pub endpoint_urls: Vec<String>,
    pub region: Option<String>,
    pub tls: bool,
    pub credentials: RemoteCredentialStrategy,
    pub timeouts: Option<RemoteTimeoutOverrides>,
    pub sigv4: RemoteSigv4Settings,
}

impl RemoteQueueSettings {
    pub fn validate(&self) -> StorageResult<()> {
        if self.endpoint_urls.is_empty() {
            return Err(StorageError::validation(
                "remote queue requires at least one endpoint URL",
            ));
        }
        if self.sigv4.enabled && self.region.as_deref().is_none_or(str::is_empty) {
            return Err(StorageError::validation(
                "remote queue with SigV4 enabled requires a region",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum RemoteCredentialStrategy {
    DefaultChain,
    Static(RemoteStaticCredentials),
}

#[derive(Debug, Clone)]
pub struct RemoteStaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RemoteTimeoutOverrides {
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RemoteSigv4Settings {
    pub enabled: bool,
    pub service_name: String,
}

impl Default for RemoteSigv4Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "queue".to_string(),
        }
    }
}
