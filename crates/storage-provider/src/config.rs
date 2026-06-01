use std::collections::HashMap;

use storage_types::{StorageError, StorageResult};

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend_type: StorageBackend,
    pub connection_string: Option<String>,
    pub file_path: Option<String>,
    pub sqlite: Option<SqliteSettings>,
    pub postgres: Option<PostgresSettings>,
    pub turso: Option<TursoSettings>,
    pub rocksdb: Option<RocksdbSettings>,
    pub foundationdb: Option<FoundationDbSettings>,
    pub remote: Option<RemoteStorageSettings>,
}

#[derive(Debug, Clone)]
pub enum StorageBackend {
    SQLite,
    Turso,
    Postgres,
    RocksDB,
    FoundationDb,
    Remote,
}

impl StorageBackend {
    #[must_use]
    pub const fn supports_multi_region_replication_control_plane(&self) -> bool {
        !matches!(self, Self::Remote)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SqliteSettings {
    pub immediate_gsi_consistency: bool,
    pub force_file_backed_database: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TursoSettings {
    pub immediate_gsi_consistency: bool,
}

#[derive(Debug, Clone)]
pub struct PostgresSettings {
    pub dsn: String,
    pub max_pool_size: usize,
    pub background_max_pool_size: usize,
    pub tls: bool,
    pub immediate_gsi_consistency: bool,
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            max_pool_size: default_postgres_max_pool_size(),
            background_max_pool_size: 4,
            tls: true,
            immediate_gsi_consistency: false,
        }
    }
}

fn default_postgres_max_pool_size() -> usize {
    std::thread::available_parallelism()
        .map_or(20, |cores| usize::from(cores) + 8)
        .max(20)
}

#[derive(Debug, Clone, Default)]
pub struct RocksdbSettings {
    pub immediate_gsi_consistency: bool,
}

#[derive(Debug, Clone, Default)]
pub struct FoundationDbSettings {
    pub cluster_file: Option<String>,
    pub tenant_name: Option<String>,
    pub subspace_prefix: Option<String>,
    pub cache_read_version_ms: u16,
    pub immediate_gsi_consistency: bool,
}

#[derive(Debug, Clone)]
pub struct RemoteStorageSettings {
    pub endpoint_urls: Vec<String>,
    pub region: Option<String>,
    pub tls: bool,
    pub credentials: RemoteCredentialStrategy,
    pub timeouts: Option<RemoteTimeoutOverrides>,
}

impl RemoteStorageSettings {
    pub fn validate(&self) -> StorageResult<()> {
        if self.endpoint_urls.is_empty() {
            return Err(StorageError::validation(
                "remote storage requires at least one endpoint URL",
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
pub struct StorageConnectionRegistry {
    pub default_connection_id: String,
    pub connections: HashMap<String, StorageConnectionConfig>,
}

#[derive(Debug, Clone)]
pub struct StorageConnectionConfig {
    pub backend_type: StorageBackend,
    pub connection_string: Option<String>,
    pub file_path: Option<String>,
    pub sqlite: Option<SqliteSettings>,
    pub postgres: Option<PostgresSettings>,
    pub turso: Option<TursoSettings>,
    pub rocksdb: Option<RocksdbSettings>,
    pub foundationdb: Option<FoundationDbSettings>,
    pub remote: Option<RemoteStorageSettings>,
}
