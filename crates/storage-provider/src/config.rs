use std::collections::HashMap;

use storage_types::{
    FOUNDATIONDB_DEFAULT_CACHE_READ_VERSION_MS, FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
    StorageError, StorageResult,
};

/// `Debug` redacts `connection_string`, which holds the Postgres DSN.
#[derive(Clone)]
pub struct StorageConfig {
    pub backend_type: StorageBackend,
    pub initial_stream_lineages: Option<storage_types::InitialStreamLineages>,
    pub connection_string: Option<String>,
    pub file_path: Option<String>,
    pub sqlite: Option<SqliteSettings>,
    pub postgres: Option<PostgresSettings>,
    pub turso: Option<TursoSettings>,
    pub rocksdb: Option<RocksdbSettings>,
    pub foundationdb: Option<FoundationDbSettings>,
    pub remote: Option<RemoteStorageSettings>,
}

impl StorageConfig {
    /// Local database path of a SQLite or RocksDB connection:
    /// `connection_string`, then `file_path`. The one place that orders the two
    /// fields.
    #[must_use]
    pub fn local_path(&self) -> Option<&str> {
        self.connection_string
            .as_deref()
            .or(self.file_path.as_deref())
    }
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageConfig")
            .field("backend_type", &self.backend_type)
            .field("initial_stream_lineages", &self.initial_stream_lineages)
            .field("connection_string", &format_args!("<redacted>"))
            .field("file_path", &self.file_path)
            .field("sqlite", &self.sqlite)
            .field("postgres", &self.postgres)
            .field("turso", &self.turso)
            .field("rocksdb", &self.rocksdb)
            .field("foundationdb", &self.foundationdb)
            .field("remote", &self.remote)
            .finish()
    }
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
    pub const fn default_initial_stream_lineages(&self) -> storage_types::InitialStreamLineages {
        match self {
            Self::Postgres | Self::FoundationDb => storage_types::InitialStreamLineages::FOUR,
            _ => storage_types::InitialStreamLineages::ONE,
        }
    }
}

/// SQLite has no provider settings; `StorageConfig::sqlite` stays `Some` for a
/// SQLite connection.
#[derive(Debug, Clone, Default)]
pub struct SqliteSettings {}

/// Turso has no provider settings; `StorageConfig::turso` stays `Some` for a
/// Turso connection.
#[derive(Debug, Clone, Default)]
pub struct TursoSettings {}

/// `Debug` redacts the DSN, which can carry a password.
#[derive(Clone)]
pub struct PostgresSettings {
    pub dsn: String,
    pub max_pool_size: usize,
    pub tls: bool,
}

impl std::fmt::Debug for PostgresSettings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresSettings")
            .field("dsn", &format_args!("<redacted>"))
            .field("max_pool_size", &self.max_pool_size)
            .field("tls", &self.tls)
            .finish()
    }
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            max_pool_size: default_postgres_max_pool_size(),
            tls: true,
        }
    }
}

fn default_postgres_max_pool_size() -> usize {
    std::thread::available_parallelism()
        .map_or(20, |cores| usize::from(cores) + 8)
        .max(20)
}

/// Write attempts queued or running on the one RocksDB writer; later ones wait
/// for a slot until their deadline. Equals the configuration default.
pub const ROCKSDB_DEFAULT_WRITER_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct RocksdbSettings {
    pub writer_queue_capacity: usize,
}

impl Default for RocksdbSettings {
    fn default() -> Self {
        Self {
            writer_queue_capacity: ROCKSDB_DEFAULT_WRITER_QUEUE_CAPACITY,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FoundationDbSettings {
    pub transaction_byte_limit: Option<std::num::NonZeroUsize>,
    pub cluster_file: Option<String>,
    pub tenant_name: Option<String>,
    pub subspace_prefix: Option<String>,
    pub cache_read_version_ms: u16,
    /// Native range reply bytes; see `FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT`.
    pub reply_byte_limit: u32,
}

impl Default for FoundationDbSettings {
    fn default() -> Self {
        Self {
            transaction_byte_limit: None,
            cluster_file: None,
            tenant_name: None,
            subspace_prefix: None,
            cache_read_version_ms: FOUNDATIONDB_DEFAULT_CACHE_READ_VERSION_MS,
            reply_byte_limit: FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
        }
    }
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

/// `Debug` redacts every field: all three are credentials.
#[derive(Clone)]
pub struct RemoteStaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for RemoteStaticCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteStaticCredentials")
            .field("access_key_id", &format_args!("<redacted>"))
            .field("secret_access_key", &format_args!("<redacted>"))
            .field("session_token", &format_args!("<redacted>"))
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RemoteTimeoutOverrides {
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StorageConnectionRegistry {
    pub default_connection_id: String,
    pub connections: HashMap<String, StorageConfig>,
}
