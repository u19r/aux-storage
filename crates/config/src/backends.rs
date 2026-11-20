use std::{collections::HashMap, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage_types::{
    FOUNDATIONDB_DEFAULT_CACHE_READ_VERSION_MS, FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
    FOUNDATIONDB_MAX_REPLY_BYTE_LIMIT, FOUNDATIONDB_MIN_REPLY_BYTE_LIMIT,
};

use crate::constants::{
    DEFAULT_REMOTE_REGION, DEFAULT_STORAGE_ROCKS_DB_PATH, DEFAULT_STORAGE_SQLITE_DB_PATH,
    DEFAULT_STORAGE_TURSO_DB_PATH,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Backends {
    /// Initial roots for an absent provider scope; existing topology is
    /// preserved.
    #[serde(default)]
    pub initial_stream_lineages: Option<storage_types::InitialStreamLineages>,
    #[serde(default)]
    pub sqlite: Option<SqliteBackendConfig>,
    #[serde(default)]
    pub turso: Option<TursoBackendConfig>,
    #[serde(default)]
    pub postgres: Option<PostgresBackendConfig>,
    #[serde(default)]
    pub rocksdb: Option<RocksdbBackendConfig>,
    #[serde(default)]
    pub foundationdb: Option<FoundationdbBackendConfig>,
    #[serde(default)]
    pub remote: Option<RemoteBackendConfig>,
}

impl Backends {
    #[must_use]
    pub fn sqlite_default() -> Self {
        Self {
            initial_stream_lineages: None,
            sqlite: Some(SqliteBackendConfig::default()),
            turso: None,
            postgres: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        }
    }
}

impl Default for Backends {
    fn default() -> Self {
        Self::sqlite_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StorageConnectionsConfig {
    pub default_connection: String,
    pub connections: HashMap<String, Backends>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SqliteBackendConfig {
    #[serde(default = "default_sqlite_db_path")]
    #[schemars(default = "default_sqlite_db_path")]
    pub db_path: String,
}

impl Default for SqliteBackendConfig {
    fn default() -> Self {
        Self {
            db_path: default_sqlite_db_path(),
        }
    }
}

fn default_sqlite_db_path() -> String {
    DEFAULT_STORAGE_SQLITE_DB_PATH.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TursoBackendConfig {
    #[serde(default = "default_turso_db_path")]
    #[schemars(default = "default_turso_db_path")]
    pub db_path: String,
}

impl Default for TursoBackendConfig {
    fn default() -> Self {
        Self {
            db_path: default_turso_db_path(),
        }
    }
}

fn default_turso_db_path() -> String {
    DEFAULT_STORAGE_TURSO_DB_PATH.to_string()
}

/// `Debug` redacts the DSN, which can carry a password.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PostgresBackendConfig {
    pub dsn: String,
    #[serde(default = "default_postgres_max_pool_size")]
    #[schemars(default = "default_postgres_max_pool_size")]
    pub max_pool_size: usize,
    #[serde(default)]
    #[schemars(default)]
    pub tls: bool,
}

impl fmt::Debug for PostgresBackendConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresBackendConfig")
            .field("dsn", &format_args!("<redacted>"))
            .field("max_pool_size", &self.max_pool_size)
            .field("tls", &self.tls)
            .finish()
    }
}

impl PostgresBackendConfig {
    /// Defaults for every setting except the DSN.
    #[must_use]
    pub fn new(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            max_pool_size: default_postgres_max_pool_size(),
            tls: false,
        }
    }
}

fn default_postgres_max_pool_size() -> usize {
    20
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RocksdbBackendConfig {
    #[serde(default = "default_rocksdb_path")]
    #[schemars(default = "default_rocksdb_path")]
    pub db_path: String,
    #[serde(default = "default_rocksdb_writer_queue_capacity")]
    #[schemars(default = "default_rocksdb_writer_queue_capacity")]
    pub writer_queue_capacity: usize,
}

impl Default for RocksdbBackendConfig {
    fn default() -> Self {
        Self {
            db_path: default_rocksdb_path(),
            writer_queue_capacity: default_rocksdb_writer_queue_capacity(),
        }
    }
}

fn default_rocksdb_writer_queue_capacity() -> usize {
    256
}

fn default_rocksdb_path() -> String {
    DEFAULT_STORAGE_ROCKS_DB_PATH.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FoundationdbBackendConfig {
    /// Enforced on every native attempt; absent preserves database defaults.
    #[serde(default)]
    #[schemars(default)]
    pub transaction_byte_limit: Option<std::num::NonZeroUsize>,
    #[serde(default)]
    #[schemars(default)]
    pub cluster_file: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub tenant_name: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub subspace_prefix: Option<String>,
    #[serde(default = "default_foundationdb_cache_read_version_ms")]
    #[schemars(default = "default_foundationdb_cache_read_version_ms")]
    pub cache_read_version_ms: u16,
    /// Most bytes of one native range reply (the client `reply_byte_limit`
    /// knob), set once per process before the network starts.
    #[serde(default = "default_foundationdb_reply_byte_limit")]
    #[schemars(
        default = "default_foundationdb_reply_byte_limit",
        range(min = FOUNDATIONDB_MIN_REPLY_BYTE_LIMIT, max = FOUNDATIONDB_MAX_REPLY_BYTE_LIMIT)
    )]
    pub reply_byte_limit: u32,
}

impl Default for FoundationdbBackendConfig {
    fn default() -> Self {
        Self {
            transaction_byte_limit: None,
            cluster_file: None,
            tenant_name: None,
            subspace_prefix: None,
            cache_read_version_ms: default_foundationdb_cache_read_version_ms(),
            reply_byte_limit: default_foundationdb_reply_byte_limit(),
        }
    }
}

fn default_foundationdb_cache_read_version_ms() -> u16 {
    FOUNDATIONDB_DEFAULT_CACHE_READ_VERSION_MS
}

fn default_foundationdb_reply_byte_limit() -> u32 {
    FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteBackendConfig {
    #[serde(default)]
    #[schemars(default)]
    pub endpoint_urls: Vec<String>,
    #[serde(default = "default_remote_region")]
    #[schemars(default = "default_remote_region")]
    pub region: Option<String>,
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub tls: bool,
    #[serde(default)]
    #[schemars(default)]
    pub credentials: Option<RemoteCredentialsConfig>,
    #[serde(default)]
    #[schemars(default)]
    pub timeout_overrides: Option<RemoteTimeoutOverrides>,
}

fn default_remote_region() -> Option<String> {
    Some(DEFAULT_REMOTE_REGION.to_string())
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteCredentialsConfig {
    #[serde(default)]
    #[schemars(default)]
    pub r#static: Option<RemoteStaticCredentialsConfig>,
}

/// `Debug` redacts every field: all three are credentials.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteStaticCredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    #[schemars(default)]
    pub session_token: Option<String>,
}

impl fmt::Debug for RemoteStaticCredentialsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteStaticCredentialsConfig")
            .field("access_key", &format_args!("<redacted>"))
            .field("secret_key", &format_args!("<redacted>"))
            .field("session_token", &format_args!("<redacted>"))
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteTimeoutOverrides {
    /// Connect timeout. Default: a quarter of `request_timeout_ms`, at most
    /// 2000 (2000 without a request timeout). When both are set it must be
    /// less than `request_timeout_ms`, or the config is rejected at load: only
    /// a connect that fails first is known not sent; a write cut off later
    /// answers an unknown outcome.
    #[serde(default)]
    #[schemars(default)]
    pub connect_timeout_ms: Option<u64>,
    /// Whole-request timeout, connect included. Default: none.
    #[serde(default)]
    #[schemars(default)]
    pub request_timeout_ms: Option<u64>,
}
