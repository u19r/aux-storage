use std::{collections::HashMap, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::{
    DEFAULT_POSTGRES_MAX_POOL_SIZE, DEFAULT_REMOTE_REGION, DEFAULT_STORAGE_ROCKS_DB_PATH,
    DEFAULT_STORAGE_SQLITE_DB_PATH, DEFAULT_STORAGE_TURSO_DB_PATH,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Backends {
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
    #[serde(default)]
    #[schemars(default)]
    pub immediate_gsi_consistency: bool,
}

impl Default for SqliteBackendConfig {
    fn default() -> Self {
        Self {
            db_path: default_sqlite_db_path(),
            immediate_gsi_consistency: false,
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
    #[serde(default)]
    #[schemars(default)]
    pub immediate_gsi_consistency: bool,
}

impl Default for TursoBackendConfig {
    fn default() -> Self {
        Self {
            db_path: default_turso_db_path(),
            immediate_gsi_consistency: false,
        }
    }
}

fn default_turso_db_path() -> String {
    DEFAULT_STORAGE_TURSO_DB_PATH.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PostgresBackendConfig {
    pub dsn: String,
    #[serde(default = "default_postgres_max_pool_size")]
    #[schemars(default = "default_postgres_max_pool_size")]
    pub max_pool_size: usize,
    #[serde(default)]
    #[schemars(default)]
    pub tls: bool,
    #[serde(default)]
    #[schemars(default)]
    pub immediate_gsi_consistency: bool,
}

fn default_postgres_max_pool_size() -> usize {
    DEFAULT_POSTGRES_MAX_POOL_SIZE
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RocksdbBackendConfig {
    #[serde(default = "default_rocksdb_path")]
    #[schemars(default = "default_rocksdb_path")]
    pub db_path: String,
    #[serde(default)]
    #[schemars(default)]
    pub immediate_gsi_consistency: bool,
}

impl Default for RocksdbBackendConfig {
    fn default() -> Self {
        Self {
            db_path: default_rocksdb_path(),
            immediate_gsi_consistency: false,
        }
    }
}

fn default_rocksdb_path() -> String {
    DEFAULT_STORAGE_ROCKS_DB_PATH.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FoundationdbBackendConfig {
    #[serde(default)]
    #[schemars(default)]
    pub cluster_file: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub tenant_name: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub subspace_prefix: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub cache_read_version_ms: u16,
    #[serde(default)]
    #[schemars(default)]
    pub immediate_gsi_consistency: bool,
    #[serde(default)]
    #[schemars(default)]
    pub report_conflicting_keys: bool,
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
    pub default_storage_mode: RemoteDefaultStorageMode,
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteDefaultStorageMode {
    Shared,
    #[default]
    Dedicated,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteCredentialsConfig {
    #[serde(default)]
    #[schemars(default)]
    pub r#static: Option<RemoteStaticCredentialsConfig>,
    #[serde(default)]
    #[schemars(default)]
    pub instance_keys: Option<bool>,
}

impl RemoteCredentialsConfig {
    pub fn validate(&self) -> Result<(), RemoteCredentialsConfigError> {
        if self.r#static.is_some() && self.instance_keys == Some(true) {
            return Err(RemoteCredentialsConfigError::ConflictingCredentialSources);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteCredentialsConfigError {
    ConflictingCredentialSources,
}

impl fmt::Display for RemoteCredentialsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingCredentialSources => formatter.write_str(
                "remote credentials must use either static credentials or instance keys, not both",
            ),
        }
    }
}

impl std::error::Error for RemoteCredentialsConfigError {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteStaticCredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    #[schemars(default)]
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteTimeoutOverrides {
    #[serde(default)]
    #[schemars(default)]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default)]
    #[schemars(default)]
    pub request_timeout_ms: Option<u64>,
}
