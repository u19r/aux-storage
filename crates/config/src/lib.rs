#![doc(hidden)]

#[cfg(test)]
pub mod src_tests;
#[cfg(test)]
mod sync_replication_tests;

mod compile_time;

pub mod runtime;
pub mod startup;

#[cfg(test)]
mod startup_tests;

mod backends;
mod cache;
mod constants;
mod error;
mod launch;
mod loader;
mod messaging;
mod model;
mod schema;
mod sync_replication;

pub use backends::{
    Backends, FoundationdbBackendConfig, PostgresBackendConfig, RemoteBackendConfig,
    RemoteCredentialsConfig, RemoteCredentialsConfigError, RemoteDefaultStorageMode,
    RemoteStaticCredentialsConfig, RemoteTimeoutOverrides, RocksdbBackendConfig,
    SqliteBackendConfig, StorageConnectionsConfig, TursoBackendConfig,
};
pub use cache::{
    CacheTtlOverridesConfig, DistributedCacheConfig, StoragePointReadCacheConfig,
    StoragePointReadCacheEvictionPolicy, StoragePointReadCacheMemoryBudgetConfig,
    StoragePointReadCacheMemoryBudgetMode, StorageQueryProofCacheConfig,
    StorageQueryProofCacheEvictionPolicy,
};
pub use compile_time::{CompileTimeManifest, CrateFeature, ManifestCrate};
pub use constants::{
    DEFAULT_STORAGE_ROCKS_DB_PATH, DEFAULT_STORAGE_SQLITE_DB_PATH, DEFAULT_STORAGE_TURSO_DB_PATH,
};
pub use error::ConfigError;
pub use launch::{
    LaunchInputs, StorageApiLaunchConfig, StorageApiLaunchEffectiveConfig, StorageBackendArg,
};
pub use loader::{Config, load, load_optional_with_overrides, load_with_overrides};
pub use messaging::{PubsubConfig, QueueConfig};
pub use model::{
    AppRole, Cors, Features, HttpConfig, HttpRoutesConfig, Jobs, MetricsConfig,
    PrometheusMetricsConfig, RootConfig, RuntimeEnvironment, RuntimeFeatures,
    SlowOperationLogThresholds, StorageReplicationConfig, StorageReplicationPeerConfig, Tracing,
    TracingTrace,
};
pub use sync_replication::{StorageSyncReplicationConfig, StorageSyncReplicationPeerConfig};

pub type RemoteStaticCredentials = RemoteStaticCredentialsConfig;
