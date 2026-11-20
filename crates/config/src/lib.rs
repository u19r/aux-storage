#![doc(hidden)]

mod compile_time;
#[cfg(test)]
mod config_test_support_tests;
#[cfg(test)]
mod src_tests;

pub mod runtime;
pub mod startup;

#[cfg(test)]
mod startup_tests;

mod backends;
mod constants;
mod error;
pub(crate) mod launch;
mod loader;
mod messaging;
mod model;
mod remote_static_credentials;
mod schema;

pub use backends::{
    Backends, FoundationdbBackendConfig, PostgresBackendConfig, RemoteBackendConfig,
    RemoteCredentialsConfig, RemoteStaticCredentialsConfig, RemoteTimeoutOverrides,
    RocksdbBackendConfig, SqliteBackendConfig, StorageConnectionsConfig, TursoBackendConfig,
};
pub use compile_time::{CompileTimeManifest, CrateFeature, ManifestCrate};
pub use constants::{
    DEFAULT_STORAGE_ROCKS_DB_PATH, DEFAULT_STORAGE_SQLITE_DB_PATH, DEFAULT_STORAGE_TURSO_DB_PATH,
};
pub use error::ConfigError;
pub use launch::{StorageApiLaunchConfig, StorageApiLaunchEffectiveConfig};
pub use loader::{Config, load, load_optional_with_overrides, load_with_overrides};
pub use messaging::{PubsubConfig, PubsubRunnerConfig, QueueConfig};
pub use model::{
    Cors, Features, HttpConfig, Jobs, MetricsConfig, RootConfig, RuntimeFeatures,
    StorageAdmissionConfig, StorageDeadlinesConfig, Tracing,
};
pub use remote_static_credentials::RemoteStaticCredentials;
