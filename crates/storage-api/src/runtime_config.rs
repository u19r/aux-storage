use std::time::Duration;

use config::Tracing;
use storage_provider::StorageConfig;
use storage_types::{StorageError, StorageResult};
use tracing_subscriber::EnvFilter;

pub fn storage_config_from_backends(backends: &config::Backends) -> StorageResult<StorageConfig> {
    storage::startup::storage_config_from_backends(backends)
}

pub fn ensure_backend_matches(backends: &config::Backends) -> StorageResult<()> {
    if backends.remote.is_some() {
        return Err(StorageError::internal(
            &"storage-api does not support remote backend in standalone mode".to_string(),
        ));
    }

    storage::startup::ensure_backend_matches(backends)
        .map_err(|message| StorageError::internal(&message))
}

pub fn shutdown_grace_period() -> Duration {
    std::env::var("APP_SHUTDOWN_GRACE_SECONDS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or_else(|| Duration::from_secs(5), Duration::from_secs)
}

pub fn resolve_filter(tracing_cfg: &Tracing) -> (EnvFilter, FilterSource) {
    if let Some(spec) = tracing_cfg.log_level.as_deref() {
        match EnvFilter::try_new(spec) {
            Ok(filter) => return (filter, FilterSource::Config),
            Err(err) => {
                eprintln!(
                    "Invalid log level '{spec}' in config ({err}), falling back to default filter"
                );
            }
        }
    }
    (EnvFilter::new("warn"), FilterSource::Default)
}

#[derive(Debug, Copy, Clone)]
pub enum FilterSource {
    Config,
    Default,
}

impl std::fmt::Display for FilterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterSource::Config => write!(f, "config"),
            FilterSource::Default => write!(f, "default"),
        }
    }
}
