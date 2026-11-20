use config::Tracing;
use storage_provider::{StorageBackend, StorageConnectionRegistry};
use storage_types::{StorageError, StorageResult};
use tracing_subscriber::EnvFilter;

/// storage-api serves local stores only; a remote connection is a client of
/// another storage-api.
pub(crate) fn require_local_connections(registry: &StorageConnectionRegistry) -> StorageResult<()> {
    if registry
        .connections
        .values()
        .any(|connection| matches!(connection.backend_type, StorageBackend::Remote))
    {
        return Err(StorageError::validation(
            "storage-api does not support remote storage connections",
        ));
    }
    Ok(())
}

pub(crate) fn resolve_filter(tracing_cfg: &Tracing) -> (EnvFilter, FilterSource) {
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
pub(crate) enum FilterSource {
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
