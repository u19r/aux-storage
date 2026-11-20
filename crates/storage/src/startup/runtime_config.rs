use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use storage_common::{DatabaseJobIntervals, JobIntervalMillis};
use storage_provider::{StorageBackend, StorageConnectionRegistry};
use storage_types::{AttemptDeadlines, StorageError, StorageResult};

use super::storage_config::storage_connection_registry_from_features;
use crate::{DatabaseManager, DatabaseManagerRuntimeOptions, admission::AdmissionConfig};

/// The storage settings of one file configuration, translated once: the
/// connection registry and every runtime option the file holds. The host sets
/// the options the file does not hold (`enable_single_table_mode`,
/// `metrics_facade`, `scheduled_job_store_config`) on `options` before `open`.
pub struct RuntimeConfig {
    pub registry: StorageConnectionRegistry,
    pub options: DatabaseManagerRuntimeOptions,
}

impl RuntimeConfig {
    pub fn from_file(root: &config::RootConfig) -> StorageResult<Self> {
        let features = &root.features;
        let registry = storage_connection_registry_from_features(features)?;
        let regional_connections = regional_connections_from_file(&features.regional_connections)?;
        let admission_config = AdmissionConfig::from_config(&features.storage_admission)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let attempt_deadlines = attempt_deadlines_from_file(&features.storage_deadlines)?;
        let options = DatabaseManagerRuntimeOptions {
            enable_database_jobs: features.runtime.enable_background_workers,
            database_job_intervals: DatabaseJobIntervals {
                gsi_backfill_interval_ms: JobIntervalMillis(root.jobs.gsi_backfill_interval_ms),
                ..DatabaseJobIntervals::default()
            },
            admission_config,
            attempt_deadlines,
            regional_connections,
            ..DatabaseManagerRuntimeOptions::default()
        };
        Ok(Self { registry, options })
    }

    /// Backend of the default connection, for the startup line.
    #[must_use]
    pub fn default_backend(&self) -> Option<&StorageBackend> {
        self.registry
            .connections
            .get(&self.registry.default_connection_id)
            .map(|connection| &connection.backend_type)
    }

    pub async fn open(self) -> StorageResult<DatabaseManager> {
        DatabaseManager::new_with_connection_registry_and_runtime_options(
            self.registry,
            self.options,
        )
        .await
    }
}

/// Load each configured regional file. Without one (multi-region off) no
/// file is read and the map is allocated with capacity 0.
fn regional_connections_from_file(
    files: &HashMap<String, PathBuf>,
) -> StorageResult<HashMap<String, Arc<regional_replication::RegionalConfig>>> {
    let mut connections = HashMap::with_capacity(files.len());
    for (connection_id, path) in files {
        let config = regional_replication::RegionalConfig::load(path)?;
        connections.insert(connection_id.clone(), Arc::new(config));
    }
    Ok(connections)
}

/// Convert the configured millisecond budgets, in `DeadlineClass::ALL` order.
fn attempt_deadlines_from_file(
    deadlines: &config::StorageDeadlinesConfig,
) -> StorageResult<AttemptDeadlines> {
    AttemptDeadlines::try_new(
        [
            Duration::from_millis(deadlines.point_read_ms),
            Duration::from_millis(deadlines.range_read_ms),
            Duration::from_millis(deadlines.write_ms),
            Duration::from_millis(deadlines.transaction_ms),
            Duration::from_millis(deadlines.schema_change_ms),
            Duration::from_millis(deadlines.background_page_ms),
            Duration::from_millis(deadlines.long_poll_probe_ms),
        ],
        Duration::from_millis(deadlines.commit_grace_ms),
    )
}
