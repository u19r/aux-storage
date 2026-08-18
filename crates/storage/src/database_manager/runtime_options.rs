use std::sync::Arc;

use metrics_facade::MetricsFacade;
use storage_common::DatabaseJobIntervals;

#[cfg(all(test, feature = "cache-write-planner"))]
use super::DatabaseManagerTestPauseHandle;
use crate::{admission::AdmissionConfig, cache_coordinator::StorageAuthoritativeCacheOptions};

#[derive(Debug, Clone)]
pub struct DatabaseManagerRuntimeOptions {
    pub enable_database_jobs: bool,
    pub enable_background_refresh: bool,
    pub enable_background_watchers: bool,
    pub enable_single_node_sync_mode: bool,
    /// Enables legacy aux single-table conveniences on generic raw write
    /// operations.
    ///
    /// Entity-specific manager APIs own their own metadata stamping and do not
    /// require this flag. Keep this option for older callers that intentionally
    /// want raw DynamoDB-compatible writes to receive single-table metadata.
    pub enable_single_table_mode: bool,
    pub run_gsi_maintenance_after_write: Option<bool>,
    pub database_job_intervals: DatabaseJobIntervals,
    pub authoritative_cache_options: StorageAuthoritativeCacheOptions,
    pub metrics_facade: Option<Arc<dyn MetricsFacade>>,
    /// Per-connection foreground admission settings.  Provider call sites can
    /// migrate incrementally through `DatabaseManager::acquire_admission`.
    pub admission_config: AdmissionConfig,
    #[cfg(all(test, feature = "cache-write-planner"))]
    pub(crate) pause_after_storage_write: Option<DatabaseManagerTestPauseHandle>,
}

impl Default for DatabaseManagerRuntimeOptions {
    fn default() -> Self {
        Self {
            enable_database_jobs: true,
            enable_background_refresh: true,
            enable_background_watchers: true,
            enable_single_node_sync_mode: false,
            enable_single_table_mode: false,
            run_gsi_maintenance_after_write: None,
            database_job_intervals: DatabaseJobIntervals::default(),
            authoritative_cache_options: StorageAuthoritativeCacheOptions::default(),
            metrics_facade: None,
            admission_config: AdmissionConfig::default(),
            #[cfg(all(test, feature = "cache-write-planner"))]
            pause_after_storage_write: None,
        }
    }
}

impl DatabaseManagerRuntimeOptions {
    #[must_use]
    pub fn builder() -> DatabaseManagerRuntimeOptionsBuilder {
        DatabaseManagerRuntimeOptionsBuilder {
            options: Self::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseManagerRuntimeOptionsBuilder {
    options: DatabaseManagerRuntimeOptions,
}

impl DatabaseManagerRuntimeOptionsBuilder {
    #[must_use]
    pub fn enable_database_jobs(mut self, enable_database_jobs: bool) -> Self {
        self.options.enable_database_jobs = enable_database_jobs;
        self
    }

    #[must_use]
    pub fn enable_background_refresh(mut self, enable_background_refresh: bool) -> Self {
        self.options.enable_background_refresh = enable_background_refresh;
        self
    }

    #[must_use]
    pub fn enable_background_watchers(mut self, enable_background_watchers: bool) -> Self {
        self.options.enable_background_watchers = enable_background_watchers;
        self
    }

    #[must_use]
    pub fn enable_single_node_sync_mode(mut self, enable_single_node_sync_mode: bool) -> Self {
        self.options.enable_single_node_sync_mode = enable_single_node_sync_mode;
        self
    }

    #[must_use]
    /// Opts generic raw write operations into aux single-table conveniences.
    ///
    /// Prefer entity-specific manager APIs for single-table entities so callers
    /// do not depend on this storage-internal compatibility mode.
    pub fn enable_single_table_mode(mut self, enable_single_table_mode: bool) -> Self {
        self.options.enable_single_table_mode = enable_single_table_mode;
        self
    }

    #[must_use]
    pub fn run_gsi_maintenance_after_write(
        mut self,
        run_gsi_maintenance_after_write: Option<bool>,
    ) -> Self {
        self.options.run_gsi_maintenance_after_write = run_gsi_maintenance_after_write;
        self
    }

    #[must_use]
    pub fn database_job_intervals(mut self, database_job_intervals: DatabaseJobIntervals) -> Self {
        self.options.database_job_intervals = database_job_intervals;
        self
    }

    #[must_use]
    pub fn authoritative_cache_options(
        mut self,
        authoritative_cache_options: StorageAuthoritativeCacheOptions,
    ) -> Self {
        self.options.authoritative_cache_options = authoritative_cache_options;
        self
    }

    #[must_use]
    pub fn metrics_facade(mut self, metrics_facade: Arc<dyn MetricsFacade>) -> Self {
        self.options.metrics_facade = Some(metrics_facade);
        self
    }

    #[must_use]
    pub fn admission_config(mut self, admission_config: AdmissionConfig) -> Self {
        self.options.admission_config = admission_config;
        self
    }

    #[cfg(all(test, feature = "cache-write-planner"))]
    #[must_use]
    pub(crate) fn pause_after_storage_write(
        mut self,
        pause_after_storage_write: Option<DatabaseManagerTestPauseHandle>,
    ) -> Self {
        self.options.pause_after_storage_write = pause_after_storage_write;
        self
    }

    #[must_use]
    pub fn build(self) -> DatabaseManagerRuntimeOptions {
        self.options
    }
}
