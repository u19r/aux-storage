use std::sync::Arc;

use metrics_facade::MetricsFacade;
use storage_common::DatabaseJobIntervals;

use crate::admission::AdmissionConfig;

#[derive(Debug, Clone)]
pub struct DatabaseManagerRuntimeOptions {
    pub enable_database_jobs: bool,
    /// Native scheduled-job operation budget for capabilities on the
    /// initialized local store.
    pub scheduled_job_store_config: bg_job_store::ScheduledJobStoreConfig,
    /// Enables legacy aux single-table conveniences on generic raw write
    /// operations.
    ///
    /// Entity-specific manager APIs own their own metadata stamping and do not
    /// require this flag. Keep this option for older callers that intentionally
    /// want raw DynamoDB-compatible writes to receive single-table metadata.
    pub enable_single_table_mode: bool,
    pub database_job_intervals: DatabaseJobIntervals,
    pub metrics_facade: Option<Arc<dyn MetricsFacade>>,
    /// Per-connection foreground admission settings. Manager dispatch retains
    /// each permit through provider settlement, including response
    /// cancellation.
    pub admission_config: AdmissionConfig,
    /// Native attempt budgets given to each provider constructor.
    pub attempt_deadlines: storage_types::AttemptDeadlines,
    /// Validated regional files, keyed by native connection id (REG-9 starts
    /// their servers).
    pub regional_connections:
        std::collections::HashMap<String, Arc<regional_replication::RegionalConfig>>,
}

impl Default for DatabaseManagerRuntimeOptions {
    fn default() -> Self {
        Self {
            enable_database_jobs: true,
            scheduled_job_store_config: Default::default(),
            enable_single_table_mode: false,
            database_job_intervals: DatabaseJobIntervals::default(),
            metrics_facade: None,
            admission_config: AdmissionConfig::default(),
            attempt_deadlines: storage_types::AttemptDeadlines::default(),
            regional_connections: std::collections::HashMap::new(),
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
    pub fn scheduled_job_store_config(
        mut self,
        config: bg_job_store::ScheduledJobStoreConfig,
    ) -> Self {
        self.options.scheduled_job_store_config = config;
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
    pub fn database_job_intervals(mut self, database_job_intervals: DatabaseJobIntervals) -> Self {
        self.options.database_job_intervals = database_job_intervals;
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

    #[must_use]
    pub fn attempt_deadlines(mut self, attempt_deadlines: storage_types::AttemptDeadlines) -> Self {
        self.options.attempt_deadlines = attempt_deadlines;
        self
    }

    #[must_use]
    pub fn build(self) -> DatabaseManagerRuntimeOptions {
        self.options
    }
}
