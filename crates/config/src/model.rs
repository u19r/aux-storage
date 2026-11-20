use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    backends::{Backends, StorageConnectionsConfig},
    constants::{
        DEFAULT_HTTP_BIND_ADDR, DEFAULT_JOBS_GSI_BACKFILL_INTERVAL_MS,
        DEFAULT_RUNTIME_SHUTDOWN_GRACE_SECONDS, DEFAULT_TRACING_LOG_LEVEL,
    },
    messaging::{PubsubConfig, QueueConfig},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RootConfig {
    #[serde(rename = "$schema")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub description: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub http: HttpConfig,
    #[serde(default)]
    #[schemars(default)]
    pub features: Features,
    #[serde(default)]
    #[schemars(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    #[schemars(default)]
    pub pubsub: PubsubConfig,
    #[serde(default)]
    #[schemars(default)]
    pub jobs: Jobs,
}

impl RootConfig {
    #[must_use]
    pub fn background_workers_enabled(&self) -> bool {
        self.features.runtime.enable_background_workers
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_bind_addr")]
    #[schemars(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default)]
    #[schemars(default)]
    pub routes: HttpRoutesConfig,
    #[serde(default)]
    #[schemars(default)]
    pub cors: Cors,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            routes: HttpRoutesConfig::default(),
            cors: Cors::default(),
        }
    }
}

fn default_bind_addr() -> String {
    DEFAULT_HTTP_BIND_ADDR.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpRoutesConfig {
    #[serde(default = "default_storage_route")]
    #[schemars(default = "default_storage_route")]
    pub storage: String,
    #[serde(default = "default_queue_route")]
    #[schemars(default = "default_queue_route")]
    pub queue: String,
    #[serde(default = "default_pubsub_route")]
    #[schemars(default = "default_pubsub_route")]
    pub pubsub: String,
}

impl Default for HttpRoutesConfig {
    fn default() -> Self {
        Self {
            storage: default_storage_route(),
            queue: default_queue_route(),
            pubsub: default_pubsub_route(),
        }
    }
}

fn default_storage_route() -> String {
    "/storage".to_string()
}

fn default_queue_route() -> String {
    "/queue".to_string()
}

fn default_pubsub_route() -> String {
    "/pubsub".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[schemars(deny_unknown_fields)]
pub struct Cors {
    #[serde(default)]
    #[schemars(default)]
    pub allow_origins: Vec<String>,
}

/// Foreground provider admission and overload protection settings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[schemars(deny_unknown_fields)]
pub struct StorageAdmissionConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_storage_admission_initial_sustainable_throughput_rps")]
    #[schemars(default = "default_storage_admission_initial_sustainable_throughput_rps")]
    pub initial_sustainable_throughput_rps: u64,
    #[serde(default = "default_storage_admission_initial_latency_estimate_ms")]
    #[schemars(default = "default_storage_admission_initial_latency_estimate_ms")]
    pub initial_latency_estimate_ms: u64,
    #[serde(default = "default_storage_admission_minimum_concurrency")]
    #[schemars(default = "default_storage_admission_minimum_concurrency")]
    pub minimum_concurrency: usize,
    #[serde(default = "default_storage_admission_maximum_concurrency")]
    #[schemars(default = "default_storage_admission_maximum_concurrency")]
    pub maximum_concurrency: usize,
    #[serde(default = "default_storage_admission_control_reserve_concurrency")]
    #[schemars(default = "default_storage_admission_control_reserve_concurrency")]
    pub control_reserve_concurrency: usize,
    #[serde(default = "default_storage_admission_queue_capacity")]
    #[schemars(default = "default_storage_admission_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_storage_admission_max_queue_wait_ms")]
    #[schemars(default = "default_storage_admission_max_queue_wait_ms")]
    pub max_queue_wait_ms: u64,
}

impl Default for StorageAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_sustainable_throughput_rps:
                default_storage_admission_initial_sustainable_throughput_rps(),
            initial_latency_estimate_ms: default_storage_admission_initial_latency_estimate_ms(),
            minimum_concurrency: default_storage_admission_minimum_concurrency(),
            maximum_concurrency: default_storage_admission_maximum_concurrency(),
            control_reserve_concurrency: default_storage_admission_control_reserve_concurrency(),
            queue_capacity: default_storage_admission_queue_capacity(),
            max_queue_wait_ms: default_storage_admission_max_queue_wait_ms(),
        }
    }
}

const fn default_storage_admission_initial_sustainable_throughput_rps() -> u64 {
    20_000
}

const fn default_storage_admission_initial_latency_estimate_ms() -> u64 {
    5
}

const fn default_storage_admission_minimum_concurrency() -> usize {
    4
}

const fn default_storage_admission_maximum_concurrency() -> usize {
    1_024
}

const fn default_storage_admission_control_reserve_concurrency() -> usize {
    4
}

const fn default_storage_admission_queue_capacity() -> usize {
    256
}

const fn default_storage_admission_max_queue_wait_ms() -> u64 {
    25
}

/// Deadline budgets of native attempts, one per deadline class.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[schemars(deny_unknown_fields)]
pub struct StorageDeadlinesConfig {
    #[serde(default = "default_storage_deadline_point_read_ms")]
    #[schemars(default = "default_storage_deadline_point_read_ms")]
    pub point_read_ms: u64,
    #[serde(default = "default_storage_deadline_range_read_ms")]
    #[schemars(default = "default_storage_deadline_range_read_ms")]
    pub range_read_ms: u64,
    #[serde(default = "default_storage_deadline_write_ms")]
    #[schemars(default = "default_storage_deadline_write_ms")]
    pub write_ms: u64,
    #[serde(default = "default_storage_deadline_transaction_ms")]
    #[schemars(default = "default_storage_deadline_transaction_ms")]
    pub transaction_ms: u64,
    #[serde(default = "default_storage_deadline_schema_change_ms")]
    #[schemars(default = "default_storage_deadline_schema_change_ms")]
    pub schema_change_ms: u64,
    #[serde(default = "default_storage_deadline_background_page_ms")]
    #[schemars(default = "default_storage_deadline_background_page_ms")]
    pub background_page_ms: u64,
    #[serde(default = "default_storage_deadline_long_poll_probe_ms")]
    #[schemars(default = "default_storage_deadline_long_poll_probe_ms")]
    pub long_poll_probe_ms: u64,
    #[serde(default = "default_storage_deadline_commit_grace_ms")]
    #[schemars(default = "default_storage_deadline_commit_grace_ms")]
    pub commit_grace_ms: u64,
}

impl Default for StorageDeadlinesConfig {
    fn default() -> Self {
        Self {
            point_read_ms: default_storage_deadline_point_read_ms(),
            range_read_ms: default_storage_deadline_range_read_ms(),
            write_ms: default_storage_deadline_write_ms(),
            transaction_ms: default_storage_deadline_transaction_ms(),
            schema_change_ms: default_storage_deadline_schema_change_ms(),
            background_page_ms: default_storage_deadline_background_page_ms(),
            long_poll_probe_ms: default_storage_deadline_long_poll_probe_ms(),
            commit_grace_ms: default_storage_deadline_commit_grace_ms(),
        }
    }
}

const fn default_storage_deadline_point_read_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[0]
}

const fn default_storage_deadline_range_read_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[1]
}

const fn default_storage_deadline_write_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[2]
}

const fn default_storage_deadline_transaction_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[3]
}

const fn default_storage_deadline_schema_change_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[4]
}

const fn default_storage_deadline_background_page_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[5]
}

const fn default_storage_deadline_long_poll_probe_ms() -> u64 {
    storage_types::DEFAULT_ATTEMPT_BUDGETS_MS[6]
}

const fn default_storage_deadline_commit_grace_ms() -> u64 {
    storage_types::DEFAULT_COMMIT_GRACE_MS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[schemars(deny_unknown_fields)]
pub struct Features {
    #[serde(default = "default_backends")]
    #[schemars(default = "default_backends")]
    pub backends: Backends,
    #[serde(default)]
    #[schemars(default)]
    pub storage_connections: Option<StorageConnectionsConfig>,
    /// Startup-only regional configuration files, keyed by native connection
    /// ID.
    #[serde(default)]
    #[schemars(default)]
    pub regional_connections: std::collections::HashMap<String, std::path::PathBuf>,
    #[serde(default)]
    #[schemars(default)]
    pub tracing: Tracing,
    #[serde(default)]
    #[schemars(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    #[schemars(default)]
    pub runtime: RuntimeFeatures,
    #[serde(default)]
    #[schemars(default)]
    pub storage_admission: StorageAdmissionConfig,
    #[serde(default)]
    #[schemars(default)]
    pub storage_deadlines: StorageDeadlinesConfig,
}

fn default_backends() -> Backends {
    Backends::sqlite_default()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Tracing {
    #[serde(default = "default_tracing_log_level")]
    #[schemars(default = "default_tracing_log_level")]
    pub log_level: Option<String>,
}

impl Default for Tracing {
    fn default() -> Self {
        Self {
            log_level: default_tracing_log_level(),
        }
    }
}

fn default_tracing_log_level() -> Option<String> {
    Some(DEFAULT_TRACING_LOG_LEVEL.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enabled: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RuntimeFeatures {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enable_background_workers: bool,
    /// Seconds the HTTP server drains after a shutdown signal.
    #[serde(default = "default_runtime_shutdown_grace_seconds")]
    #[schemars(default = "default_runtime_shutdown_grace_seconds")]
    pub shutdown_grace_seconds: u64,
    /// Tokio worker threads; absent uses the available parallelism.
    #[serde(default)]
    #[schemars(default)]
    pub worker_threads: Option<std::num::NonZeroUsize>,
    /// Starts the tokio console layer; needs the `tokio-console` build feature.
    #[serde(default)]
    #[schemars(default)]
    pub tokio_console: bool,
}

impl Default for RuntimeFeatures {
    fn default() -> Self {
        Self {
            enable_background_workers: true,
            shutdown_grace_seconds: default_runtime_shutdown_grace_seconds(),
            worker_threads: None,
            tokio_console: false,
        }
    }
}

fn default_runtime_shutdown_grace_seconds() -> u64 {
    DEFAULT_RUNTIME_SHUTDOWN_GRACE_SECONDS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Jobs {
    #[serde(default = "default_jobs_gsi_backfill_interval_ms")]
    #[schemars(default = "default_jobs_gsi_backfill_interval_ms")]
    pub gsi_backfill_interval_ms: u64,
}

impl Default for Jobs {
    fn default() -> Self {
        Self {
            gsi_backfill_interval_ms: default_jobs_gsi_backfill_interval_ms(),
        }
    }
}

fn default_jobs_gsi_backfill_interval_ms() -> u64 {
    DEFAULT_JOBS_GSI_BACKFILL_INTERVAL_MS
}
