pub(crate) const MAINTENANCE_DRIVER_CLAIMS: u32 = 8;
pub(crate) const MAINTENANCE_DRIVER_CONCURRENCY: u16 = 4;
pub(crate) const MAINTENANCE_DRIVER_ERROR_WAKE_MS: u64 = 1_000;
pub(crate) const MAINTENANCE_DRIVER_FINISH_WINDOW_MS: u64 = 500;
pub(crate) const MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS: u32 = 64;
pub(crate) const MAINTENANCE_DRIVER_STOP_WINDOW_MS: u64 = 250;
pub(crate) const MAINTENANCE_JOB_SCHEMA_VERSION: u16 = 1;
pub(crate) const QUEUE_MESSAGE_CLEANUP_PAGE_LIMIT: u32 = 100;
pub(crate) const MAINTENANCE_NAMESPACE_PREFIX: &str = "aux-storage:scheduled-jobs";

/// Storage operation metrics (`DatabaseManager` wrapper).
pub const STORAGE_OPERATION_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageOperationTotalMetric;
pub const STORAGE_OPERATION_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageOperationLatencyMsMetric;
/// Base expression-attribute name placeholder used when injecting `updated_at`.
pub(crate) const UPDATED_AT_NAME_PLACEHOLDER_BASE: &str = "#__updated_at";

/// Base expression-attribute value placeholder used when injecting
/// `updated_at`.
pub(crate) const UPDATED_AT_VALUE_PLACEHOLDER_BASE: &str = ":__updated_at";

/// Maximum JSON input for startup-only regional configuration files.
#[cfg(feature = "regional")]
pub(crate) const REGIONAL_STARTUP_FILE_BYTES: u64 = 1_048_576;
