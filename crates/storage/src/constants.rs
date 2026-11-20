pub(crate) const MAINTENANCE_DRIVER_CLAIMS: u32 = 8;
pub(crate) const MAINTENANCE_DRIVER_CONCURRENCY: u16 = 4;
pub(crate) const MAINTENANCE_DRIVER_ERROR_WAKE_MS: u64 = 1_000;
pub(crate) const MAINTENANCE_DRIVER_FINISH_WINDOW_MS: u64 = 500;
// Each claim reserves its checked lifecycle journal and bounded discovery.
pub(crate) const MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS: u32 = MAINTENANCE_DRIVER_CLAIMS * 256;
pub(crate) const MAINTENANCE_DRIVER_STOP_WINDOW_MS: u64 = 250;
pub(crate) const MAINTENANCE_JOB_SCHEMA_VERSION: u16 = 1;
pub(crate) const QUEUE_MESSAGE_CLEANUP_PAGE_LIMIT: u32 = 100;
pub(crate) const MAINTENANCE_NAMESPACE_PREFIX: &str = "aux-storage:scheduled-jobs";

/// Storage operation metrics (`DatabaseManager` wrapper).
pub const STORAGE_OPERATION_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageOperationTotalMetric;
pub const STORAGE_OPERATION_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageOperationLatencyMsMetric;
/// Age in ms of the oldest unsent history entry of a regional pair's last page:
/// the largest over the pairs whose turns ran in this process within the lag
/// window (gauge, label `connection`).
pub(crate) const REGIONAL_LAG_METRIC: &str = "storage.regional.lag_ms";
/// Longest wait between retries of a failed regional peer call or local pair
/// turn, unless `send_interval` is already longer.
pub(crate) const REGIONAL_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(10);
/// After the first failure, a failed regional peer call or local pair turn is
/// logged on every this-many-th consecutive failure.
pub(crate) const REGIONAL_WARN_EVERY: u32 = 20;
/// The lag gauge forgets a pair this many longest retry delays (plus one claim
/// lease) after its last turn in this process.
pub(crate) const REGIONAL_LAG_WINDOW_TURNS: u32 = 3;
/// Base expression-attribute name placeholder used when injecting `updated_at`.
pub(crate) const UPDATED_AT_NAME_PLACEHOLDER_BASE: &str = "#__updated_at";

/// Base expression-attribute value placeholder used when injecting
/// `updated_at`.
pub(crate) const UPDATED_AT_VALUE_PLACEHOLDER_BASE: &str = ":__updated_at";
