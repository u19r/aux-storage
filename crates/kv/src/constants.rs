pub const TTL_SWEEP_INTERVAL_MINUTES: u64 = 5;
pub const TTL_SWEEP_TABLE_CONCURRENCY: usize = 4;
pub const TTL_SWEEP_INITIAL_SHARD_BATCH: usize = 8;
pub const TTL_SWEEP_MIN_SHARD_BATCH: usize = 2;
pub const TTL_SWEEP_MAX_SHARD_BATCH: usize = 32;

pub const TTL_SWEEP_ITEMS_PER_SHARD: usize = 1_000;
pub const TTL_SWEEP_DELETE_BATCH_SIZE: usize = 25;
pub const TTL_SWEEP_DELETE_BATCH_CONCURRENCY: usize = 4;

pub const TTL_SWEEP_LOCK_TTL_MS: i64 = 30_000;
pub const TTL_SWEEP_MAX_SKIP: u32 = 10;
pub const TTL_SWEEP_RETRY_MAX_ATTEMPTS: u32 = 3;
pub const TTL_SWEEP_RETRY_BASE_DELAY_MS: u64 = 10;
pub const TTL_SWEEP_RETRY_MAX_DELAY_MS: u64 = 100;
pub const TTL_SWEEP_HEALTH_CHECK_INTERVAL_MINUTES: u64 = 1_440;

// Provider cache tuning. The table cache is intentionally larger because table
// metadata is on most DynamoDB-compatible read and write paths.
pub const TABLE_CACHE_CAPACITY: usize = 5_000;
pub const TABLE_CACHE_TTL_SECONDS: u64 = 3_600;
pub const TABLE_METADATA_HOT_CACHE_CAPACITY: usize = 8;
pub const TABLE_METADATA_HOT_CACHE_TTL_MILLIS: u64 = 5;
pub const PARTITION_FAMILY_CACHE_CAPACITY: usize = 256;
pub const PARTITION_FAMILY_CACHE_TTL_SECONDS: u64 = 5;
pub const PARTITION_FAMILY_CACHE_WATCH_TIMEOUT_SECONDS: u64 = 30;
pub const TTL_CONFIG_CACHE_CAPACITY: usize = 1_024;
pub const TTL_CONFIG_CACHE_TTL_SECONDS: u64 = 60;

// Stream trim tuning.
pub const STREAM_TRIM_INTERVAL_MINUTES: u64 = 60;
pub const STREAM_TRIM_RETENTION_HOURS: i64 = 72;
pub const MILLIS_PER_HOUR: i64 = 60 * 60 * 1000;
pub const STREAM_TRIM_READ_LIMIT: u32 = 1_000;
pub const STREAM_TRIM_DELETE_BATCH_SIZE: usize = 25;
pub const STREAM_TRIM_DELETE_BATCH_CONCURRENCY: usize = 4;
pub const STREAM_TRIM_BATCH_DELAY_MS: u64 = 10;

// GSI update job tuning.
pub const GSI_UPDATE_STREAM_FETCH_LIMIT: u32 = 1_000;
pub const GSI_UPDATE_LOG_INTERVAL_MS: u64 = 30_000;
pub const GSI_UPDATE_SLOW_LOG_MS: u64 = 100;

// Partition-family reconcile tuning.
pub const DEFAULT_ORDERED_LOG_PARTITION_COUNT: u16 = 16;
pub const DEFAULT_STANDARD_QUEUE_PARTITION_COUNT: u16 = 64;
pub const DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND: u64 = 500;
pub const DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND: u64 = 256 * 1024;
pub const DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW: u64 = 8;
pub const DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS: u64 = 1_000;
pub const PARTITION_RECONCILE_INTERVAL_SECONDS: u64 = 30;
pub const PARTITION_LOAD_SAMPLE_WINDOW_SECONDS: i64 = 10;
pub const PARTITION_LOAD_SAMPLE_RETENTION_WINDOWS: i64 = 12;
pub const PARTITION_AUTOSCALE_COOLDOWN_MS: i64 = 5 * 60 * 1_000;
pub const PARTITION_CONTROLLER_EWMA_ALPHA: f64 = 0.4;
pub const PARTITION_CONTROLLER_INTEGRAL_MIN: f64 = -2.0;
pub const PARTITION_CONTROLLER_INTEGRAL_MAX: f64 = 4.0;
pub const PARTITION_CONTROLLER_SPLIT_THRESHOLD: f64 = 1.3;
pub const PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD: f64 = 1.2;
pub const PARTITION_CONTROLLER_LOW_THRESHOLD: f64 = 0.35;
pub const PARTITION_CONTROLLER_HIGH_STREAK_TARGET: u32 = 3;
pub const PARTITION_CONTROLLER_LOW_STREAK_TARGET: u32 = 12;
pub const PARTITION_RECONCILE_RUNS_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::PartitionReconcileRunsTotalMetric;
pub const PARTITION_RECONCILE_ACTIONS_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::PartitionReconcileActionsTotalMetric;
pub const PARTITION_LOAD_SAMPLES_FLUSHED_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::PartitionLoadSamplesFlushedTotalMetric;
pub const PARTITION_ROUTING_RETRIES_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::PartitionRoutingRetriesTotalMetric;
pub const PARTITION_FAMILY_HOT_FAMILIES_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::PartitionFamilyHotFamiliesMetric;
pub const PARTITION_FAMILY_MANAGED_FAMILIES_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::PartitionFamilyManagedFamiliesMetric;
pub const PARTITION_FAMILY_OPEN_PARTITIONS_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::PartitionFamilyOpenPartitionsMetric;
pub const PARTITION_FAMILY_PRESSURE_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::PartitionFamilyPressureMetric;
pub const PARTITION_FAMILY_TRANSITION_PARTITIONS_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::PartitionFamilyTransitionPartitionsMetric;
pub const PARTITION_RECONCILE_RUNTIME_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::PartitionReconcileRuntimeMsMetric;
pub const FOUNDATIONDB_GET_READ_VERSION_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::FoundationdbGetReadVersionLatencyMs;
pub const FOUNDATIONDB_OPERATION_BYTES_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::FoundationdbOperationBytesTotal;
pub const FOUNDATIONDB_OPERATIONS_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::FoundationdbOperationsTotal;
pub const STORAGE_PROVIDER_STAGE_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageProviderStageTotalMetric;
pub const STORAGE_PROVIDER_STAGE_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageProviderStageLatencyMsMetric;

// Stream TTL cleanup metrics.
pub const STREAM_TTL_CLEANUP_RUNTIME_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StreamTtlCleanupRuntimeMsMetric;
pub const STREAM_TTL_CLEANUP_ITEMS_DELETED_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StreamTtlCleanupItemsDeletedTotalMetric;
pub const STREAM_TTL_CLEANUP_STREAMS_SCANNED_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StreamTtlCleanupStreamsScannedTotalMetric;
pub const STREAM_TTL_CLEANUP_RUNS_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StreamTtlCleanupRunsTotalMetric;
