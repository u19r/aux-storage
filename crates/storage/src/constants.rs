/// Storage operation metrics (`DatabaseManager` wrapper).
pub const STORAGE_OPERATION_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageOperationTotalMetric;
pub const STORAGE_OPERATION_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageOperationLatencyMsMetric;
pub const STORAGE_DDB_GET_ITEM_CACHE_HIT_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbGetItemCacheHitMetric;
pub const STORAGE_DDB_GET_ITEM_CACHE_MISS_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbGetItemCacheMissMetric;
pub const STORAGE_DDB_AUTHORITATIVE_PREIMAGE_HIT_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbAuthoritativePreimageHitMetric;
pub const STORAGE_DDB_AUTHORITATIVE_PREIMAGE_MISS_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbAuthoritativePreimageMissMetric;
pub const STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbGuardConflictFallbackMetric;
pub const STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbGuardUnsupportedFallbackMetric;
pub const STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbBatchGetItemCacheHitMetric;
pub const STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_PARTIAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbBatchGetItemCacheHitPartialMetric;
pub const STORAGE_DDB_BATCH_GET_ITEM_CACHE_MISS_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbBatchGetItemCacheMissMetric;
pub const STORAGE_DDB_QUERY_CACHE_HIT_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbQueryCacheHitMetric;
pub const STORAGE_DDB_QUERY_CACHE_HIT_PARTIAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbQueryCacheHitPartialMetric;
pub const STORAGE_DDB_QUERY_CACHE_MISS_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageDdbQueryCacheMissMetric;
pub const STORAGE_DDB_CACHE_HIT_RATIO_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::StorageDdbCacheHitRatioMetric;
pub const STORAGE_MULTI_REGION_CONFLICT_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageMultiRegionConflictTotalMetric;
pub const STORAGE_MULTI_REGION_REPLICATION_LAG_MS_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::StorageMultiRegionReplicationLagMsMetric;
pub const STORAGE_MULTI_REGION_HEARTBEAT_RTT_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageMultiRegionHeartbeatRttMsMetric;
pub const STORAGE_MULTI_REGION_HEARTBEAT_STALENESS_MS_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::StorageMultiRegionHeartbeatStalenessMsMetric;
pub const STORAGE_MULTI_REGION_SENDER_QUEUE_DEPTH_METRIC: metrics_facade::GaugeMetric =
    metrics_facade::GaugeMetric::StorageMultiRegionSenderQueueDepthMetric;
pub const STORAGE_MULTI_REGION_APPLY_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageMultiRegionApplyTotalMetric;
pub const STORAGE_MULTI_REGION_AUTH_FAILURE_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageMultiRegionAuthFailureTotalMetric;
pub const TABLE_ACTIVE_RETRY_ATTEMPTS: usize = 60;
pub const TABLE_ACTIVE_RETRY_DELAY_MS: u64 = 2_000;

/// Base expression-attribute name placeholder used when injecting `updated_at`.
pub(crate) const UPDATED_AT_NAME_PLACEHOLDER_BASE: &str = "#__updated_at";

/// Base expression-attribute value placeholder used when injecting
/// `updated_at`.
pub(crate) const UPDATED_AT_VALUE_PLACEHOLDER_BASE: &str = ":__updated_at";

/// Startup reachability timeout for a local FoundationDB provider.
#[cfg(feature = "foundationdb")]
pub const FOUNDATIONDB_STARTUP_REACHABILITY_TIMEOUT_SECS: u64 = 5;
