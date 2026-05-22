//! Storage API constants.
pub const BASE_PATH: &str = "/storage";
pub const STORAGE_GATEWAY_API_KEY_HEADER: &str = "x-api-key";
pub const STORAGE_GATEWAY_SERVICE_NAME: &str = "storage-gateway";
pub const STORAGE_REPLICATION_SERVICE_NAME: &str = "storage-replication";
pub const STORAGE_REPLICATION_SELF_REGION_ENV: &str = "AUX_MULTI_REGION_SELF_REGION";
pub const STORAGE_REPLICATION_HEARTBEAT_MISS_THRESHOLD_MS: u64 = 30_000;
pub const STORAGE_REPLICATION_LAG_WARNING_THRESHOLD_MS: u64 = 60_000;
pub const STORAGE_REPLICATION_LAG_CRITICAL_THRESHOLD_MS: u64 = 300_000;
pub const STORAGE_API_DYNAMODB_REQUESTS_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageApiDynamodbRequestsTotalMetric;
pub const STORAGE_API_DYNAMODB_REQUEST_LATENCY_MICROS_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageApiDynamodbRequestLatencyMicrosTotalMetric;
pub const STORAGE_API_DYNAMODB_REQUEST_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageApiDynamodbRequestLatencyMsMetric;
pub const STORAGE_API_DYNAMODB_STAGE_TOTAL_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::StorageApiDynamodbStageTotalMetric;
pub const STORAGE_API_DYNAMODB_STAGE_LATENCY_MS_METRIC: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::StorageApiDynamodbStageLatencyMsMetric;
