//! Storage API constants.
use std::time::Duration;

pub const BASE_PATH: &str = "/storage";
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

pub(crate) const DYNAMODB_JSON_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;
#[cfg(any(
    test,
    feature = "queue",
    feature = "pubsub",
    feature = "internal-test-routes"
))]
pub(crate) const AUXILIARY_JSON_BODY_LIMIT_BYTES: usize = 5 * 1024 * 1024;
#[cfg(feature = "queue")]
pub(crate) const QUEUE_JSON_TARGET_PREFIX: &str = "AmazonSQS.";

pub(crate) const MERGED_SHARD_ID: &str = "shardId-00000000000000000000-auxfn1";
pub(crate) const STREAM_ITERATOR_VERSION: &str = "aux-stream-v1";

pub const PROMETHEUS_UPKEEP_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(feature = "pubsub")]
pub const PUBSUB_DELIVERY_IDLE_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(feature = "pubsub")]
pub const PUBSUB_DELIVERY_ERROR_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(feature = "pubsub")]
pub const PUBSUB_DELIVERY_BATCH_LIMIT: usize = 100;
pub const PROMETHEUS_LATENCY_MS_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
    1000.0, 2500.0, 5000.0, 10000.0,
];

pub(crate) const DYNAMODB_STATUS_UNKNOWN_OPERATION: &str = "unknown";
pub(crate) const DYNAMODB_OPERATIONS: &[&str] = &[
    "get_canonical_history",
    "batch_get_item",
    "batch_write_item",
    "create_table",
    "delete_item",
    "delete_table",
    "describe_table",
    "describe_time_to_live",
    "get_item",
    "get_stream_records",
    "list_tables",
    "put_item",
    "query",
    "scan",
    "transact_get_items",
    "transact_write_items",
    "update_continuous_backups",
    "update_item",
    "update_table",
    "update_time_to_live",
    DYNAMODB_STATUS_UNKNOWN_OPERATION,
];
pub(crate) const DYNAMODB_STATUSES: &[&str] = &[DYNAMODB_SUCCESS_STATUS, DYNAMODB_ERROR_STATUS];
pub(crate) const DYNAMODB_STAGES: &[&str] = &[
    DYNAMODB_BODY_READ_STAGE,
    DYNAMODB_JSON_DECODE_STAGE,
    DYNAMODB_REQUEST_CONVERT_STAGE,
    DYNAMODB_MANAGER_STAGE,
    DYNAMODB_RESPONSE_ENCODE_STAGE,
];

pub(crate) const DYNAMODB_SUCCESS_STATUS: &str = "success";
pub(crate) const DYNAMODB_ERROR_STATUS: &str = "error";
pub(crate) const DYNAMODB_JSON_DECODE_STAGE: &str = "json_decode";
pub(crate) const DYNAMODB_REQUEST_CONVERT_STAGE: &str = "request_convert";
pub(crate) const DYNAMODB_MANAGER_STAGE: &str = "manager";
pub(crate) const DYNAMODB_RESPONSE_ENCODE_STAGE: &str = "response_encode";
pub(crate) const DYNAMODB_BODY_READ_STAGE: &str = "body_read";
