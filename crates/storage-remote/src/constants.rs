use metrics_facade::CounterMetric;

pub const MAX_REMOTE_RETRIES: usize = 5;
pub const MAX_ENDPOINT_RETRIES: usize = 3;
pub const BASE_BACKOFF_MS: u64 = 50;
pub const FAILURE_ALERT_THRESHOLD: usize = 10;

// Upper bound used when computing jitter for retry backoff.
pub const MAX_JITTER_MS: u64 = 50;

pub const AWS_SERVICE_NAME: &str = "dynamodb";
pub const REMOTE_STORAGE_REQUEST_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::RemoteStorageRequestBytesTotalMetric;
pub const REMOTE_STORAGE_RESPONSE_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::RemoteStorageResponseBytesTotalMetric;
pub const STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC: CounterMetric =
    CounterMetric::StorageBilledItemOpsTotalMetric;
pub const STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::StorageLogicalItemBytesTotalMetric;

pub const MANAGED_TABLE_TTL_ATTRIBUTE: &str = "ttl";
pub const TABLE_ACTIVE_RETRY_ATTEMPTS: usize = 60;
pub const TABLE_ACTIVE_RETRY_DELAY_MS: u64 = 2_000;
pub const PITR_RETRY_ATTEMPTS: usize = 6;
pub const PITR_RETRY_DELAY_SECS: u64 = 5;
