use metrics_facade::CounterMetric;

pub const MAX_REMOTE_RETRIES: usize = 5;
pub const MAX_ENDPOINT_RETRIES: usize = 3;
pub const BASE_BACKOFF_MS: u64 = 50;
pub const MAX_BACKOFF_MS: u64 = 5_000;
pub const FAILURE_ALERT_THRESHOLD: usize = 10;

// Retry admission is deliberately small and bounded. A request consumes one
// token for every retry (including a failover), then tokens are replenished
// lazily over time.
pub const RETRY_TOKEN_CAPACITY: u64 = 100;
pub const RETRY_TOKEN_REFILL_PER_SECOND: u64 = 10;
pub const MAX_RETRY_AFTER_SECS: u64 = 60;
pub const MIN_RETRY_ATTEMPT_BUDGET_MS: u64 = 1;

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
