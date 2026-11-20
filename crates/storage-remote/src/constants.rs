use std::time::Duration;

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
pub const MAX_SCHEDULED_JOB_REDIRECT_HOPS: usize = 4;
/// Longest default connect timeout: a connect that fails ends well before the
/// request deadline, so it is a known-not-sent failure that fails over.
pub const MAX_DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Idle pooled connections close before common peer idle closes (hyper 30 s
/// header read, load balancers 60 s), so a write rarely meets a connection
/// its peer already closed, which answers an unknown outcome.
pub const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

pub const AWS_SERVICE_NAME: &str = "dynamodb";
pub const REMOTE_STORAGE_REQUEST_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::RemoteStorageRequestBytesTotalMetric;
pub const REMOTE_STORAGE_RESPONSE_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::RemoteStorageResponseBytesTotalMetric;

pub const MANAGED_TABLE_TTL_ATTRIBUTE: &str = "ttl";
pub const TABLE_ACTIVE_RETRY_ATTEMPTS: usize = 60;
pub const TABLE_ACTIVE_RETRY_DELAY_MS: u64 = 2_000;
pub const PITR_RETRY_ATTEMPTS: usize = 6;
pub const PITR_RETRY_DELAY_SECS: u64 = 5;

pub(crate) const NO_ENDPOINT: usize = usize::MAX;
