use std::time::Duration;

/// Default SQS visibility timeout when a receive request omits
/// `VisibilityTimeout`.
pub(crate) const DEFAULT_VISIBILITY_TIMEOUT_SECS: u32 = 30;

/// SQS long-poll wait time is capped at 20 seconds.
pub(crate) const MAX_RECEIVE_WAIT_TIME_SECS: u32 = 20;

/// Sleep between local empty receive retries while the long-poll budget
/// remains.
pub(crate) const EMPTY_RECEIVE_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Short backoff after a storage transaction conflict during receive.
pub(crate) const RECEIVE_CONFLICT_RETRY_DELAY: Duration = Duration::from_millis(25);

/// Number of storage transaction conflict retries for one receive call.
pub(crate) const RECEIVE_CONFLICT_RETRY_ATTEMPTS: usize = 3;

pub(crate) const DEFAULT_DELAY_SECONDS: u32 = 0;
pub(crate) const DEFAULT_MAXIMUM_MESSAGE_SIZE: u32 = 1048576;
pub(crate) const DEFAULT_MESSAGE_RETENTION_PERIOD: u32 = 345600;
pub(crate) const DEFAULT_RECEIVE_WAIT_TIME_SECONDS: u32 = 0;

pub(crate) const DEFAULT_JOB_VISIBILITY_TIMEOUT_SECS: u32 = 30;
pub(crate) const DEFAULT_MAXIMUM_JOB_WORKERS: u32 = 4;
pub(crate) const QUEUE_ATTRIBUTE_SENT_TIMESTAMP: &str = "SentTimestamp";
pub(crate) const MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS: u32 = 43_200;

pub(crate) const METRIC_QUEUE_EMPTY_RECEIVES_TOTAL: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::MetricQueueEmptyReceivesTotal;
pub(crate) const METRIC_QUEUE_MESSAGE_DELAY_MS: metrics_facade::HistogramMetric =
    metrics_facade::HistogramMetric::MetricQueueMessageDelayMs;

pub(crate) const SENDER_FAULT_RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(30);
pub(crate) const TRANSIENT_RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(1);

pub(crate) const DEFAULT_MAXIMUM_JOB_ATTEMPTS: u32 = 180;
pub(crate) const QUEUE_ATTRIBUTE_RECEIVE_COUNT: &str = "ApproximateReceiveCount";
pub(crate) const MAX_IMMEDIATE_JOB_BACKOFF_SECS: u64 = 300;

pub(crate) const MAX_EMPTY_JOB_POLL_DELAY_MS: u64 = 30_000;
