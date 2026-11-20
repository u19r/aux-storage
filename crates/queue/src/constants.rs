use std::time::Duration;

/// SQS long-poll wait time is capped at 20 seconds.
pub(crate) const MAX_RECEIVE_WAIT_TIME_SECS: u32 = 20;

/// Age at which waiting receives refresh their shared read-only probe of one
/// queue. It bounds the delay for messages committed by another process.
pub(crate) const RECEIVE_PROBE_INTERVAL: Duration = Duration::from_millis(200);

/// Shortest time after a queue's previous probe at which a change of its wake
/// slot opens the next probe (hot-slot bound).
pub(crate) const RECEIVE_WAKE_SPACING: Duration = Duration::from_millis(200);

/// Base pause after a claim returns empty but not drained (it lost a conflict
/// or skipped a locked row while due messages remain). Jitter adds up to the
/// same amount.
pub(crate) const EMPTY_CLAIM_BACKOFF: Duration = Duration::from_millis(10);

/// Default bound on receives that wait at the same time in one process. A
/// receive over the bound returns its empty result at once.
pub(crate) const DEFAULT_MAX_WAITING_RECEIVES: usize = 10_000;

/// Queue wait states without waiters are swept when the registry reaches this
/// size, and then at twice the retained size.
pub(crate) const RECEIVE_WAIT_SWEEP_FLOOR: usize = 256;

/// A queue wait state without waiters is retained this long after its latest
/// observation so that busy queues keep their observation between receives.
pub(crate) const RECEIVE_WAIT_RETENTION: Duration = Duration::from_secs(60);

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
