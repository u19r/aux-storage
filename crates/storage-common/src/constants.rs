//! Shared storage-common constants.

// Item limits of one Query or Scan page; a request without `Limit` uses
// them, and the 1 MB evaluated-bytes budget usually ends the page first.
pub const MAX_QUERY_LIMIT: u32 = 10_000;
pub const MAX_SCAN_LIMIT: u32 = MAX_QUERY_LIMIT;

pub const MAX_TTL_SWEEP_PAGE_LIMIT: usize = 500;

pub(crate) const PARTITION_AUTOSCALE_COOLDOWN_MS: i64 = 5 * 60 * 1_000;
pub const DEFAULT_ORDERED_LOG_PARTITION_COUNT: std::num::NonZeroU16 =
    std::num::NonZeroU16::MIN.saturating_add(16 - 1);
pub const DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND: u64 = 500;
pub const DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND: u64 = 256 * 1024;
pub const DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW: u64 = 8;
pub const DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS: u64 = 1_000;
pub(crate) const PARTITION_CONTROLLER_SPLIT_THRESHOLD: f64 = 1.3;
pub(crate) const PARTITION_CONTROLLER_QUEUE_SCALE_OUT_THRESHOLD: f64 = 1.2;
pub(crate) const PARTITION_CONTROLLER_LOW_THRESHOLD: f64 = 0.35;
pub(crate) const PARTITION_CONTROLLER_HIGH_STREAK_TARGET: u32 = 3;
pub(crate) const PARTITION_CONTROLLER_LOW_STREAK_TARGET: u32 = 12;
pub(crate) const PARTITION_CONTROLLER_EWMA_ALPHA: f64 = 0.4;
pub(crate) const PARTITION_CONTROLLER_INTEGRAL_MIN: f64 = -2.0;
pub(crate) const PARTITION_CONTROLLER_INTEGRAL_MAX: f64 = 4.0;

/// Estimate append pressure without a shared counter or another native attempt.
pub const PARTITION_LOAD_SAMPLE_RATE: u64 = 64;

/// Bound one native telemetry page and publication payload.
pub const PARTITION_SAMPLE_PAGE_LIMIT: u32 = 256;
pub const PARTITION_PUBLISHER_MAX_BYTES: usize = 256;
pub const PARTITION_SAMPLE_MAX_AGE_MS: i64 = 60_000;

pub(crate) const PARTITION_SAMPLE_WINDOW_MS: i64 = 1_000;

/// Longest extra wait before a sync while other write attempts are between
/// their first write and their commit.
pub(crate) const DURABILITY_BATCH_WINDOW_MS: u64 = 2;

/// Pause before a barrier is tried again after it returned no sync result.
pub(crate) const DURABILITY_BARRIER_RETRY_MS: u64 = 10;

/// Default storage maintenance cadences in milliseconds.
pub(crate) const DEFAULT_JOB_INTERVALS: crate::jobs::DatabaseJobIntervals =
    crate::jobs::DatabaseJobIntervals {
        partition_reconciliation_interval_ms: crate::JobIntervalMillis(1_000),
        gsi_backfill_interval_ms: crate::JobIntervalMillis(30_000),
        ttl_sweep_interval_ms: crate::JobIntervalMillis(300_000),
        stream_trim_interval_ms: crate::JobIntervalMillis(3_600_000),
        table_reclaim_interval_ms: crate::JobIntervalMillis(30_000),
        queue_message_cleanup_interval_ms: crate::JobIntervalMillis(60_000),
        // Pending subscriptions are deleted 48 hours after Subscribe; an
        // hourly turn keeps the overrun under an hour.
        pubsub_stale_subscription_sweep_interval_ms: crate::JobIntervalMillis(3_600_000),
        // Matches the ten-minute transact-write idempotency window.
        transact_receipt_trim_interval_ms: crate::JobIntervalMillis(600_000),
    };
