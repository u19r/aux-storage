//! Shared storage-common constants.

pub const DEFAULT_GENERIC_LIMIT: u32 = 100;
pub const MAX_GENERIC_LIMIT: u32 = 10_000;

// Scan/query pagination shared by sql backends (formerly sql-only).
// Query max matches DynamoDB parity (10k); scan max stays conservative
// (1k) until full 1MB fetch support lands.
pub const DEFAULT_SCAN_LIMIT: u32 = 100;
pub const MAX_SCAN_LIMIT: u32 = 1_000;
pub const DEFAULT_QUERY_LIMIT: u32 = 100;
pub const MAX_QUERY_LIMIT: u32 = 10_000;

pub const TTL_INDEX_PREFIX: &str = "__ttl-index/";
pub const TTL_INDEX_WIDTH: usize = 20;
pub const MAX_TTL_SWEEP_PAGE_LIMIT: usize = 500;

// Stream trim tuning shared by kv and sql backends (formerly duplicated in
// kv::constants and sql::constants). Retention horizon and paging shape the
// trim worker on every backend.
pub const STREAM_TRIM_RETENTION_HOURS: i64 = 72;
pub const MILLIS_PER_HOUR: i64 = 60 * 60 * 1000;
pub const STREAM_TRIM_READ_LIMIT: u32 = 1_000;
pub const STREAM_TRIM_DELETE_BATCH_SIZE: usize = 25;

pub const GSI_LAG_TARGET_MS: u64 = 200;
pub const GSI_LAG_SOFT_LIMIT_MS: u64 = 400;
pub const GSI_LAG_HARD_LIMIT_MS: u64 = 1_000;
pub const GSI_LAG_CRITICAL_LIMIT_MS: u64 = 3_000;
pub(crate) const MAX_SOFT_DELAY_MS: u64 = 25;
pub(crate) const HARD_DELAY_MS: u64 = 50;

pub(crate) const PARTITION_AUTOSCALE_COOLDOWN_MS: i64 = 5 * 60 * 1_000;
pub const DEFAULT_ORDERED_LOG_PARTITION_COUNT: std::num::NonZeroU16 =
    std::num::NonZeroU16::MIN.saturating_add(16 - 1);
pub const DEFAULT_STANDARD_QUEUE_PARTITION_COUNT: u16 = 64;
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

pub(crate) const DURABILITY_BATCH_WINDOW_MS: u64 = 2;
pub(crate) const DURABILITY_BATCH_CAPACITY: usize = 1024;

/// Default storage maintenance cadences in milliseconds.
pub(crate) const DEFAULT_JOB_INTERVALS: crate::jobs::DatabaseJobIntervals =
    crate::jobs::DatabaseJobIntervals {
        partition_reconciliation_interval_ms: crate::JobIntervalMillis(1_000),
        gsi_backfill_interval_ms: crate::JobIntervalMillis(30_000),
        ttl_sweep_interval_ms: crate::JobIntervalMillis(300_000),
        stream_trim_interval_ms: crate::JobIntervalMillis(3_600_000),
        table_reclaim_interval_ms: crate::JobIntervalMillis(30_000),
        queue_message_cleanup_interval_ms: crate::JobIntervalMillis(60_000),
    };
