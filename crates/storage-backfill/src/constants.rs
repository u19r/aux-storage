//! Default configuration values for storage backfill coordination.

/// Maximum number of concurrent GSI backfill jobs that may execute in a single
/// worker interval. The coordinator will cap acquired locks at this number
/// before yielding control back to the scheduler.
pub const MAX_CONCURRENT_GSI_BACKFILLS: usize = 4;

/// Default number of items to scan and project into an index per batch.
/// Backfill drivers may clamp this further based on backend limits.
pub const BACKFILL_BATCH_SIZE: usize = 500;

/// Delay in milliseconds inserted between batches when no work was performed.
/// This provides a small back-off window to avoid hot-looping when a table has
/// very few updates.
pub const BACKFILL_BATCH_SLEEP_MS: u64 = 50;

/// Milliseconds a lock remains valid before another worker may preempt it.
/// Locks are renewed on each successful batch.
pub const BACKFILL_LOCK_TTL_MS: i64 = 30_000;
