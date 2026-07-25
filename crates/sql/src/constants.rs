//! Storage layer constants for `SQLite` backend.
//!
//! Centralizes tunables, limits, and `DynamoDB` parity constraints used across
//! the storage implementation.

/// Default page size for scan operations when no limit is specified.
pub const DEFAULT_SCAN_LIMIT: u32 = 100;

/// Maximum page size for scan operations to prevent resource exhaustion.
pub const MAX_SCAN_LIMIT: u32 = 1_000;

/// Default page size for query operations when no limit is specified.
pub const DEFAULT_QUERY_LIMIT: u32 = 100;

/// Maximum page size for query operations to prevent resource exhaustion.
pub const MAX_QUERY_LIMIT: u32 = 10_000;

/// Maximum number of global secondary indexes per table (`DynamoDB` parity).
pub const MAX_GSI_COUNT: usize = 20;

/// Maximum retry attempts for Turso storage conflicts.
#[cfg(feature = "turso-backend")]
pub const MAX_CONFLICT_ATTEMPTS: u32 = 13;

/// Base backoff duration in milliseconds for exponential backoff on retries.
#[cfg(feature = "turso-backend")]
pub const BASE_BACKOFF_MS: u64 = 2;

// TTL sweep tuning (parity with KV backend).
pub const TTL_SWEEP_INTERVAL_MINUTES: u64 = 5;
pub const TTL_SWEEP_TABLE_CONCURRENCY: usize = 4;
pub const TTL_SWEEP_INITIAL_SHARD_BATCH: usize = 8;
pub const TTL_SWEEP_MIN_SHARD_BATCH: usize = 2;
pub const TTL_SWEEP_MAX_SHARD_BATCH: usize = 32;
pub const TTL_SWEEP_ITEMS_PER_SHARD: usize = 100;
pub const TTL_SWEEP_DELETE_BATCH_SIZE: usize = 25;
pub const TTL_SWEEP_DELETE_BATCH_CONCURRENCY: usize = 4;
pub const TTL_SWEEP_LOCK_TTL_MS: i64 = 30_000;
pub const TTL_SWEEP_MAX_SKIP: u32 = 10;
pub const TTL_SWEEP_RETRY_MAX_ATTEMPTS: u32 = 3;
pub const TTL_SWEEP_RETRY_BASE_DELAY_MS: u64 = 10;
pub const TTL_SWEEP_RETRY_MAX_DELAY_MS: u64 = 100;
pub const TTL_SWEEP_HEALTH_CHECK_INTERVAL_MINUTES: u64 = 1_440;

// Stream trim tuning (parity with KV backend).
pub const STREAM_TRIM_RETENTION_HOURS: i64 = 72;
pub const MILLIS_PER_HOUR: i64 = 60 * 60 * 1000;
pub const STREAM_TRIM_READ_LIMIT: u32 = 1_000;
pub const STREAM_TRIM_DELETE_BATCH_SIZE: usize = 25;
pub const STREAM_TRIM_DELETE_BATCH_CONCURRENCY: usize = 4;
pub const STREAM_TRIM_BATCH_DELAY_MS: u64 = 10;

// GSI update job tuning.
pub const GSI_UPDATE_STREAM_FETCH_LIMIT: u32 = 1_000;
pub const GSI_UPDATE_LOG_INTERVAL_MS: u64 = 30_000;
pub const GSI_UPDATE_SLOW_LOG_MS: u64 = 100;
