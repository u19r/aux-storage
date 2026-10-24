//! Shared table-metadata cache tuning.
//!
//! SQL and KV reuse decoded table metadata after checking the durable catalog
//! incarnation and revision in the operation attempt. Local mutations also
//! invalidate the cache after commit. The short expiry bounds retention of
//! decoded metadata; TTL policy tolerates bounded cross-node staleness.

use std::time::Duration;

use lru_ttl_cache::CacheConfig;

/// Table-info cache capacity: bounds entries per provider.
pub const TABLE_INFO_CACHE_CAPACITY: usize = 5_000;

/// Metadata expiry backstop and bounded TTL-policy staleness in seconds.
pub const TABLE_INFO_CACHE_TTL_SECONDS: u64 = 5;

/// Shared table-info cache configuration for every backend's table
/// metadata cache. Generic over the cached value because KV caches
/// identity bundles while SQL backends cache info snapshots.
pub fn table_info_cache_config<K, V>() -> CacheConfig<K, V> {
    CacheConfig::new()
        .with_capacity(TABLE_INFO_CACHE_CAPACITY)
        .with_ttl(Duration::from_secs(TABLE_INFO_CACHE_TTL_SECONDS))
}
