/// Trait for entities that participate in LRU/TTL caching.
///
/// Implementations define cache parameters as associated functions so
/// that cache construction is driven by the entity type rather than
/// hard-coded values scattered across manager/service code.
pub trait Cacheable {
    /// Maximum time in seconds an item is valid in the cache.
    fn cache_time_seconds() -> u64;

    /// How often (in seconds) the cache should proactively refetch.
    /// Must be ≤ `cache_time_seconds`.
    fn refetch_time_seconds() -> u64;

    /// Maximum number of items in the LRU cache.
    fn max_cache_items() -> usize;
}
