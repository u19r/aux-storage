use std::sync::{
    LazyLock,
    atomic::{AtomicU64, Ordering},
};

use metrics::{Counter, Gauge};

use crate::constants::{
    STORAGE_DDB_AUTHORITATIVE_PREIMAGE_HIT_METRIC, STORAGE_DDB_AUTHORITATIVE_PREIMAGE_MISS_METRIC,
    STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_METRIC,
    STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_PARTIAL_METRIC,
    STORAGE_DDB_BATCH_GET_ITEM_CACHE_MISS_METRIC, STORAGE_DDB_CACHE_HIT_RATIO_METRIC,
    STORAGE_DDB_GET_ITEM_CACHE_HIT_METRIC, STORAGE_DDB_GET_ITEM_CACHE_MISS_METRIC,
    STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC, STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC,
    STORAGE_DDB_QUERY_CACHE_HIT_METRIC, STORAGE_DDB_QUERY_CACHE_HIT_PARTIAL_METRIC,
    STORAGE_DDB_QUERY_CACHE_MISS_METRIC,
};

static GET_ITEM_CACHE_HIT_COUNT: AtomicU64 = AtomicU64::new(0);
static GET_ITEM_CACHE_MISS_COUNT: AtomicU64 = AtomicU64::new(0);
static BATCH_GET_ITEM_CACHE_HIT_COUNT: AtomicU64 = AtomicU64::new(0);
static BATCH_GET_ITEM_CACHE_HIT_PARTIAL_COUNT: AtomicU64 = AtomicU64::new(0);
static BATCH_GET_ITEM_CACHE_MISS_COUNT: AtomicU64 = AtomicU64::new(0);
static QUERY_CACHE_HIT_COUNT: AtomicU64 = AtomicU64::new(0);
static QUERY_CACHE_HIT_PARTIAL_COUNT: AtomicU64 = AtomicU64::new(0);
static QUERY_CACHE_MISS_COUNT: AtomicU64 = AtomicU64::new(0);

struct CacheReadOutcomeMetricHandles {
    get_item_hit: Counter,
    get_item_miss: Counter,
    batch_get_item_hit: Counter,
    batch_get_item_hit_partial: Counter,
    batch_get_item_miss: Counter,
    query_hit: Counter,
    query_hit_partial: Counter,
    query_miss: Counter,
}

struct CacheHitRatioMetricHandles {
    get_item: Gauge,
    batch_get_item: Gauge,
    query: Gauge,
}

struct GuardFallbackMetricHandles {
    guard_conflict_put_item: Counter,
    guard_conflict_delete_item: Counter,
    guard_conflict_update_item: Counter,
    unsupported_put_item: Counter,
    unsupported_delete_item: Counter,
    unsupported_update_item: Counter,
}

static CACHE_READ_OUTCOME_METRICS: LazyLock<CacheReadOutcomeMetricHandles> =
    LazyLock::new(|| CacheReadOutcomeMetricHandles {
        get_item_hit: metrics::counter!(STORAGE_DDB_GET_ITEM_CACHE_HIT_METRIC.name()),
        get_item_miss: metrics::counter!(STORAGE_DDB_GET_ITEM_CACHE_MISS_METRIC.name()),
        batch_get_item_hit: metrics::counter!(STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_METRIC.name()),
        batch_get_item_hit_partial: metrics::counter!(
            STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_PARTIAL_METRIC.name()
        ),
        batch_get_item_miss: metrics::counter!(STORAGE_DDB_BATCH_GET_ITEM_CACHE_MISS_METRIC.name()),
        query_hit: metrics::counter!(STORAGE_DDB_QUERY_CACHE_HIT_METRIC.name()),
        query_hit_partial: metrics::counter!(STORAGE_DDB_QUERY_CACHE_HIT_PARTIAL_METRIC.name()),
        query_miss: metrics::counter!(STORAGE_DDB_QUERY_CACHE_MISS_METRIC.name()),
    });

static CACHE_HIT_RATIO_METRICS: LazyLock<CacheHitRatioMetricHandles> =
    LazyLock::new(|| CacheHitRatioMetricHandles {
        get_item: metrics::gauge!(
            STORAGE_DDB_CACHE_HIT_RATIO_METRIC.name(),
            "operation" => "get_item",
        ),
        batch_get_item: metrics::gauge!(
            STORAGE_DDB_CACHE_HIT_RATIO_METRIC.name(),
            "operation" => "batch_get_item",
        ),
        query: metrics::gauge!(
            STORAGE_DDB_CACHE_HIT_RATIO_METRIC.name(),
            "operation" => "query",
        ),
    });

static GUARD_FALLBACK_METRICS: LazyLock<GuardFallbackMetricHandles> =
    LazyLock::new(|| GuardFallbackMetricHandles {
        guard_conflict_put_item: metrics::counter!(
            STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC.name(),
            "operation" => "put_item",
        ),
        guard_conflict_delete_item: metrics::counter!(
            STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC.name(),
            "operation" => "delete_item",
        ),
        guard_conflict_update_item: metrics::counter!(
            STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC.name(),
            "operation" => "update_item",
        ),
        unsupported_put_item: metrics::counter!(
            STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC.name(),
            "operation" => "put_item",
        ),
        unsupported_delete_item: metrics::counter!(
            STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC.name(),
            "operation" => "delete_item",
        ),
        unsupported_update_item: metrics::counter!(
            STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC.name(),
            "operation" => "update_item",
        ),
    });

#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
pub struct StorageCacheReadDiagnostics {
    pub get_item_hit: u64,
    pub get_item_miss: u64,
    pub get_item_hit_ratio: f64,
    pub batch_get_item_hit: u64,
    pub batch_get_item_hit_partial: u64,
    pub batch_get_item_miss: u64,
    pub batch_get_item_hit_ratio: f64,
    pub query_hit: u64,
    pub query_hit_partial: u64,
    pub query_miss: u64,
    pub query_hit_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageCacheReadOutcome {
    Hit,
    Miss,
    HitPartial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageCacheReadOperation {
    GetItem,
    BatchGetItem,
    Query,
}

pub fn record_storage_cache_read_outcome(
    operation: StorageCacheReadOperation,
    outcome: StorageCacheReadOutcome,
) {
    let metric = match (operation, outcome) {
        (StorageCacheReadOperation::GetItem, StorageCacheReadOutcome::Hit) => {
            GET_ITEM_CACHE_HIT_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.get_item_hit
        }
        (StorageCacheReadOperation::GetItem, StorageCacheReadOutcome::Miss) => {
            GET_ITEM_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.get_item_miss
        }
        (StorageCacheReadOperation::GetItem, StorageCacheReadOutcome::HitPartial) => {
            GET_ITEM_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.get_item_miss
        }
        (StorageCacheReadOperation::BatchGetItem, StorageCacheReadOutcome::Hit) => {
            BATCH_GET_ITEM_CACHE_HIT_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.batch_get_item_hit
        }
        (StorageCacheReadOperation::BatchGetItem, StorageCacheReadOutcome::Miss) => {
            BATCH_GET_ITEM_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.batch_get_item_miss
        }
        (StorageCacheReadOperation::BatchGetItem, StorageCacheReadOutcome::HitPartial) => {
            BATCH_GET_ITEM_CACHE_HIT_PARTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.batch_get_item_hit_partial
        }
        (StorageCacheReadOperation::Query, StorageCacheReadOutcome::Hit) => {
            QUERY_CACHE_HIT_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.query_hit
        }
        (StorageCacheReadOperation::Query, StorageCacheReadOutcome::Miss) => {
            QUERY_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.query_miss
        }
        (StorageCacheReadOperation::Query, StorageCacheReadOutcome::HitPartial) => {
            QUERY_CACHE_HIT_PARTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
            &CACHE_READ_OUTCOME_METRICS.query_hit_partial
        }
    };
    metric.increment(1);
    record_cache_hit_ratio(operation);
}

#[must_use]
pub fn storage_cache_read_diagnostics() -> StorageCacheReadDiagnostics {
    let get_item_hit = GET_ITEM_CACHE_HIT_COUNT.load(Ordering::Relaxed);
    let get_item_miss = GET_ITEM_CACHE_MISS_COUNT.load(Ordering::Relaxed);
    let batch_get_item_hit = BATCH_GET_ITEM_CACHE_HIT_COUNT.load(Ordering::Relaxed);
    let batch_get_item_hit_partial = BATCH_GET_ITEM_CACHE_HIT_PARTIAL_COUNT.load(Ordering::Relaxed);
    let batch_get_item_miss = BATCH_GET_ITEM_CACHE_MISS_COUNT.load(Ordering::Relaxed);
    let query_hit = QUERY_CACHE_HIT_COUNT.load(Ordering::Relaxed);
    let query_hit_partial = QUERY_CACHE_HIT_PARTIAL_COUNT.load(Ordering::Relaxed);
    let query_miss = QUERY_CACHE_MISS_COUNT.load(Ordering::Relaxed);
    StorageCacheReadDiagnostics {
        get_item_hit,
        get_item_miss,
        get_item_hit_ratio: hit_ratio(get_item_hit, 0, get_item_miss),
        batch_get_item_hit,
        batch_get_item_hit_partial,
        batch_get_item_miss,
        batch_get_item_hit_ratio: hit_ratio(
            batch_get_item_hit,
            batch_get_item_hit_partial,
            batch_get_item_miss,
        ),
        query_hit,
        query_hit_partial,
        query_miss,
        query_hit_ratio: hit_ratio(query_hit, query_hit_partial, query_miss),
    }
}

fn record_cache_hit_ratio(operation: StorageCacheReadOperation) {
    let (gauge, ratio) = match operation {
        StorageCacheReadOperation::GetItem => (
            &CACHE_HIT_RATIO_METRICS.get_item,
            hit_ratio(
                GET_ITEM_CACHE_HIT_COUNT.load(Ordering::Relaxed),
                0,
                GET_ITEM_CACHE_MISS_COUNT.load(Ordering::Relaxed),
            ),
        ),
        StorageCacheReadOperation::BatchGetItem => (
            &CACHE_HIT_RATIO_METRICS.batch_get_item,
            hit_ratio(
                BATCH_GET_ITEM_CACHE_HIT_COUNT.load(Ordering::Relaxed),
                BATCH_GET_ITEM_CACHE_HIT_PARTIAL_COUNT.load(Ordering::Relaxed),
                BATCH_GET_ITEM_CACHE_MISS_COUNT.load(Ordering::Relaxed),
            ),
        ),
        StorageCacheReadOperation::Query => (
            &CACHE_HIT_RATIO_METRICS.query,
            hit_ratio(
                QUERY_CACHE_HIT_COUNT.load(Ordering::Relaxed),
                QUERY_CACHE_HIT_PARTIAL_COUNT.load(Ordering::Relaxed),
                QUERY_CACHE_MISS_COUNT.load(Ordering::Relaxed),
            ),
        ),
    };
    gauge.set(ratio);
}

fn hit_ratio(hits: u64, partial_hits: u64, misses: u64) -> f64 {
    let hit_count = hits.saturating_add(partial_hits);
    let total = hit_count.saturating_add(misses);
    if total == 0 {
        0.0
    } else {
        hit_count as f64 / total as f64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageGuardFallbackReason {
    GuardConflict,
    Unsupported,
}

pub fn record_authoritative_preimage_hit(purpose: &'static str) {
    metrics_facade::counter!(STORAGE_DDB_AUTHORITATIVE_PREIMAGE_HIT_METRIC, "purpose" => purpose)
        .increment(1);
}

pub fn record_authoritative_preimage_miss(purpose: &'static str) {
    metrics_facade::counter!(STORAGE_DDB_AUTHORITATIVE_PREIMAGE_MISS_METRIC, "purpose" => purpose)
        .increment(1);
}

pub fn record_guard_fallback(operation: &'static str, reason: StorageGuardFallbackReason) {
    if let Some(metric) = guard_fallback_metric(operation, reason) {
        metric.increment(1);
        return;
    }
    let metric = match reason {
        StorageGuardFallbackReason::GuardConflict => STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC,
        StorageGuardFallbackReason::Unsupported => STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC,
    };
    metrics_facade::counter!(metric, "operation" => operation).increment(1);
}

fn guard_fallback_metric(
    operation: &'static str,
    reason: StorageGuardFallbackReason,
) -> Option<&'static Counter> {
    match (operation, reason) {
        ("put_item", StorageGuardFallbackReason::GuardConflict) => {
            Some(&GUARD_FALLBACK_METRICS.guard_conflict_put_item)
        }
        ("delete_item", StorageGuardFallbackReason::GuardConflict) => {
            Some(&GUARD_FALLBACK_METRICS.guard_conflict_delete_item)
        }
        ("update_item", StorageGuardFallbackReason::GuardConflict) => {
            Some(&GUARD_FALLBACK_METRICS.guard_conflict_update_item)
        }
        ("put_item", StorageGuardFallbackReason::Unsupported) => {
            Some(&GUARD_FALLBACK_METRICS.unsupported_put_item)
        }
        ("delete_item", StorageGuardFallbackReason::Unsupported) => {
            Some(&GUARD_FALLBACK_METRICS.unsupported_delete_item)
        }
        ("update_item", StorageGuardFallbackReason::Unsupported) => {
            Some(&GUARD_FALLBACK_METRICS.unsupported_update_item)
        }
        _ => None,
    }
}
