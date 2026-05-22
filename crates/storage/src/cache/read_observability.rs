use std::sync::atomic::{AtomicU64, Ordering};

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
            STORAGE_DDB_GET_ITEM_CACHE_HIT_METRIC
        }
        (StorageCacheReadOperation::GetItem, StorageCacheReadOutcome::Miss) => {
            GET_ITEM_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_GET_ITEM_CACHE_MISS_METRIC
        }
        (StorageCacheReadOperation::GetItem, StorageCacheReadOutcome::HitPartial) => {
            GET_ITEM_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_GET_ITEM_CACHE_MISS_METRIC
        }
        (StorageCacheReadOperation::BatchGetItem, StorageCacheReadOutcome::Hit) => {
            BATCH_GET_ITEM_CACHE_HIT_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_METRIC
        }
        (StorageCacheReadOperation::BatchGetItem, StorageCacheReadOutcome::Miss) => {
            BATCH_GET_ITEM_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_BATCH_GET_ITEM_CACHE_MISS_METRIC
        }
        (StorageCacheReadOperation::BatchGetItem, StorageCacheReadOutcome::HitPartial) => {
            BATCH_GET_ITEM_CACHE_HIT_PARTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_BATCH_GET_ITEM_CACHE_HIT_PARTIAL_METRIC
        }
        (StorageCacheReadOperation::Query, StorageCacheReadOutcome::Hit) => {
            QUERY_CACHE_HIT_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_QUERY_CACHE_HIT_METRIC
        }
        (StorageCacheReadOperation::Query, StorageCacheReadOutcome::Miss) => {
            QUERY_CACHE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_QUERY_CACHE_MISS_METRIC
        }
        (StorageCacheReadOperation::Query, StorageCacheReadOutcome::HitPartial) => {
            QUERY_CACHE_HIT_PARTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
            STORAGE_DDB_QUERY_CACHE_HIT_PARTIAL_METRIC
        }
    };
    metrics_facade::counter!(metric).increment(1);
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
    let diagnostics = storage_cache_read_diagnostics();
    let (operation_label, ratio) = match operation {
        StorageCacheReadOperation::GetItem => ("get_item", diagnostics.get_item_hit_ratio),
        StorageCacheReadOperation::BatchGetItem => {
            ("batch_get_item", diagnostics.batch_get_item_hit_ratio)
        }
        StorageCacheReadOperation::Query => ("query", diagnostics.query_hit_ratio),
    };
    metrics_facade::gauge!(STORAGE_DDB_CACHE_HIT_RATIO_METRIC, "operation" => operation_label)
        .set(ratio);
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
    let metric = match reason {
        StorageGuardFallbackReason::GuardConflict => STORAGE_DDB_GUARD_CONFLICT_FALLBACK_METRIC,
        StorageGuardFallbackReason::Unsupported => STORAGE_DDB_GUARD_UNSUPPORTED_FALLBACK_METRIC,
    };
    metrics_facade::counter!(metric, "operation" => operation).increment(1);
}
