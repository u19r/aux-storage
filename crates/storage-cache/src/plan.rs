use std::collections::BTreeSet;

use crate::model::Slot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheRouteOutcome {
    Ok,
    StaleEpoch,
    WrongLeader,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheReadOutcome {
    ServeCache,
    FallbackDb,
    Mixed,
    InvalidGsiStrong,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchGetPlan {
    pub served_keys: BTreeSet<Slot>,
    pub fallback_keys: BTreeSet<Slot>,
    pub cache_payload_keys: BTreeSet<Slot>,
    pub cache_negative_keys: BTreeSet<Slot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchGetDecision {
    pub route: CacheRouteOutcome,
    pub outcome: CacheReadOutcome,
    pub served_keys: BTreeSet<Slot>,
    pub fallback_keys: BTreeSet<Slot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub proof_active: bool,
    pub cache_prefix_slots: Vec<Slot>,
    pub cache_evaluated_keys: Vec<Slot>,
    pub cache_returned_keys: Vec<Slot>,
    pub evaluated_bytes: usize,
    pub payload_misses: usize,
    pub serve_whole_page: bool,
    pub db_suffix_needed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDecision {
    pub route: CacheRouteOutcome,
    pub outcome: CacheReadOutcome,
    pub serve_whole_page: bool,
    pub cache_evaluated_keys: Vec<Slot>,
}
