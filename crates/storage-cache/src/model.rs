use std::collections::BTreeSet;

use crate::{
    plan::{
        BatchGetDecision, BatchGetPlan, CacheReadOutcome, CacheRouteOutcome, QueryDecision,
        QueryPlan,
    },
    query::{GsiQuerySpace, PartitionId, QueryDirection, QueryRequest, QueryTarget},
};

pub type Slot = u8;
pub type Epoch = u8;
pub type SchemaVersion = u8;
pub type GsiSchemaVersion = u8;

const SLOT_ORDER: [Slot; 4] = [0, 1, 2, 3];
const SLOT_ORDER_LEFT: [Slot; 2] = [0, 1];
const SLOT_ORDER_LEFT_DESC: [Slot; 2] = [1, 0];
const SLOT_ORDER_RIGHT: [Slot; 2] = [2, 3];
const SLOT_ORDER_RIGHT_DESC: [Slot; 2] = [3, 2];
const PRIMARY_GSI_ORDER_V0: [Slot; 4] = [0, 1, 2, 3];
const PRIMARY_GSI_ORDER_V1: [Slot; 4] = [1, 0, 2, 3];
const PRIMARY_GSI_ORDER_V0_DESC: [Slot; 4] = [3, 2, 1, 0];
const PRIMARY_GSI_ORDER_V1_DESC: [Slot; 4] = [3, 2, 0, 1];
const ALT_GSI_ORDER_V0: [Slot; 4] = [0, 1, 2, 3];
const ALT_GSI_ORDER_V1: [Slot; 4] = [0, 1, 3, 2];
const ALT_GSI_ORDER_V0_DESC: [Slot; 4] = [3, 2, 1, 0];
const ALT_GSI_ORDER_V1_DESC: [Slot; 4] = [2, 3, 1, 0];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum GsiOrderVersion {
    #[default]
    V0,
    V1,
}

impl GsiOrderVersion {
    #[must_use]
    pub const fn next(self) -> Self {
        match self {
            Self::V0 => Self::V1,
            Self::V1 => Self::V0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeId {
    #[default]
    Leader,
    Follower,
}

impl NodeId {
    pub const ALL: [Self; 2] = [Self::Leader, Self::Follower];

    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::Leader => Self::Follower,
            Self::Follower => Self::Leader,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ItemCacheState {
    pub payload_keys: BTreeSet<Slot>,
    pub negative_keys: BTreeSet<Slot>,
    pub manifest_keys: BTreeSet<Slot>,
    pub covered_slots: BTreeSet<Slot>,
    pub current_schema_covered_slots: BTreeSet<Slot>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexCacheState {
    pub manifest_keys: BTreeSet<Slot>,
    pub covered_slots: BTreeSet<Slot>,
    pub current_schema_covered_slots: BTreeSet<Slot>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalReplicaState {
    pub items: ItemCacheState,
    pub primary_gsi: IndexCacheState,
    pub alternate_gsi: IndexCacheState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheState {
    pub db_present: BTreeSet<Slot>,
    pub gsi_present: BTreeSet<Slot>,
    pub gsi_alt_present: BTreeSet<Slot>,
    pub leader: LocalReplicaState,
    pub follower: LocalReplicaState,
    pub prepared_puts: BTreeSet<Slot>,
    pub prepared_deletes: BTreeSet<Slot>,
    pub serving_node: NodeId,
    pub actual_leader_node: NodeId,
    pub actual_epoch: Epoch,
    pub cache_epoch: Epoch,
    pub actual_base_schema_version: SchemaVersion,
    pub actual_gsi_schema_version: GsiSchemaVersion,
    pub actual_primary_gsi_order_version: GsiOrderVersion,
    pub actual_alternate_gsi_order_version: GsiOrderVersion,
    pub leader_primary_gsi_order_version: GsiOrderVersion,
    pub follower_primary_gsi_order_version: GsiOrderVersion,
    pub leader_alternate_gsi_order_version: GsiOrderVersion,
    pub follower_alternate_gsi_order_version: GsiOrderVersion,
    pub cached_writes_only: bool,
    pub item_authority: bool,
    pub query_authority: bool,
    pub gsi_query_authority: bool,
    pub manifest_rebuilding: bool,
    pub continuity_broken: bool,
    pub shard_busted: bool,
    pub shard_assigned: bool,
}

impl Default for CacheState {
    fn default() -> Self {
        Self::authoritative_leader_base_state()
    }
}

impl CacheState {
    #[must_use]
    pub fn authoritative_leader_base_state() -> Self {
        Self {
            db_present: BTreeSet::new(),
            gsi_present: BTreeSet::new(),
            gsi_alt_present: BTreeSet::new(),
            leader: LocalReplicaState::default(),
            follower: LocalReplicaState::default(),
            prepared_puts: BTreeSet::new(),
            prepared_deletes: BTreeSet::new(),
            serving_node: NodeId::Leader,
            actual_leader_node: NodeId::Leader,
            actual_epoch: 0,
            cache_epoch: 0,
            actual_base_schema_version: 0,
            actual_gsi_schema_version: 0,
            actual_primary_gsi_order_version: GsiOrderVersion::V0,
            actual_alternate_gsi_order_version: GsiOrderVersion::V0,
            leader_primary_gsi_order_version: GsiOrderVersion::V0,
            follower_primary_gsi_order_version: GsiOrderVersion::V0,
            leader_alternate_gsi_order_version: GsiOrderVersion::V0,
            follower_alternate_gsi_order_version: GsiOrderVersion::V0,
            cached_writes_only: true,
            item_authority: true,
            query_authority: true,
            gsi_query_authority: true,
            manifest_rebuilding: false,
            continuity_broken: false,
            shard_busted: false,
            shard_assigned: true,
        }
    }

    #[must_use]
    pub fn uninitialized_shard_base_state() -> Self {
        Self {
            item_authority: false,
            query_authority: false,
            gsi_query_authority: false,
            shard_assigned: false,
            ..Self::authoritative_leader_base_state()
        }
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        let stale_slots = self.unresolved_intents();
        self.gsi_present.is_subset(&self.db_present)
            && self.gsi_alt_present.is_subset(&self.db_present)
            && self.gsi_present.is_disjoint(&self.gsi_alt_present)
            && Self::valid_local_cache_state(&self.leader.items, &self.db_present, &stale_slots)
            && Self::valid_local_cache_state(&self.follower.items, &self.db_present, &stale_slots)
            && Self::valid_index_cache_state(
                &self.leader.primary_gsi,
                &self.gsi_present,
                &stale_slots,
            )
            && Self::valid_index_cache_state(
                &self.leader.alternate_gsi,
                &self.gsi_alt_present,
                &stale_slots,
            )
            && Self::valid_index_cache_state(
                &self.follower.primary_gsi,
                &self.gsi_present,
                &stale_slots,
            )
            && Self::valid_index_cache_state(
                &self.follower.alternate_gsi,
                &self.gsi_alt_present,
                &stale_slots,
            )
            && self.prepared_puts.is_disjoint(&self.prepared_deletes)
            && (!self.item_authority || self.cache_epoch == self.actual_epoch)
            && (!self.query_authority || self.item_authority)
            && (!self.gsi_query_authority || self.item_authority)
            && (!self.shard_busted
                || (!self.item_authority && !self.query_authority && !self.gsi_query_authority))
            && (!self.item_authority || self.shard_assigned)
    }

    fn valid_local_cache_state(
        state: &ItemCacheState,
        db_present: &BTreeSet<Slot>,
        stale_slots: &BTreeSet<Slot>,
    ) -> bool {
        let absent = Self::absent_slots(db_present);
        let payload_keys = Self::difference(&state.payload_keys, stale_slots);
        let negative_keys = Self::difference(&state.negative_keys, stale_slots);
        let manifest_keys = Self::difference(&state.manifest_keys, stale_slots);
        let present_inside_coverage = Self::difference(
            &Self::intersection(db_present, &state.covered_slots),
            stale_slots,
        );

        payload_keys.is_subset(db_present)
            && negative_keys.is_subset(&absent)
            && manifest_keys.is_subset(db_present)
            && present_inside_coverage.is_subset(&state.manifest_keys)
            && state
                .current_schema_covered_slots
                .is_subset(&state.covered_slots)
    }

    fn valid_index_cache_state(
        state: &IndexCacheState,
        source_keys: &BTreeSet<Slot>,
        stale_slots: &BTreeSet<Slot>,
    ) -> bool {
        let manifest_keys = Self::difference(&state.manifest_keys, stale_slots);
        let present_inside_coverage = Self::difference(
            &Self::intersection(source_keys, &state.covered_slots),
            stale_slots,
        );

        manifest_keys.is_subset(source_keys)
            && present_inside_coverage.is_subset(&state.manifest_keys)
            && state
                .current_schema_covered_slots
                .is_subset(&state.covered_slots)
    }

    #[must_use]
    pub fn unresolved_intents(&self) -> BTreeSet<Slot> {
        Self::union(&self.prepared_puts, &self.prepared_deletes)
    }

    #[must_use]
    pub fn fresh_request_epoch(&self) -> Epoch {
        self.actual_epoch
    }

    #[must_use]
    pub fn stale_request_epoch(&self) -> Epoch {
        if self.actual_epoch == 0 { 1 } else { 0 }
    }

    #[must_use]
    pub fn serving_replica(&self) -> &LocalReplicaState {
        match self.serving_node {
            NodeId::Leader => &self.leader,
            NodeId::Follower => &self.follower,
        }
    }

    #[must_use]
    pub fn serving_manifest_keys(&self, query: &QueryRequest) -> &BTreeSet<Slot> {
        match query.target {
            QueryTarget::Base => &self.serving_replica().items.manifest_keys,
            QueryTarget::Gsi(GsiQuerySpace::Primary) => {
                &self.serving_replica().primary_gsi.manifest_keys
            }
            QueryTarget::Gsi(GsiQuerySpace::Alternate) => {
                &self.serving_replica().alternate_gsi.manifest_keys
            }
        }
    }

    #[must_use]
    pub fn serving_covered_slots(&self, query: &QueryRequest) -> &BTreeSet<Slot> {
        match query.target {
            QueryTarget::Base => &self.serving_replica().items.covered_slots,
            QueryTarget::Gsi(GsiQuerySpace::Primary) => {
                &self.serving_replica().primary_gsi.covered_slots
            }
            QueryTarget::Gsi(GsiQuerySpace::Alternate) => {
                &self.serving_replica().alternate_gsi.covered_slots
            }
        }
    }

    #[must_use]
    pub fn serving_current_schema_covered_slots(&self, query: &QueryRequest) -> &BTreeSet<Slot> {
        match query.target {
            QueryTarget::Base => &self.serving_replica().items.current_schema_covered_slots,
            QueryTarget::Gsi(GsiQuerySpace::Primary) => {
                &self
                    .serving_replica()
                    .primary_gsi
                    .current_schema_covered_slots
            }
            QueryTarget::Gsi(GsiQuerySpace::Alternate) => {
                &self
                    .serving_replica()
                    .alternate_gsi
                    .current_schema_covered_slots
            }
        }
    }

    #[must_use]
    pub fn source_keys(&self, query: &QueryRequest) -> &BTreeSet<Slot> {
        match query.target {
            QueryTarget::Base => &self.db_present,
            QueryTarget::Gsi(GsiQuerySpace::Primary) => &self.gsi_present,
            QueryTarget::Gsi(GsiQuerySpace::Alternate) => &self.gsi_alt_present,
        }
    }

    #[must_use]
    pub fn item_proof_active(&self) -> bool {
        self.serving_node == self.actual_leader_node
            && self.cache_epoch == self.actual_epoch
            && self.cached_writes_only
            && self.item_authority
            && !self.shard_busted
            && self.shard_assigned
    }

    #[must_use]
    pub fn query_proof_active(&self, query: &QueryRequest) -> bool {
        self.item_proof_active()
            && match query.target {
                QueryTarget::Base => self.query_authority,
                QueryTarget::Gsi(_) => self.gsi_query_authority,
            }
            && self.manifest_order_current(query)
            && !self.manifest_rebuilding
            && !self.continuity_broken
            && !self.query_touches_unresolved_intent(query)
    }

    #[must_use]
    pub fn can_serve_eventual_get(&self, slot: Slot) -> bool {
        self.item_proof_active()
            && !self.slot_has_unresolved_intent(slot)
            && (self.serving_replica().items.payload_keys.contains(&slot)
                || self.serving_replica().items.negative_keys.contains(&slot))
    }

    #[must_use]
    pub fn can_serve_strong_get(&self, slot: Slot) -> bool {
        self.can_serve_eventual_get(slot)
    }

    #[must_use]
    pub fn eventual_get_matches_source(&self, slot: Slot) -> bool {
        (self.serving_replica().items.payload_keys.contains(&slot)
            && self.db_present.contains(&slot))
            || (self.serving_replica().items.negative_keys.contains(&slot)
                && !self.db_present.contains(&slot))
    }

    #[must_use]
    pub fn batch_get_plan(&self, strong: bool, requested_keys: &BTreeSet<Slot>) -> BatchGetPlan {
        let served_keys: BTreeSet<Slot> = if strong {
            requested_keys
                .iter()
                .copied()
                .filter(|slot| self.can_serve_strong_get(*slot))
                .collect()
        } else {
            requested_keys
                .iter()
                .copied()
                .filter(|slot| self.can_serve_eventual_get(*slot))
                .collect()
        };

        let cache_payload_keys = served_keys
            .iter()
            .copied()
            .filter(|slot| self.serving_replica().items.payload_keys.contains(slot))
            .collect();
        let cache_negative_keys = served_keys
            .iter()
            .copied()
            .filter(|slot| self.serving_replica().items.negative_keys.contains(slot))
            .collect();

        BatchGetPlan {
            served_keys: served_keys.clone(),
            fallback_keys: Self::difference(requested_keys, &served_keys),
            cache_payload_keys,
            cache_negative_keys,
        }
    }

    #[must_use]
    pub fn request_route(&self, request_epoch: Epoch) -> CacheRouteOutcome {
        if request_epoch != self.actual_epoch {
            CacheRouteOutcome::StaleEpoch
        } else if self.serving_node != self.actual_leader_node {
            CacheRouteOutcome::WrongLeader
        } else {
            CacheRouteOutcome::Ok
        }
    }

    #[must_use]
    pub fn eventual_get_decision(&self, slot: Slot, request_epoch: Epoch) -> CacheReadOutcome {
        match self.request_route(request_epoch) {
            CacheRouteOutcome::Ok => {
                if self.can_serve_eventual_get(slot) {
                    CacheReadOutcome::ServeCache
                } else {
                    CacheReadOutcome::FallbackDb
                }
            }
            CacheRouteOutcome::StaleEpoch | CacheRouteOutcome::WrongLeader => {
                CacheReadOutcome::FallbackDb
            }
        }
    }

    #[must_use]
    pub fn strong_get_decision(&self, slot: Slot, request_epoch: Epoch) -> CacheReadOutcome {
        match self.request_route(request_epoch) {
            CacheRouteOutcome::Ok => {
                if self.can_serve_strong_get(slot) {
                    CacheReadOutcome::ServeCache
                } else {
                    CacheReadOutcome::FallbackDb
                }
            }
            CacheRouteOutcome::StaleEpoch | CacheRouteOutcome::WrongLeader => {
                CacheReadOutcome::FallbackDb
            }
        }
    }

    #[must_use]
    pub fn batch_get_decision(
        &self,
        strong: bool,
        requested_keys: &BTreeSet<Slot>,
        request_epoch: Epoch,
    ) -> BatchGetDecision {
        let route = self.request_route(request_epoch);
        if route != CacheRouteOutcome::Ok {
            return BatchGetDecision {
                route,
                outcome: CacheReadOutcome::FallbackDb,
                served_keys: BTreeSet::new(),
                fallback_keys: requested_keys.clone(),
            };
        }

        let plan = self.batch_get_plan(strong, requested_keys);
        let outcome = if plan.served_keys.is_empty() {
            CacheReadOutcome::FallbackDb
        } else if plan.fallback_keys.is_empty() {
            CacheReadOutcome::ServeCache
        } else {
            CacheReadOutcome::Mixed
        };

        BatchGetDecision {
            route,
            outcome,
            served_keys: plan.served_keys,
            fallback_keys: plan.fallback_keys,
        }
    }

    #[must_use]
    pub fn query_decision(
        &self,
        query: &QueryRequest,
        strong: bool,
        request_epoch: Epoch,
    ) -> QueryDecision {
        let route = self.request_route(request_epoch);
        if route != CacheRouteOutcome::Ok {
            return QueryDecision {
                route,
                outcome: CacheReadOutcome::FallbackDb,
                serve_whole_page: false,
                cache_evaluated_keys: Vec::new(),
            };
        }
        if strong && query.target.is_gsi() {
            return QueryDecision {
                route,
                outcome: CacheReadOutcome::InvalidGsiStrong,
                serve_whole_page: false,
                cache_evaluated_keys: Vec::new(),
            };
        }
        if strong {
            return QueryDecision {
                route,
                outcome: CacheReadOutcome::FallbackDb,
                serve_whole_page: false,
                cache_evaluated_keys: Vec::new(),
            };
        }

        let plan = self.cache_plan(query);
        QueryDecision {
            route,
            outcome: if plan.serve_whole_page {
                CacheReadOutcome::ServeCache
            } else if plan.cache_evaluated_keys.is_empty() {
                CacheReadOutcome::FallbackDb
            } else {
                CacheReadOutcome::Mixed
            },
            serve_whole_page: plan.serve_whole_page,
            cache_evaluated_keys: plan.cache_evaluated_keys,
        }
    }

    #[must_use]
    pub fn cache_plan(&self, query: &QueryRequest) -> QueryPlan {
        let cache_prefix_slots = self.covered_prefix_slots(query);
        let prefix_raw = self.cache_prefix_raw_keys(query);
        let cache_evaluated_keys = self.cache_raw_page(query);
        let source_raw_page = self.source_raw_page(query);
        let exhaustive_to_boundary = cache_prefix_slots.len() == self.candidate_slots(query).len();

        QueryPlan {
            proof_active: self.query_proof_active(query),
            cache_prefix_slots,
            cache_returned_keys: self.cache_returned_page(query),
            evaluated_bytes: Self::raw_page_bytes(&cache_evaluated_keys, query),
            payload_misses: self.payload_misses_on_cache_page(query),
            serve_whole_page: self.query_proof_active(query)
                && (self.raw_stop_satisfied_within_prefix(&prefix_raw, query)
                    || exhaustive_to_boundary),
            db_suffix_needed: cache_evaluated_keys.len() < source_raw_page.len(),
            cache_evaluated_keys,
        }
    }

    #[must_use]
    pub fn source_raw_keys(&self, query: &QueryRequest) -> Vec<Slot> {
        self.candidate_slots(query)
            .into_iter()
            .filter(|slot| self.source_keys(query).contains(slot))
            .collect()
    }

    #[must_use]
    pub fn source_raw_page(&self, query: &QueryRequest) -> Vec<Slot> {
        Self::take_within_raw_boundary(&self.source_raw_keys(query), query)
    }

    #[must_use]
    pub fn source_returned_page(&self, query: &QueryRequest) -> Vec<Slot> {
        self.source_raw_page(query)
            .into_iter()
            .filter(|slot| Self::filter_matches(query, *slot))
            .collect()
    }

    #[must_use]
    pub fn cache_returned_page(&self, query: &QueryRequest) -> Vec<Slot> {
        self.cache_raw_page(query)
            .into_iter()
            .filter(|slot| Self::filter_matches(query, *slot))
            .collect()
    }

    #[must_use]
    pub fn covered_prefix_slots(&self, query: &QueryRequest) -> Vec<Slot> {
        if !self.query_proof_active(query) {
            return Vec::new();
        }

        let mut prefix = Vec::new();
        let effective_coverage = self.effective_covered_slots(query);
        for slot in self.candidate_slots(query) {
            if effective_coverage.contains(&slot) {
                prefix.push(slot);
            } else {
                break;
            }
        }
        prefix
    }

    #[must_use]
    pub fn cache_prefix_raw_keys(&self, query: &QueryRequest) -> Vec<Slot> {
        self.covered_prefix_slots(query)
            .into_iter()
            .filter(|slot| self.serving_manifest_keys(query).contains(slot))
            .collect()
    }

    #[must_use]
    pub fn cache_raw_page(&self, query: &QueryRequest) -> Vec<Slot> {
        Self::take_within_raw_boundary(&self.cache_prefix_raw_keys(query), query)
    }

    #[must_use]
    pub fn effective_covered_slots(&self, query: &QueryRequest) -> BTreeSet<Slot> {
        Self::intersection(
            self.serving_covered_slots(query),
            self.serving_current_schema_covered_slots(query),
        )
    }

    #[must_use]
    pub fn query_touches_unresolved_intent(&self, query: &QueryRequest) -> bool {
        self.candidate_slots(query)
            .into_iter()
            .any(|slot| self.slot_has_unresolved_intent(slot))
    }

    #[must_use]
    pub fn slot_has_unresolved_intent(&self, slot: Slot) -> bool {
        self.prepared_puts.contains(&slot) || self.prepared_deletes.contains(&slot)
    }

    #[must_use]
    pub fn candidate_slots(&self, query: &QueryRequest) -> Vec<Slot> {
        self.ordered_slots(query)
            .iter()
            .copied()
            .filter(|slot| self.slot_in_range(query, *slot))
            .collect()
    }

    #[must_use]
    pub fn ordered_slots(&self, query: &QueryRequest) -> &'static [Slot] {
        match query.target {
            QueryTarget::Base => match (query.partition, query.direction) {
                (PartitionId::Left, QueryDirection::Forward) => &SLOT_ORDER_LEFT,
                (PartitionId::Left, QueryDirection::Reverse) => &SLOT_ORDER_LEFT_DESC,
                (PartitionId::Right, QueryDirection::Forward) => &SLOT_ORDER_RIGHT,
                (PartitionId::Right, QueryDirection::Reverse) => &SLOT_ORDER_RIGHT_DESC,
            },
            QueryTarget::Gsi(GsiQuerySpace::Primary) => {
                match (self.actual_primary_gsi_order_version, query.direction) {
                    (GsiOrderVersion::V0, QueryDirection::Forward) => &PRIMARY_GSI_ORDER_V0,
                    (GsiOrderVersion::V1, QueryDirection::Forward) => &PRIMARY_GSI_ORDER_V1,
                    (GsiOrderVersion::V0, QueryDirection::Reverse) => &PRIMARY_GSI_ORDER_V0_DESC,
                    (GsiOrderVersion::V1, QueryDirection::Reverse) => &PRIMARY_GSI_ORDER_V1_DESC,
                }
            }
            QueryTarget::Gsi(GsiQuerySpace::Alternate) => {
                match (self.actual_alternate_gsi_order_version, query.direction) {
                    (GsiOrderVersion::V0, QueryDirection::Forward) => &ALT_GSI_ORDER_V0,
                    (GsiOrderVersion::V1, QueryDirection::Forward) => &ALT_GSI_ORDER_V1,
                    (GsiOrderVersion::V0, QueryDirection::Reverse) => &ALT_GSI_ORDER_V0_DESC,
                    (GsiOrderVersion::V1, QueryDirection::Reverse) => &ALT_GSI_ORDER_V1_DESC,
                }
            }
        }
    }

    #[must_use]
    pub fn actual_gsi_order_version(&self, query: &QueryRequest) -> Option<GsiOrderVersion> {
        match query.target {
            QueryTarget::Base => None,
            QueryTarget::Gsi(GsiQuerySpace::Primary) => Some(self.actual_primary_gsi_order_version),
            QueryTarget::Gsi(GsiQuerySpace::Alternate) => {
                Some(self.actual_alternate_gsi_order_version)
            }
        }
    }

    #[must_use]
    pub fn serving_gsi_order_version(&self, query: &QueryRequest) -> Option<GsiOrderVersion> {
        match (self.serving_node, query.target) {
            (_, QueryTarget::Base) => None,
            (NodeId::Leader, QueryTarget::Gsi(GsiQuerySpace::Primary)) => {
                Some(self.leader_primary_gsi_order_version)
            }
            (NodeId::Follower, QueryTarget::Gsi(GsiQuerySpace::Primary)) => {
                Some(self.follower_primary_gsi_order_version)
            }
            (NodeId::Leader, QueryTarget::Gsi(GsiQuerySpace::Alternate)) => {
                Some(self.leader_alternate_gsi_order_version)
            }
            (NodeId::Follower, QueryTarget::Gsi(GsiQuerySpace::Alternate)) => {
                Some(self.follower_alternate_gsi_order_version)
            }
        }
    }

    #[must_use]
    pub fn manifest_order_current(&self, query: &QueryRequest) -> bool {
        self.actual_gsi_order_version(query) == self.serving_gsi_order_version(query)
    }

    fn slot_in_range(&self, query: &QueryRequest, slot: Slot) -> bool {
        query.lower_bound <= slot
            && slot <= query.upper_bound
            && match query.direction {
                QueryDirection::Forward => i16::from(slot) > i16::from(query.start_exclusive),
                QueryDirection::Reverse => i16::from(slot) < i16::from(query.start_exclusive),
            }
    }

    fn payload_misses_on_cache_page(&self, query: &QueryRequest) -> usize {
        self.cache_raw_page(query)
            .into_iter()
            .filter(|slot| !self.serving_replica().items.payload_keys.contains(slot))
            .count()
    }

    fn raw_stop_satisfied_within_prefix(&self, prefix_raw: &[Slot], query: &QueryRequest) -> bool {
        let page = Self::take_within_raw_boundary(prefix_raw, query);
        page.len() == query.limit
            || Self::raw_page_bytes(&page, query) == query.byte_budget
            || page.len() < prefix_raw.len()
    }

    fn take_within_raw_boundary(raw_keys: &[Slot], query: &QueryRequest) -> Vec<Slot> {
        let mut page = Vec::new();
        let mut current_bytes = 0usize;
        for slot in raw_keys {
            let next_bytes = current_bytes + Self::raw_item_bytes(query, *slot);
            if page.len() < query.limit && next_bytes <= query.byte_budget {
                page.push(*slot);
                current_bytes = next_bytes;
            } else {
                break;
            }
        }
        page
    }

    #[must_use]
    pub fn raw_page_bytes(raw_keys: &[Slot], query: &QueryRequest) -> usize {
        raw_keys
            .iter()
            .copied()
            .map(|slot| Self::raw_item_bytes(query, slot))
            .sum()
    }

    fn raw_item_bytes(query: &QueryRequest, slot: Slot) -> usize {
        match query.target {
            QueryTarget::Base => Self::base_raw_item_bytes(slot),
            QueryTarget::Gsi(_) => Self::gsi_raw_item_bytes(slot),
        }
    }

    fn base_raw_item_bytes(slot: Slot) -> usize {
        match slot {
            0 => 128,
            1 => 256,
            2 => 384,
            _ => 640,
        }
    }

    fn gsi_raw_item_bytes(slot: Slot) -> usize {
        match slot {
            0 => 96,
            1 => 160,
            2 => 224,
            _ => 320,
        }
    }

    fn filter_matches(query: &QueryRequest, slot: Slot) -> bool {
        !query.only_even || slot.is_multiple_of(2)
    }

    #[must_use]
    pub fn absent_slots(db_present: &BTreeSet<Slot>) -> BTreeSet<Slot> {
        SLOT_ORDER
            .iter()
            .copied()
            .filter(|slot| !db_present.contains(slot))
            .collect()
    }

    #[must_use]
    pub fn slots() -> &'static [Slot] {
        &SLOT_ORDER
    }

    #[must_use]
    pub fn intersection(left: &BTreeSet<Slot>, right: &BTreeSet<Slot>) -> BTreeSet<Slot> {
        left.intersection(right).copied().collect()
    }

    #[must_use]
    pub fn union(left: &BTreeSet<Slot>, right: &BTreeSet<Slot>) -> BTreeSet<Slot> {
        left.union(right).copied().collect()
    }

    #[must_use]
    pub fn difference(left: &BTreeSet<Slot>, right: &BTreeSet<Slot>) -> BTreeSet<Slot> {
        left.difference(right).copied().collect()
    }
}
