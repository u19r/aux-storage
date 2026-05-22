use std::collections::BTreeSet;

use crate::{
    model::{
        CacheState, GsiOrderVersion, IndexCacheState, ItemCacheState, LocalReplicaState, NodeId,
        SchemaVersion, Slot,
    },
    query::GsiQuerySpace,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionRange {
    pub lower_bound: Slot,
    pub upper_bound: Slot,
}

impl TransitionRange {
    #[must_use]
    pub const fn new(lower_bound: Slot, upper_bound: Slot) -> Self {
        Self {
            lower_bound,
            upper_bound,
        }
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.lower_bound <= self.upper_bound
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transition {
    PreparePut {
        slot: Slot,
    },
    PrepareDelete {
        slot: Slot,
    },
    AbortPrepared {
        slot: Slot,
    },
    LeaderCommitPut {
        slot: Slot,
    },
    FollowerAcknowledgePut {
        slot: Slot,
    },
    LeaderCommitDelete {
        slot: Slot,
    },
    FollowerAcknowledgeDelete {
        slot: Slot,
    },
    QueryFillBase {
        range: TransitionRange,
    },
    QueryFillGsi {
        query_space: GsiQuerySpace,
        range: TransitionRange,
    },
    SyncFollowerFromLeader,
    AdvanceBaseSchemaVersion,
    AdvanceGsiSchemaVersion,
    RewriteGsiSortOrder {
        query_space: GsiQuerySpace,
    },
    AddGsiMembership {
        slot: Slot,
    },
    RemoveGsiMembership {
        slot: Slot,
    },
    MoveGsiMembership {
        slot: Slot,
        to_query_space: GsiQuerySpace,
    },
    LoseLeader,
    RegainLeader,
    LoseEpochAuthority,
    GainEpochAuthority,
    PromoteFollowerCatchingUp,
    RecoverPreparedOnFollower,
    FinishCatchUp,
    BustShard,
    AssignShard,
    DrainShard,
    PartialSyncFollower {
        synced_slot_mask: u8,
    },
    DropFollowerReplication {
        slot: Slot,
    },
}

impl CacheState {
    #[must_use]
    pub fn try_apply(&self, transition: Transition) -> Option<Self> {
        let next = match transition {
            Transition::PreparePut { slot } => self.prepare_put(slot)?,
            Transition::PrepareDelete { slot } => self.prepare_delete(slot)?,
            Transition::AbortPrepared { slot } => self.abort_prepared(slot)?,
            Transition::LeaderCommitPut { slot } => self.leader_commit_put(slot)?,
            Transition::FollowerAcknowledgePut { slot } => self.follower_acknowledge_put(slot)?,
            Transition::LeaderCommitDelete { slot } => self.leader_commit_delete(slot)?,
            Transition::FollowerAcknowledgeDelete { slot } => {
                self.follower_acknowledge_delete(slot)?
            }
            Transition::QueryFillBase { range } => self.query_fill_base(range)?,
            Transition::QueryFillGsi { query_space, range } => {
                self.query_fill_gsi(query_space, range)?
            }
            Transition::SyncFollowerFromLeader => self.sync_follower_from_leader(),
            Transition::AdvanceBaseSchemaVersion => self.advance_base_schema_version(),
            Transition::AdvanceGsiSchemaVersion => self.advance_gsi_schema_version(),
            Transition::RewriteGsiSortOrder { query_space } => {
                self.rewrite_gsi_sort_order(query_space)?
            }
            Transition::AddGsiMembership { slot } => self.add_gsi_membership(slot)?,
            Transition::RemoveGsiMembership { slot } => self.remove_gsi_membership(slot)?,
            Transition::MoveGsiMembership {
                slot,
                to_query_space,
            } => self.move_gsi_membership(slot, to_query_space)?,
            Transition::LoseLeader => self.lose_leader(),
            Transition::RegainLeader => self.regain_leader(),
            Transition::LoseEpochAuthority => self.lose_epoch_authority(),
            Transition::GainEpochAuthority => self.gain_epoch_authority()?,
            Transition::PromoteFollowerCatchingUp => self.promote_follower_catching_up()?,
            Transition::RecoverPreparedOnFollower => self.recover_prepared_on_follower()?,
            Transition::FinishCatchUp => self.finish_catch_up()?,
            Transition::BustShard => self.bust_shard(),
            Transition::AssignShard => self.assign_shard()?,
            Transition::DrainShard => self.drain_shard()?,
            Transition::PartialSyncFollower { synced_slot_mask } => {
                self.partial_sync_follower(synced_slot_mask)?
            }
            Transition::DropFollowerReplication { slot } => self.drop_follower_replication(slot)?,
        };

        next.is_valid().then_some(next)
    }

    fn prepare_put(&self, slot: Slot) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader
            || self.serving_node != NodeId::Leader
            || !self.cached_writes_only
            || self.shard_busted
            || self.unresolved_intents().contains(&slot)
        {
            return None;
        }

        let mut next = self.clone();
        next.prepared_puts.insert(slot);
        next.prepared_deletes.remove(&slot);
        Some(next)
    }

    fn prepare_delete(&self, slot: Slot) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader
            || self.serving_node != NodeId::Leader
            || !self.cached_writes_only
            || self.shard_busted
            || self.unresolved_intents().contains(&slot)
        {
            return None;
        }

        let mut next = self.clone();
        next.prepared_deletes.insert(slot);
        next.prepared_puts.remove(&slot);
        Some(next)
    }

    fn abort_prepared(&self, slot: Slot) -> Option<Self> {
        if !self.unresolved_intents().contains(&slot) || !self.abort_still_matches_source(slot) {
            return None;
        }

        let mut next = self.clone();
        next.prepared_puts.remove(&slot);
        next.prepared_deletes.remove(&slot);
        Some(next)
    }

    fn abort_still_matches_source(&self, slot: Slot) -> bool {
        if self.prepared_puts.contains(&slot) {
            !self.db_present.contains(&slot)
        } else if self.prepared_deletes.contains(&slot) {
            self.db_present.contains(&slot)
        } else {
            false
        }
    }

    fn leader_commit_put(&self, slot: Slot) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader
            || self.serving_node != NodeId::Leader
            || !self.prepared_puts.contains(&slot)
        {
            return None;
        }

        let mut next = self.clone();
        next.db_present.insert(slot);
        next.leader.items = Self::visible_put_item_cache(&next.leader.items, slot);
        Some(next)
    }

    fn follower_acknowledge_put(&self, slot: Slot) -> Option<Self> {
        if !self.prepared_puts.contains(&slot) || !self.db_present.contains(&slot) {
            return None;
        }

        let mut next = self.clone();
        next.prepared_puts.remove(&slot);
        next.follower.items = Self::visible_put_item_cache(&next.follower.items, slot);
        Some(next)
    }

    fn leader_commit_delete(&self, slot: Slot) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader
            || self.serving_node != NodeId::Leader
            || !self.prepared_deletes.contains(&slot)
        {
            return None;
        }

        let mut next = self.clone();
        next.db_present.remove(&slot);
        next.gsi_present.remove(&slot);
        next.gsi_alt_present.remove(&slot);
        next.leader.items = Self::visible_delete_item_cache(&next.leader.items, slot);
        next.leader.primary_gsi = Self::visible_delete_index_cache(&next.leader.primary_gsi, slot);
        next.leader.alternate_gsi =
            Self::visible_delete_index_cache(&next.leader.alternate_gsi, slot);
        Some(next)
    }

    fn follower_acknowledge_delete(&self, slot: Slot) -> Option<Self> {
        if !self.prepared_deletes.contains(&slot) || self.db_present.contains(&slot) {
            return None;
        }

        let mut next = self.clone();
        next.prepared_deletes.remove(&slot);
        next.follower.items = Self::visible_delete_item_cache(&next.follower.items, slot);
        next.follower.primary_gsi =
            Self::visible_delete_index_cache(&next.follower.primary_gsi, slot);
        next.follower.alternate_gsi =
            Self::visible_delete_index_cache(&next.follower.alternate_gsi, slot);
        Some(next)
    }

    fn query_fill_base(&self, range: TransitionRange) -> Option<Self> {
        if !range.is_valid() || self.actual_leader_node != self.serving_node || !self.item_authority
        {
            return None;
        }

        let mut next = self.clone();
        let filled_slots = Self::interval_slots(range);
        let db_present = next.db_present.clone();
        let replica = next.serving_replica_mut();
        replica.items = Self::query_fill_item_cache(&replica.items, &db_present, &filled_slots);
        replica.items.current_schema_covered_slots =
            CacheState::union(&replica.items.current_schema_covered_slots, &filled_slots);
        Some(next)
    }

    fn query_fill_gsi(&self, query_space: GsiQuerySpace, range: TransitionRange) -> Option<Self> {
        if !range.is_valid() || self.actual_leader_node != self.serving_node || !self.item_authority
        {
            return None;
        }

        let mut next = self.clone();
        let filled_slots = Self::interval_slots(range);
        let source_keys = match query_space {
            GsiQuerySpace::Primary => next.gsi_present.clone(),
            GsiQuerySpace::Alternate => next.gsi_alt_present.clone(),
        };
        let replica = next.serving_replica_mut();
        let cache = match query_space {
            GsiQuerySpace::Primary => &mut replica.primary_gsi,
            GsiQuerySpace::Alternate => &mut replica.alternate_gsi,
        };
        *cache = Self::query_fill_index_cache(cache, &source_keys, &filled_slots);
        cache.current_schema_covered_slots =
            CacheState::union(&cache.current_schema_covered_slots, &filled_slots);
        Some(next)
    }

    fn sync_follower_from_leader(&self) -> Self {
        let mut next = self.clone();
        next.follower = next.leader.clone();
        next.follower_primary_gsi_order_version = next.leader_primary_gsi_order_version;
        next.follower_alternate_gsi_order_version = next.leader_alternate_gsi_order_version;
        next
    }

    fn advance_base_schema_version(&self) -> Self {
        let mut next = self.clone();
        next.actual_base_schema_version =
            Self::bump_schema_version(next.actual_base_schema_version);
        next.leader.items.current_schema_covered_slots.clear();
        next.follower.items.current_schema_covered_slots.clear();
        next
    }

    fn advance_gsi_schema_version(&self) -> Self {
        let mut next = self.clone();
        next.actual_gsi_schema_version = Self::bump_schema_version(next.actual_gsi_schema_version);
        next.leader.primary_gsi.current_schema_covered_slots.clear();
        next.leader
            .alternate_gsi
            .current_schema_covered_slots
            .clear();
        next.follower
            .primary_gsi
            .current_schema_covered_slots
            .clear();
        next.follower
            .alternate_gsi
            .current_schema_covered_slots
            .clear();
        next
    }

    fn rewrite_gsi_sort_order(&self, query_space: GsiQuerySpace) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader || self.serving_node != NodeId::Leader {
            return None;
        }

        let mut next = self.clone();
        match query_space {
            GsiQuerySpace::Primary => {
                let next_version =
                    Self::bump_gsi_order_version(next.actual_primary_gsi_order_version);
                next.actual_primary_gsi_order_version = next_version;
                next.leader_primary_gsi_order_version = next_version;
            }
            GsiQuerySpace::Alternate => {
                let next_version =
                    Self::bump_gsi_order_version(next.actual_alternate_gsi_order_version);
                next.actual_alternate_gsi_order_version = next_version;
                next.leader_alternate_gsi_order_version = next_version;
            }
        }
        Some(next)
    }

    fn add_gsi_membership(&self, slot: Slot) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader
            || self.serving_node != NodeId::Leader
            || !self.db_present.contains(&slot)
        {
            return None;
        }

        let mut next = self.clone();
        next.gsi_present.insert(slot);
        next.leader.primary_gsi = Self::visible_insert_index_cache(&next.leader.primary_gsi, slot);
        Some(next)
    }

    fn remove_gsi_membership(&self, slot: Slot) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader
            || self.serving_node != NodeId::Leader
            || !self.gsi_present.contains(&slot)
        {
            return None;
        }

        let mut next = self.clone();
        next.gsi_present.remove(&slot);
        next.leader.primary_gsi = Self::visible_delete_index_cache(&next.leader.primary_gsi, slot);
        Some(next)
    }

    fn move_gsi_membership(&self, slot: Slot, to_query_space: GsiQuerySpace) -> Option<Self> {
        if self.actual_leader_node != NodeId::Leader || self.serving_node != NodeId::Leader {
            return None;
        }

        let mut next = self.clone();
        match to_query_space {
            GsiQuerySpace::Primary => {
                if !next.gsi_alt_present.contains(&slot) || next.gsi_present.contains(&slot) {
                    return None;
                }
                next.gsi_alt_present.remove(&slot);
                next.gsi_present.insert(slot);
                next.leader.alternate_gsi =
                    Self::visible_delete_index_cache(&next.leader.alternate_gsi, slot);
                next.leader.primary_gsi =
                    Self::visible_insert_index_cache(&next.leader.primary_gsi, slot);
            }
            GsiQuerySpace::Alternate => {
                if !next.gsi_present.contains(&slot) || next.gsi_alt_present.contains(&slot) {
                    return None;
                }
                next.gsi_present.remove(&slot);
                next.gsi_alt_present.insert(slot);
                next.leader.primary_gsi =
                    Self::visible_delete_index_cache(&next.leader.primary_gsi, slot);
                next.leader.alternate_gsi =
                    Self::visible_insert_index_cache(&next.leader.alternate_gsi, slot);
            }
        }
        Some(next)
    }

    fn lose_leader(&self) -> Self {
        let mut next = self.clone();
        next.actual_leader_node = self.serving_node.other();
        next
    }

    fn regain_leader(&self) -> Self {
        let mut next = self.clone();
        next.actual_leader_node = self.serving_node;
        next
    }

    fn lose_epoch_authority(&self) -> Self {
        let mut next = self.clone();
        next.actual_epoch = self.stale_request_epoch();
        next.item_authority = false;
        next.query_authority = false;
        next.gsi_query_authority = false;
        next
    }

    fn gain_epoch_authority(&self) -> Option<Self> {
        if self.shard_busted {
            return None;
        }

        let mut next = self.clone();
        next.cache_epoch = next.actual_epoch;
        next.item_authority = true;
        next.query_authority = true;
        next.gsi_query_authority = true;
        Some(next)
    }

    fn promote_follower_catching_up(&self) -> Option<Self> {
        if self.serving_node != NodeId::Leader || self.shard_busted {
            return None;
        }

        let mut next = self.clone();
        next.serving_node = NodeId::Follower;
        next.actual_leader_node = NodeId::Follower;
        next.cache_epoch = next.actual_epoch;
        next.item_authority = true;
        next.query_authority = false;
        next.gsi_query_authority = false;
        Some(next)
    }

    fn recover_prepared_on_follower(&self) -> Option<Self> {
        if self.serving_node != NodeId::Follower || !self.item_authority {
            return None;
        }

        let committed_puts = CacheState::intersection(&self.prepared_puts, &self.db_present);
        let committed_deletes = CacheState::difference(&self.prepared_deletes, &self.db_present);

        let mut next = self.clone();
        next.follower.items =
            Self::apply_item_outcomes(&next.follower.items, &committed_puts, &committed_deletes);
        next.follower.primary_gsi =
            Self::apply_delete_outcomes_to_index(&next.follower.primary_gsi, &committed_deletes);
        next.follower.alternate_gsi =
            Self::apply_delete_outcomes_to_index(&next.follower.alternate_gsi, &committed_deletes);
        next.prepared_puts.clear();
        next.prepared_deletes.clear();
        Some(next)
    }

    fn finish_catch_up(&self) -> Option<Self> {
        if self.serving_node != NodeId::Follower || !self.item_authority || self.shard_busted {
            return None;
        }

        let mut next = self.clone();
        next.query_authority = true;
        next.gsi_query_authority = true;
        Some(next)
    }

    fn bust_shard(&self) -> Self {
        let mut next = self.clone();
        next.shard_busted = true;
        next.item_authority = false;
        next.query_authority = false;
        next.gsi_query_authority = false;
        next
    }

    fn assign_shard(&self) -> Option<Self> {
        if self.shard_assigned || self.shard_busted {
            return None;
        }

        let mut next = self.clone();
        next.shard_assigned = true;
        next.item_authority = true;
        next.query_authority = true;
        next.gsi_query_authority = true;
        Some(next)
    }

    fn drain_shard(&self) -> Option<Self> {
        if !self.shard_assigned || self.shard_busted {
            return None;
        }

        let mut next = self.clone();
        next.shard_assigned = false;
        next.item_authority = false;
        next.query_authority = false;
        next.gsi_query_authority = false;
        Some(next)
    }

    fn partial_sync_follower(&self, synced_slot_mask: u8) -> Option<Self> {
        if self.serving_node != NodeId::Leader || self.shard_busted {
            return None;
        }

        let synced_slots: BTreeSet<Slot> = CacheState::slots()
            .iter()
            .copied()
            .filter(|slot| synced_slot_mask & (1 << slot) != 0)
            .collect();

        let mut next = self.clone();
        // Only copy data for the specified synced slots from leader to follower
        for &slot in &synced_slots {
            if next.leader.items.payload_keys.contains(&slot) {
                next.follower.items.payload_keys.insert(slot);
                next.follower.items.negative_keys.remove(&slot);
            } else if next.leader.items.negative_keys.contains(&slot) {
                next.follower.items.negative_keys.insert(slot);
                next.follower.items.payload_keys.remove(&slot);
            }
            if next.leader.items.manifest_keys.contains(&slot) {
                next.follower.items.manifest_keys.insert(slot);
            }
            if next.leader.items.covered_slots.contains(&slot) {
                next.follower.items.covered_slots.insert(slot);
            }
        }
        Some(next)
    }

    fn drop_follower_replication(&self, slot: Slot) -> Option<Self> {
        if self.serving_node != NodeId::Leader || self.shard_busted {
            return None;
        }

        let mut next = self.clone();
        // Remove the slot from follower's caches (simulating lost replication)
        next.follower.items.payload_keys.remove(&slot);
        next.follower.items.negative_keys.remove(&slot);
        next.follower.items.manifest_keys.remove(&slot);
        next.follower.items.covered_slots.remove(&slot);
        next.follower
            .items
            .current_schema_covered_slots
            .remove(&slot);
        next.follower.primary_gsi.manifest_keys.remove(&slot);
        next.follower.primary_gsi.covered_slots.remove(&slot);
        next.follower
            .primary_gsi
            .current_schema_covered_slots
            .remove(&slot);
        next.follower.alternate_gsi.manifest_keys.remove(&slot);
        next.follower.alternate_gsi.covered_slots.remove(&slot);
        next.follower
            .alternate_gsi
            .current_schema_covered_slots
            .remove(&slot);
        Some(next)
    }

    fn interval_slots(range: TransitionRange) -> BTreeSet<Slot> {
        CacheState::slots()
            .iter()
            .copied()
            .filter(|slot| range.lower_bound <= *slot && *slot <= range.upper_bound)
            .collect()
    }

    fn query_fill_item_cache(
        cache: &ItemCacheState,
        db_present: &BTreeSet<Slot>,
        filled_slots: &BTreeSet<Slot>,
    ) -> ItemCacheState {
        let present = CacheState::intersection(db_present, filled_slots);
        let empty = CacheState::difference(filled_slots, db_present);
        let mut next = cache.clone();
        next.covered_slots = CacheState::union(&next.covered_slots, filled_slots);
        next.manifest_keys = CacheState::union(&next.manifest_keys, &present);
        next.payload_keys = CacheState::union(&next.payload_keys, &present);
        next.negative_keys = CacheState::union(&next.negative_keys, &empty);
        next
    }

    fn query_fill_index_cache(
        cache: &IndexCacheState,
        source_keys: &BTreeSet<Slot>,
        filled_slots: &BTreeSet<Slot>,
    ) -> IndexCacheState {
        let present = CacheState::intersection(source_keys, filled_slots);
        let mut next = cache.clone();
        next.covered_slots = CacheState::union(&next.covered_slots, filled_slots);
        next.manifest_keys = CacheState::union(&next.manifest_keys, &present);
        next
    }

    fn visible_put_item_cache(cache: &ItemCacheState, slot: Slot) -> ItemCacheState {
        let mut next = cache.clone();
        next.payload_keys.insert(slot);
        next.negative_keys.remove(&slot);
        next.manifest_keys.insert(slot);
        next
    }

    fn visible_delete_item_cache(cache: &ItemCacheState, slot: Slot) -> ItemCacheState {
        let mut next = cache.clone();
        next.payload_keys.remove(&slot);
        next.negative_keys.insert(slot);
        next.manifest_keys.remove(&slot);
        next
    }

    fn visible_insert_index_cache(cache: &IndexCacheState, slot: Slot) -> IndexCacheState {
        let mut next = cache.clone();
        next.manifest_keys.insert(slot);
        next
    }

    fn visible_delete_index_cache(cache: &IndexCacheState, slot: Slot) -> IndexCacheState {
        let mut next = cache.clone();
        next.manifest_keys.remove(&slot);
        next
    }

    fn apply_item_outcomes(
        cache: &ItemCacheState,
        committed_puts: &BTreeSet<Slot>,
        committed_deletes: &BTreeSet<Slot>,
    ) -> ItemCacheState {
        let mut next = cache.clone();
        next.payload_keys = CacheState::difference(
            &CacheState::union(&next.payload_keys, committed_puts),
            committed_deletes,
        );
        next.negative_keys = CacheState::union(
            &CacheState::difference(&next.negative_keys, committed_puts),
            committed_deletes,
        );
        next.manifest_keys = CacheState::difference(
            &CacheState::union(&next.manifest_keys, committed_puts),
            committed_deletes,
        );
        next
    }

    fn apply_delete_outcomes_to_index(
        cache: &IndexCacheState,
        committed_deletes: &BTreeSet<Slot>,
    ) -> IndexCacheState {
        let mut next = cache.clone();
        next.manifest_keys = CacheState::difference(&next.manifest_keys, committed_deletes);
        next
    }

    fn bump_schema_version(version: SchemaVersion) -> SchemaVersion {
        match version {
            0 => 1,
            1 => 2,
            _ => 0,
        }
    }

    fn bump_gsi_order_version(version: GsiOrderVersion) -> GsiOrderVersion {
        version.next()
    }

    fn serving_replica_mut(&mut self) -> &mut LocalReplicaState {
        match self.serving_node {
            NodeId::Leader => &mut self.leader,
            NodeId::Follower => &mut self.follower,
        }
    }
}
