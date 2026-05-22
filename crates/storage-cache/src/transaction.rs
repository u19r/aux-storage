use std::collections::BTreeSet;

use crate::Slot;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TxnShardId {
    Left,
    Right,
}

impl TxnShardId {
    pub const ALL: [Self; 2] = [Self::Left, Self::Right];

    #[must_use]
    pub const fn for_slot(slot: Slot) -> Self {
        if slot < 2 { Self::Left } else { Self::Right }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TxnOutcome {
    #[default]
    None,
    Commit,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxnShardState {
    pub payload_keys: BTreeSet<Slot>,
    pub negative_keys: BTreeSet<Slot>,
    pub known_manifest_keys: BTreeSet<Slot>,
    pub covered_slots: BTreeSet<Slot>,
    pub prepared_puts: BTreeSet<Slot>,
    pub prepared_deletes: BTreeSet<Slot>,
    pub current_leader: bool,
    pub authoritative_epoch: bool,
    pub cached_writes_only: bool,
    pub item_authority: bool,
    pub query_authority: bool,
    pub shard_assigned: bool,
}

impl TxnShardState {
    #[must_use]
    pub fn make_shard_copy(
        db_present: &BTreeSet<Slot>,
        shard: TxnShardId,
        current_leader: bool,
    ) -> Self {
        let present = TxnState::shard_present_keys(db_present, shard);
        let absent = TxnState::shard_absent_keys(db_present, shard);
        Self {
            payload_keys: present.clone(),
            negative_keys: absent,
            known_manifest_keys: present,
            covered_slots: TxnState::slots_for_shard(shard),
            prepared_puts: BTreeSet::new(),
            prepared_deletes: BTreeSet::new(),
            current_leader,
            authoritative_epoch: true,
            cached_writes_only: true,
            item_authority: true,
            query_authority: true,
            shard_assigned: true,
        }
    }

    #[must_use]
    pub fn unresolved_keys(&self) -> BTreeSet<Slot> {
        set_union(&self.prepared_puts, &self.prepared_deletes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxnState {
    pub db_present: BTreeSet<Slot>,
    pub left_leader: TxnShardState,
    pub right_leader: TxnShardState,
    pub left_follower: TxnShardState,
    pub right_follower: TxnShardState,
    pub prepared_shards: BTreeSet<TxnShardId>,
    pub follower_prepared_shards: BTreeSet<TxnShardId>,
    pub applied_shards: BTreeSet<TxnShardId>,
    pub follower_applied_shards: BTreeSet<TxnShardId>,
    pub serving_from_follower: BTreeSet<TxnShardId>,
    pub txn_outcome: TxnOutcome,
}

impl Default for TxnState {
    fn default() -> Self {
        Self::initial()
    }
}

impl TxnState {
    #[must_use]
    pub fn initial() -> Self {
        let db_present = Self::initial_db_present();
        Self {
            db_present: db_present.clone(),
            left_leader: TxnShardState::make_shard_copy(&db_present, TxnShardId::Left, true),
            right_leader: TxnShardState::make_shard_copy(&db_present, TxnShardId::Right, true),
            left_follower: TxnShardState::make_shard_copy(&db_present, TxnShardId::Left, false),
            right_follower: TxnShardState::make_shard_copy(&db_present, TxnShardId::Right, false),
            prepared_shards: BTreeSet::new(),
            follower_prepared_shards: BTreeSet::new(),
            applied_shards: BTreeSet::new(),
            follower_applied_shards: BTreeSet::new(),
            serving_from_follower: BTreeSet::new(),
            txn_outcome: TxnOutcome::None,
        }
    }

    #[must_use]
    pub fn initial_db_present() -> BTreeSet<Slot> {
        BTreeSet::from([2])
    }

    #[must_use]
    pub fn committed_db_present() -> BTreeSet<Slot> {
        BTreeSet::from([0])
    }

    #[must_use]
    pub fn slots_for_shard(shard: TxnShardId) -> BTreeSet<Slot> {
        match shard {
            TxnShardId::Left => BTreeSet::from([0, 1]),
            TxnShardId::Right => BTreeSet::from([2, 3]),
        }
    }

    #[must_use]
    pub fn txn_puts_for_shard(shard: TxnShardId) -> BTreeSet<Slot> {
        match shard {
            TxnShardId::Left => BTreeSet::from([0]),
            TxnShardId::Right => BTreeSet::new(),
        }
    }

    #[must_use]
    pub fn txn_deletes_for_shard(shard: TxnShardId) -> BTreeSet<Slot> {
        match shard {
            TxnShardId::Left => BTreeSet::new(),
            TxnShardId::Right => BTreeSet::from([2]),
        }
    }

    #[must_use]
    pub fn shard_present_keys(db_present: &BTreeSet<Slot>, shard: TxnShardId) -> BTreeSet<Slot> {
        db_present
            .iter()
            .copied()
            .filter(|slot| TxnShardId::for_slot(*slot) == shard)
            .collect()
    }

    #[must_use]
    pub fn shard_absent_keys(db_present: &BTreeSet<Slot>, shard: TxnShardId) -> BTreeSet<Slot> {
        set_difference(
            &Self::slots_for_shard(shard),
            &Self::shard_present_keys(db_present, shard),
        )
    }

    #[must_use]
    pub fn leader_shard(&self, shard: TxnShardId) -> &TxnShardState {
        match shard {
            TxnShardId::Left => &self.left_leader,
            TxnShardId::Right => &self.right_leader,
        }
    }

    #[must_use]
    pub fn follower_shard(&self, shard: TxnShardId) -> &TxnShardState {
        match shard {
            TxnShardId::Left => &self.left_follower,
            TxnShardId::Right => &self.right_follower,
        }
    }

    #[must_use]
    pub fn serving_shard(&self, shard: TxnShardId) -> &TxnShardState {
        if self.serving_from_follower.contains(&shard) {
            self.follower_shard(shard)
        } else {
            self.leader_shard(shard)
        }
    }

    fn leader_shard_mut(&mut self, shard: TxnShardId) -> &mut TxnShardState {
        match shard {
            TxnShardId::Left => &mut self.left_leader,
            TxnShardId::Right => &mut self.right_leader,
        }
    }

    fn follower_shard_mut(&mut self, shard: TxnShardId) -> &mut TxnShardState {
        match shard {
            TxnShardId::Left => &mut self.left_follower,
            TxnShardId::Right => &mut self.right_follower,
        }
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        Self::valid_shard_state(&self.left_leader, &self.db_present, TxnShardId::Left)
            && Self::valid_shard_state(&self.right_leader, &self.db_present, TxnShardId::Right)
            && Self::valid_shard_state(&self.left_follower, &self.db_present, TxnShardId::Left)
            && Self::valid_shard_state(&self.right_follower, &self.db_present, TxnShardId::Right)
            && self.applied_shards.is_subset(&self.prepared_shards)
            && self
                .follower_applied_shards
                .is_subset(&self.follower_prepared_shards)
            && self
                .prepared_shards
                .iter()
                .all(|shard| TxnShardId::ALL.contains(shard))
            && self
                .follower_prepared_shards
                .iter()
                .all(|shard| TxnShardId::ALL.contains(shard))
            && self
                .serving_from_follower
                .iter()
                .all(|shard| TxnShardId::ALL.contains(shard))
            && TxnShardId::ALL.iter().copied().all(|shard| {
                if self.serving_from_follower.contains(&shard) {
                    self.follower_shard(shard).current_leader
                        && !self.leader_shard(shard).current_leader
                } else {
                    self.leader_shard(shard).current_leader
                }
            })
            && match self.txn_outcome {
                TxnOutcome::None | TxnOutcome::Abort => {
                    self.db_present == Self::initial_db_present()
                }
                TxnOutcome::Commit => self.db_present == Self::committed_db_present(),
            }
    }

    fn valid_shard_state(
        shard_state: &TxnShardState,
        db_present: &BTreeSet<Slot>,
        shard: TxnShardId,
    ) -> bool {
        let shard_slots = Self::slots_for_shard(shard);
        let unresolved = shard_state.unresolved_keys();
        let present = Self::shard_present_keys(db_present, shard);
        let absent = Self::shard_absent_keys(db_present, shard);
        let payload_keys = set_difference(&shard_state.payload_keys, &unresolved);
        let negative_keys = set_difference(&shard_state.negative_keys, &unresolved);
        let manifest_keys = set_difference(&shard_state.known_manifest_keys, &unresolved);
        let present_inside_coverage = set_difference(
            &set_intersection(&present, &shard_state.covered_slots),
            &unresolved,
        );

        shard_state.payload_keys.is_subset(&shard_slots)
            && shard_state.negative_keys.is_subset(&shard_slots)
            && shard_state.known_manifest_keys.is_subset(&shard_slots)
            && shard_state.covered_slots.is_subset(&shard_slots)
            && unresolved.is_subset(&shard_slots)
            && payload_keys.is_subset(&present)
            && negative_keys.is_subset(&absent)
            && manifest_keys.is_subset(&present)
            && present_inside_coverage.is_subset(&shard_state.known_manifest_keys)
            && shard_state
                .prepared_puts
                .is_disjoint(&shard_state.prepared_deletes)
            && (!shard_state.item_authority || shard_state.authoritative_epoch)
            && (!shard_state.query_authority || shard_state.item_authority)
            && (!shard_state.item_authority || shard_state.shard_assigned)
    }

    #[must_use]
    pub fn item_proof_active(shard_state: &TxnShardState) -> bool {
        shard_state.current_leader
            && shard_state.authoritative_epoch
            && shard_state.cached_writes_only
            && shard_state.item_authority
            && shard_state.shard_assigned
    }

    #[must_use]
    pub fn query_proof_active(shard_state: &TxnShardState) -> bool {
        Self::item_proof_active(shard_state) && shard_state.query_authority
    }

    #[must_use]
    pub fn can_serve_eventual_get(&self, slot: Slot) -> bool {
        let shard = TxnShardId::for_slot(slot);
        let local = self.serving_shard(shard);
        Self::item_proof_active(local)
            && !local.unresolved_keys().contains(&slot)
            && (local.payload_keys.contains(&slot) || local.negative_keys.contains(&slot))
    }

    #[must_use]
    pub fn batch_get_served_keys(&self, requested_keys: &BTreeSet<Slot>) -> BTreeSet<Slot> {
        requested_keys
            .iter()
            .copied()
            .filter(|slot| self.can_serve_eventual_get(*slot))
            .collect()
    }

    #[must_use]
    pub fn can_serve_shard_query(&self, shard: TxnShardId) -> bool {
        let local = self.serving_shard(shard);
        Self::query_proof_active(local)
            && local.covered_slots == Self::slots_for_shard(shard)
            && local.unresolved_keys().is_empty()
            && local.known_manifest_keys == Self::shard_present_keys(&self.db_present, shard)
    }

    #[must_use]
    pub const fn can_serve_transactional_get(&self, _slot: Slot) -> bool {
        false
    }

    #[must_use]
    pub fn serving_unresolved_keys(&self) -> BTreeSet<Slot> {
        TxnShardId::ALL
            .iter()
            .copied()
            .fold(BTreeSet::new(), |keys, shard| {
                set_union(&keys, &self.serving_shard(shard).unresolved_keys())
            })
    }

    #[must_use]
    pub fn outcome_only_after_replicated_prepare(&self) -> bool {
        self.txn_outcome == TxnOutcome::None
            || self.prepared_shards == self.follower_prepared_shards
    }

    #[must_use]
    pub fn committed_source_state_is_atomic(&self) -> bool {
        self.txn_outcome != TxnOutcome::Commit || self.db_present == Self::committed_db_present()
    }

    #[must_use]
    pub fn aborted_source_state_is_unchanged(&self) -> bool {
        self.txn_outcome != TxnOutcome::Abort || self.db_present == Self::initial_db_present()
    }

    #[must_use]
    pub fn prepared_txn_gets_are_fenced(&self) -> bool {
        [0, 1, 2, 3].into_iter().all(|slot| {
            !self
                .serving_shard(TxnShardId::for_slot(slot))
                .unresolved_keys()
                .contains(&slot)
                || !self.can_serve_eventual_get(slot)
        })
    }

    #[must_use]
    pub fn prepared_txn_batch_gets_exclude_locked_keys(&self) -> bool {
        let requested = BTreeSet::from([0, 1, 2, 3]);
        self.batch_get_served_keys(&requested)
            .is_disjoint(&self.serving_unresolved_keys())
    }

    #[must_use]
    pub fn prepared_txn_queries_are_fenced(&self) -> bool {
        TxnShardId::ALL.iter().copied().all(|shard| {
            self.serving_shard(shard).unresolved_keys().is_empty()
                || !self.can_serve_shard_query(shard)
        })
    }

    #[must_use]
    pub fn applied_leader_commit_matches_source(&self) -> bool {
        TxnShardId::ALL.iter().copied().all(|shard| {
            self.txn_outcome != TxnOutcome::Commit
                || !self.applied_shards.contains(&shard)
                || self.leader_shard(shard).known_manifest_keys
                    == Self::shard_present_keys(&self.db_present, shard)
                    && self.leader_shard(shard).payload_keys
                        == Self::shard_present_keys(&self.db_present, shard)
                    && self.leader_shard(shard).negative_keys
                        == Self::shard_absent_keys(&self.db_present, shard)
        })
    }

    #[must_use]
    pub fn applied_follower_commit_matches_source(&self) -> bool {
        TxnShardId::ALL.iter().copied().all(|shard| {
            self.txn_outcome != TxnOutcome::Commit
                || !self.follower_applied_shards.contains(&shard)
                || self.follower_shard(shard).known_manifest_keys
                    == Self::shard_present_keys(&self.db_present, shard)
                    && self.follower_shard(shard).payload_keys
                        == Self::shard_present_keys(&self.db_present, shard)
                    && self.follower_shard(shard).negative_keys
                        == Self::shard_absent_keys(&self.db_present, shard)
        })
    }

    #[must_use]
    pub fn committed_but_unapplied_serving_keys_stay_fenced(&self) -> bool {
        TxnShardId::ALL.iter().copied().all(|shard| {
            self.txn_outcome != TxnOutcome::Commit
                || (if self.serving_from_follower.contains(&shard) {
                    self.follower_applied_shards.contains(&shard)
                } else {
                    self.applied_shards.contains(&shard)
                })
                || Self::txn_puts_for_shard(shard)
                    .iter()
                    .copied()
                    .all(|slot| !self.can_serve_eventual_get(slot))
                    && Self::txn_deletes_for_shard(shard)
                        .iter()
                        .copied()
                        .all(|slot| !self.can_serve_eventual_get(slot))
                    && !self.can_serve_shard_query(shard)
        })
    }

    #[must_use]
    pub fn transactional_gets_always_bypass(&self) -> bool {
        [0, 1, 2, 3]
            .into_iter()
            .all(|slot| !self.can_serve_transactional_get(slot))
    }

    #[must_use]
    pub fn try_apply(&self, transition: TxnTransition) -> Option<Self> {
        let next = match transition {
            TxnTransition::Prepare { shard } => self.prepare(shard)?,
            TxnTransition::ReplicatePrepare { shard } => self.replicate_prepare(shard)?,
            TxnTransition::CommitSource => self.commit_source()?,
            TxnTransition::AbortSource => self.abort_source()?,
            TxnTransition::ApplyLeaderOutcome { shard } => self.apply_leader_outcome(shard)?,
            TxnTransition::ReplicateFollowerOutcome { shard } => {
                self.replicate_follower_outcome(shard)?
            }
            TxnTransition::PromoteFollower { shard } => self.promote_follower(shard)?,
            TxnTransition::RecoverPromotedFollower { shard } => {
                self.recover_promoted_follower(shard)?
            }
            TxnTransition::ReplayClientToken => self.replay_client_token()?,
        };

        next.is_valid().then_some(next)
    }

    fn prepare(&self, shard: TxnShardId) -> Option<Self> {
        if self.txn_outcome != TxnOutcome::None
            || self.prepared_shards.contains(&shard)
            || !self.leader_shard(shard).unresolved_keys().is_empty()
        {
            return None;
        }

        let mut next = self.clone();
        next.prepared_shards.insert(shard);
        let current = next.leader_shard(shard).clone();
        *next.leader_shard_mut(shard) = Self::prepare_shard_copy(&current, shard);
        Some(next)
    }

    fn replicate_prepare(&self, shard: TxnShardId) -> Option<Self> {
        if !self.prepared_shards.contains(&shard) || self.follower_prepared_shards.contains(&shard)
        {
            return None;
        }

        let mut next = self.clone();
        next.follower_prepared_shards.insert(shard);
        let current = next.follower_shard(shard).clone();
        *next.follower_shard_mut(shard) = Self::prepare_shard_copy(&current, shard);
        Some(next)
    }

    fn commit_source(&self) -> Option<Self> {
        if self.follower_prepared_shards != BTreeSet::from(TxnShardId::ALL)
            || self.txn_outcome != TxnOutcome::None
        {
            return None;
        }

        let mut next = self.clone();
        next.db_present = Self::committed_db_present();
        next.txn_outcome = TxnOutcome::Commit;
        Some(next)
    }

    fn abort_source(&self) -> Option<Self> {
        if self.prepared_shards.is_empty()
            || self.prepared_shards != self.follower_prepared_shards
            || self.txn_outcome != TxnOutcome::None
        {
            return None;
        }

        let mut next = self.clone();
        next.txn_outcome = TxnOutcome::Abort;
        Some(next)
    }

    fn apply_leader_outcome(&self, shard: TxnShardId) -> Option<Self> {
        if self.txn_outcome == TxnOutcome::None
            || !self.prepared_shards.contains(&shard)
            || self.applied_shards.contains(&shard)
        {
            return None;
        }

        let mut next = self.clone();
        next.applied_shards.insert(shard);
        let current = next.leader_shard(shard).clone();
        *next.leader_shard_mut(shard) =
            Self::apply_outcome_to_copy(&current, shard, self.txn_outcome);
        Some(next)
    }

    fn replicate_follower_outcome(&self, shard: TxnShardId) -> Option<Self> {
        if self.txn_outcome == TxnOutcome::None
            || !self.follower_prepared_shards.contains(&shard)
            || !self.applied_shards.contains(&shard)
            || self.follower_applied_shards.contains(&shard)
        {
            return None;
        }

        let mut next = self.clone();
        next.follower_applied_shards.insert(shard);
        let current = next.follower_shard(shard).clone();
        *next.follower_shard_mut(shard) =
            Self::apply_outcome_to_copy(&current, shard, self.txn_outcome);
        Some(next)
    }

    fn promote_follower(&self, shard: TxnShardId) -> Option<Self> {
        if self.serving_from_follower.contains(&shard)
            || !self.follower_prepared_shards.contains(&shard)
        {
            return None;
        }

        let mut next = self.clone();
        next.serving_from_follower.insert(shard);
        next.leader_shard_mut(shard).current_leader = false;
        next.follower_shard_mut(shard).current_leader = true;
        Some(next)
    }

    fn recover_promoted_follower(&self, shard: TxnShardId) -> Option<Self> {
        if !self.serving_from_follower.contains(&shard)
            || self.txn_outcome == TxnOutcome::None
            || !self.follower_prepared_shards.contains(&shard)
            || self.follower_applied_shards.contains(&shard)
        {
            return None;
        }

        let mut next = self.clone();
        next.follower_applied_shards.insert(shard);
        let current = next.follower_shard(shard).clone();
        *next.follower_shard_mut(shard) =
            Self::apply_outcome_to_copy(&current, shard, self.txn_outcome);
        Some(next)
    }

    fn replay_client_token(&self) -> Option<Self> {
        (self.txn_outcome != TxnOutcome::None).then_some(self.clone())
    }

    fn prepare_shard_copy(local: &TxnShardState, shard: TxnShardId) -> TxnShardState {
        let mut next = local.clone();
        next.prepared_puts = set_union(&next.prepared_puts, &Self::txn_puts_for_shard(shard));
        next.prepared_deletes =
            set_union(&next.prepared_deletes, &Self::txn_deletes_for_shard(shard));
        next
    }

    fn apply_outcome_to_copy(
        local: &TxnShardState,
        shard: TxnShardId,
        outcome: TxnOutcome,
    ) -> TxnShardState {
        let mut next = local.clone();
        if outcome == TxnOutcome::Commit {
            next.payload_keys = set_difference(
                &set_union(&next.payload_keys, &Self::txn_puts_for_shard(shard)),
                &Self::txn_deletes_for_shard(shard),
            );
            next.negative_keys = set_union(
                &set_difference(&next.negative_keys, &Self::txn_puts_for_shard(shard)),
                &Self::txn_deletes_for_shard(shard),
            );
            next.known_manifest_keys = set_difference(
                &set_union(&next.known_manifest_keys, &Self::txn_puts_for_shard(shard)),
                &Self::txn_deletes_for_shard(shard),
            );
        }
        next.prepared_puts.clear();
        next.prepared_deletes.clear();
        next
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxnTransition {
    Prepare { shard: TxnShardId },
    ReplicatePrepare { shard: TxnShardId },
    CommitSource,
    AbortSource,
    ApplyLeaderOutcome { shard: TxnShardId },
    ReplicateFollowerOutcome { shard: TxnShardId },
    PromoteFollower { shard: TxnShardId },
    RecoverPromotedFollower { shard: TxnShardId },
    ReplayClientToken,
}

fn set_union<T: Copy + Ord>(left: &BTreeSet<T>, right: &BTreeSet<T>) -> BTreeSet<T> {
    left.union(right).copied().collect()
}

fn set_intersection<T: Copy + Ord>(left: &BTreeSet<T>, right: &BTreeSet<T>) -> BTreeSet<T> {
    left.intersection(right).copied().collect()
}

fn set_difference<T: Copy + Ord>(left: &BTreeSet<T>, right: &BTreeSet<T>) -> BTreeSet<T> {
    left.difference(right).copied().collect()
}
