use crate::{Slot, TxnOutcome, TxnShardId, TxnState, TxnTransition};

fn apply_all(state: TxnState, transitions: &[TxnTransition]) -> TxnState {
    transitions.iter().fold(state, |current, transition| {
        current
            .try_apply(*transition)
            .expect("transaction scenario transition should be valid")
    })
}

fn assert_full_invariants(state: &TxnState) {
    assert!(state.is_valid());
    assert!(state.prepared_txn_gets_are_fenced());
    assert!(state.prepared_txn_batch_gets_exclude_locked_keys());
    assert!(state.prepared_txn_queries_are_fenced());
    assert!(state.outcome_only_after_replicated_prepare());
    assert!(state.committed_source_state_is_atomic());
    assert!(state.aborted_source_state_is_unchanged());
    assert!(state.applied_leader_commit_matches_source());
    assert!(state.applied_follower_commit_matches_source());
    assert!(state.committed_but_unapplied_serving_keys_stay_fenced());
    assert!(state.transactional_gets_always_bypass());
}

#[test]
fn commit_keeps_reads_fenced_until_each_leader_applies_outcome() {
    let committed = apply_all(
        TxnState::initial(),
        &[
            TxnTransition::Prepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::Prepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::CommitSource,
        ],
    );

    assert_full_invariants(&committed);
    assert_eq!(committed.txn_outcome, TxnOutcome::Commit);
    assert!(!committed.can_serve_eventual_get(0));
    assert!(!committed.can_serve_eventual_get(2));
    assert!(!committed.can_serve_shard_query(TxnShardId::Left));
    assert!(!committed.can_serve_shard_query(TxnShardId::Right));

    let left_applied = committed
        .try_apply(TxnTransition::ApplyLeaderOutcome {
            shard: TxnShardId::Left,
        })
        .expect("left outcome should apply");

    assert_full_invariants(&left_applied);
    assert!(left_applied.can_serve_eventual_get(0));
    assert!(!left_applied.can_serve_eventual_get(2));
    assert!(left_applied.can_serve_shard_query(TxnShardId::Left));
    assert!(!left_applied.can_serve_shard_query(TxnShardId::Right));

    let fully_applied = left_applied
        .try_apply(TxnTransition::ApplyLeaderOutcome {
            shard: TxnShardId::Right,
        })
        .expect("right outcome should apply");

    assert_full_invariants(&fully_applied);
    assert!(fully_applied.can_serve_eventual_get(0));
    assert!(fully_applied.can_serve_eventual_get(2));
    assert!(fully_applied.can_serve_shard_query(TxnShardId::Left));
    assert!(fully_applied.can_serve_shard_query(TxnShardId::Right));
}

#[test]
fn promoted_follower_stays_fenced_until_recovery() {
    let state = apply_all(
        TxnState::initial(),
        &[
            TxnTransition::Prepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::Prepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::CommitSource,
            TxnTransition::PromoteFollower {
                shard: TxnShardId::Left,
            },
        ],
    );

    assert_full_invariants(&state);
    assert!(!state.can_serve_eventual_get(0));
    assert!(!state.can_serve_shard_query(TxnShardId::Left));

    let recovered = state
        .try_apply(TxnTransition::RecoverPromotedFollower {
            shard: TxnShardId::Left,
        })
        .expect("promoted follower recovery should apply");

    assert_full_invariants(&recovered);
    assert!(recovered.can_serve_eventual_get(0));
    assert!(recovered.can_serve_shard_query(TxnShardId::Left));
}

#[test]
fn replicated_follower_outcome_makes_promotion_immediate() {
    let state = apply_all(
        TxnState::initial(),
        &[
            TxnTransition::Prepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::Prepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::CommitSource,
            TxnTransition::ApplyLeaderOutcome {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicateFollowerOutcome {
                shard: TxnShardId::Left,
            },
            TxnTransition::PromoteFollower {
                shard: TxnShardId::Left,
            },
        ],
    );

    assert_full_invariants(&state);
    assert!(state.can_serve_eventual_get(0));
    assert!(state.can_serve_shard_query(TxnShardId::Left));
}

#[test]
fn abort_releases_locks_without_changing_source_state() {
    let aborted = apply_all(
        TxnState::initial(),
        &[
            TxnTransition::Prepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::Prepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::AbortSource,
        ],
    );

    assert_full_invariants(&aborted);
    assert_eq!(aborted.txn_outcome, TxnOutcome::Abort);
    assert!(!aborted.can_serve_eventual_get(0));
    assert!(!aborted.can_serve_eventual_get(2));

    let leader_recovered = apply_all(
        aborted,
        &[
            TxnTransition::ApplyLeaderOutcome {
                shard: TxnShardId::Left,
            },
            TxnTransition::ApplyLeaderOutcome {
                shard: TxnShardId::Right,
            },
        ],
    );

    assert_full_invariants(&leader_recovered);
    assert_eq!(leader_recovered.db_present, TxnState::initial_db_present());
    assert!(leader_recovered.can_serve_eventual_get(0));
    assert!(leader_recovered.can_serve_eventual_get(2));
    assert!(leader_recovered.can_serve_shard_query(TxnShardId::Left));
    assert!(leader_recovered.can_serve_shard_query(TxnShardId::Right));
}

#[test]
fn replaying_committed_client_token_is_noop() {
    let committed = apply_all(
        TxnState::initial(),
        &[
            TxnTransition::Prepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::Prepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Right,
            },
            TxnTransition::CommitSource,
            TxnTransition::ApplyLeaderOutcome {
                shard: TxnShardId::Left,
            },
            TxnTransition::ApplyLeaderOutcome {
                shard: TxnShardId::Right,
            },
        ],
    );

    assert_full_invariants(&committed);

    let replayed = committed
        .try_apply(TxnTransition::ReplayClientToken)
        .expect("replay after outcome should be a no-op");

    assert_eq!(replayed, committed);
}

#[test]
fn batch_get_serves_only_unlocked_keys() {
    let state = apply_all(
        TxnState::initial(),
        &[
            TxnTransition::Prepare {
                shard: TxnShardId::Left,
            },
            TxnTransition::ReplicatePrepare {
                shard: TxnShardId::Left,
            },
        ],
    );
    let requested = [0_u8, 1, 2, 3].into_iter().collect();

    assert_full_invariants(&state);
    assert_eq!(
        state.batch_get_served_keys(&requested),
        [1_u8, 2, 3].into_iter().collect()
    );
}

#[test]
fn transactional_gets_always_bypass_cache() {
    let state = TxnState::initial();

    assert!(
        [0_u8, 1, 2, 3]
            .into_iter()
            .all(|slot: Slot| !state.can_serve_transactional_get(slot))
    );
}
