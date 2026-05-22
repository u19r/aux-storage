use std::collections::BTreeSet;

use proptest::prelude::*;

use crate::{TxnShardId, TxnState, TxnTransition};

// ------------------------------------------------------------------
// Transition catalog — all 15 variants in a flat array.
// Used for both proptest selection and state-aware filtering.
// ------------------------------------------------------------------

const ALL_TRANSITIONS: [TxnTransition; 15] = [
    TxnTransition::Prepare {
        shard: TxnShardId::Left,
    },
    TxnTransition::Prepare {
        shard: TxnShardId::Right,
    },
    TxnTransition::ReplicatePrepare {
        shard: TxnShardId::Left,
    },
    TxnTransition::ReplicatePrepare {
        shard: TxnShardId::Right,
    },
    TxnTransition::CommitSource,
    TxnTransition::AbortSource,
    TxnTransition::ApplyLeaderOutcome {
        shard: TxnShardId::Left,
    },
    TxnTransition::ApplyLeaderOutcome {
        shard: TxnShardId::Right,
    },
    TxnTransition::ReplicateFollowerOutcome {
        shard: TxnShardId::Left,
    },
    TxnTransition::ReplicateFollowerOutcome {
        shard: TxnShardId::Right,
    },
    TxnTransition::PromoteFollower {
        shard: TxnShardId::Left,
    },
    TxnTransition::PromoteFollower {
        shard: TxnShardId::Right,
    },
    TxnTransition::RecoverPromotedFollower {
        shard: TxnShardId::Left,
    },
    TxnTransition::RecoverPromotedFollower {
        shard: TxnShardId::Right,
    },
    TxnTransition::ReplayClientToken,
];

// ------------------------------------------------------------------
// Strategies
// ------------------------------------------------------------------

/// Uniform selection over all 15 transition variants.
fn txn_transition_strategy() -> impl Strategy<Value = TxnTransition> {
    prop::sample::select(&ALL_TRANSITIONS[..])
}

/// Edge-biased strategy: heavily weighted toward the four transitions that
/// drive the commit/abort/promote/recover paths, with uniform fallback.
fn edge_biased_txn_strategy() -> impl Strategy<Value = TxnTransition> {
    prop_oneof![
        5 => Just(TxnTransition::CommitSource),
        5 => Just(TxnTransition::AbortSource),
        4 => Just(TxnTransition::PromoteFollower { shard: TxnShardId::Left }),
        4 => Just(TxnTransition::PromoteFollower { shard: TxnShardId::Right }),
        4 => Just(TxnTransition::RecoverPromotedFollower { shard: TxnShardId::Left }),
        4 => Just(TxnTransition::RecoverPromotedFollower { shard: TxnShardId::Right }),
        4 => Just(TxnTransition::ReplayClientToken),
        // fill-in transitions to warm state before the interesting steps
        3 => Just(TxnTransition::Prepare { shard: TxnShardId::Left }),
        3 => Just(TxnTransition::Prepare { shard: TxnShardId::Right }),
        3 => Just(TxnTransition::ReplicatePrepare { shard: TxnShardId::Left }),
        3 => Just(TxnTransition::ReplicatePrepare { shard: TxnShardId::Right }),
        2 => Just(TxnTransition::ApplyLeaderOutcome { shard: TxnShardId::Left }),
        2 => Just(TxnTransition::ApplyLeaderOutcome { shard: TxnShardId::Right }),
        2 => Just(TxnTransition::ReplicateFollowerOutcome { shard: TxnShardId::Left }),
        2 => Just(TxnTransition::ReplicateFollowerOutcome { shard: TxnShardId::Right }),
    ]
}

/// Returns only the transitions that are valid to apply from `state`.
fn valid_transitions(state: &TxnState) -> Vec<TxnTransition> {
    ALL_TRANSITIONS
        .iter()
        .copied()
        .filter(|t| state.try_apply(*t).is_some())
        .collect()
}

// ------------------------------------------------------------------
// Invariant helpers
// ------------------------------------------------------------------

fn assert_txn_invariants(state: &TxnState) {
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

fn assert_txn_read_serving_invariants(state: &TxnState) {
    // batch_get_served_keys must always be a subset of resolvable keys.
    let all_keys = BTreeSet::from([0_u8, 1, 2, 3]);
    let served = state.batch_get_served_keys(&all_keys);
    let unresolved = state.serving_unresolved_keys();
    assert!(
        served.is_disjoint(&unresolved),
        "batch-get served keys must not overlap with unresolved keys"
    );

    // Transactional gets must never be served.
    for slot in 0_u8..=3 {
        assert!(
            !state.can_serve_transactional_get(slot),
            "transactional gets must always bypass"
        );
    }

    // If a shard query is being served, unresolved keys in that shard must be
    // empty.
    for shard in TxnShardId::ALL {
        if state.can_serve_shard_query(shard) {
            assert!(
                state.serving_shard(shard).unresolved_keys().is_empty(),
                "shard query served with unresolved key present"
            );
        }
    }
}

// ------------------------------------------------------------------
// proptest! blocks
// ------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        .. ProptestConfig::default()
    })]

    #[test]
    fn transition_sequences_preserve_transaction_invariants(
        operations in prop::collection::vec(txn_transition_strategy(), 1..80),
    ) {
        let mut state = TxnState::initial();
        for operation in operations {
            if let Some(next) = state.try_apply(operation) {
                state = next;
            }

            assert_txn_invariants(&state);
            assert_txn_read_serving_invariants(&state);
        }
    }

    #[test]
    fn edge_biased_sequences_preserve_transaction_invariants(
        operations in prop::collection::vec(edge_biased_txn_strategy(), 1..80),
    ) {
        let mut state = TxnState::initial();
        for operation in operations {
            if let Some(next) = state.try_apply(operation) {
                state = next;
            }

            assert_txn_invariants(&state);
            assert_txn_read_serving_invariants(&state);
        }
    }

    #[test]
    fn state_aware_txn_sequences_preserve_invariants(
        selectors in prop::collection::vec(any::<u8>(), 1..120),
    ) {
        let mut state = TxnState::initial();
        for selector in selectors {
            let valid = valid_transitions(&state);
            if valid.is_empty() {
                // Terminal state — reset for the next phase of the sequence.
                state = TxnState::initial();
                continue;
            }

            let operation = valid[usize::from(selector) % valid.len()];
            state = state
                .try_apply(operation)
                .expect("state-aware txn transition must succeed");

            assert_txn_invariants(&state);
            assert_txn_read_serving_invariants(&state);
        }
    }
}
