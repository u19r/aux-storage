use std::collections::{BTreeSet, HashSet, VecDeque};

use crate::{
    BatchGetPlan, CacheReadOutcome, CacheRouteOutcome, CacheState, GsiQuerySpace, PartitionId,
    QueryDirection, QueryRequest, QueryTarget, Transition, TransitionRange, TxnShardId, TxnState,
    TxnTransition,
};

fn query_request(
    range: TransitionRange,
    start_exclusive: i8,
    limit: usize,
    byte_budget: usize,
    only_even: bool,
    direction: QueryDirection,
    target: QueryTarget,
) -> QueryRequest {
    QueryRequest {
        lower_bound: range.lower_bound,
        upper_bound: range.upper_bound,
        start_exclusive,
        limit,
        byte_budget,
        only_even,
        direction,
        target,
        partition: PartitionId::infer(range.lower_bound),
    }
}

fn all_query_targets() -> [QueryTarget; 3] {
    [
        QueryTarget::Base,
        QueryTarget::Gsi(GsiQuerySpace::Primary),
        QueryTarget::Gsi(GsiQuerySpace::Alternate),
    ]
}

fn model_transition_space() -> Vec<Transition> {
    let mut transitions = vec![
        Transition::SyncFollowerFromLeader,
        Transition::AdvanceBaseSchemaVersion,
        Transition::AdvanceGsiSchemaVersion,
        Transition::RewriteGsiSortOrder {
            query_space: GsiQuerySpace::Primary,
        },
        Transition::RewriteGsiSortOrder {
            query_space: GsiQuerySpace::Alternate,
        },
        Transition::LoseLeader,
        Transition::RegainLeader,
        Transition::LoseEpochAuthority,
        Transition::GainEpochAuthority,
        Transition::PromoteFollowerCatchingUp,
        Transition::RecoverPreparedOnFollower,
        Transition::FinishCatchUp,
        Transition::BustShard,
        Transition::AssignShard,
        Transition::DrainShard,
    ];

    for slot in CacheState::slots().iter().copied() {
        transitions.push(Transition::PreparePut { slot });
        transitions.push(Transition::PrepareDelete { slot });
        transitions.push(Transition::AbortPrepared { slot });
        transitions.push(Transition::LeaderCommitPut { slot });
        transitions.push(Transition::FollowerAcknowledgePut { slot });
        transitions.push(Transition::LeaderCommitDelete { slot });
        transitions.push(Transition::FollowerAcknowledgeDelete { slot });
        transitions.push(Transition::AddGsiMembership { slot });
        transitions.push(Transition::RemoveGsiMembership { slot });
        transitions.push(Transition::MoveGsiMembership {
            slot,
            to_query_space: GsiQuerySpace::Primary,
        });
        transitions.push(Transition::MoveGsiMembership {
            slot,
            to_query_space: GsiQuerySpace::Alternate,
        });
        transitions.push(Transition::DropFollowerReplication { slot });
    }

    // Partial sync follower with all possible slot masks
    for mask in 0_u8..=15 {
        transitions.push(Transition::PartialSyncFollower {
            synced_slot_mask: mask,
        });
    }

    for lower_bound in CacheState::slots().iter().copied() {
        for upper_bound in CacheState::slots()
            .iter()
            .copied()
            .filter(|slot| *slot >= lower_bound)
        {
            let range = TransitionRange::new(lower_bound, upper_bound);
            transitions.push(Transition::QueryFillBase { range });
            transitions.push(Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range,
            });
            transitions.push(Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Alternate,
                range,
            });
        }
    }

    transitions
}

fn txn_transition_space() -> [TxnTransition; 15] {
    [
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
    ]
}

fn query_budget_space() -> Vec<usize> {
    let mut budgets = BTreeSet::from([
        1_usize, 96, 128, 160, 224, 256, 320, 384, 640, 768, 1024, 1280,
    ]);
    for target in [QueryTarget::Base, QueryTarget::Gsi(GsiQuerySpace::Primary)] {
        for ordering in [[0_u8, 1, 2, 3], [3_u8, 2, 1, 0]] {
            let mut sum = 0usize;
            for slot in ordering {
                let query = query_request(
                    TransitionRange::new(0, 1),
                    -1,
                    4,
                    usize::MAX,
                    false,
                    QueryDirection::Forward,
                    target,
                );
                sum += CacheState::raw_page_bytes(&[slot], &query);
                budgets.insert(sum);
            }
        }
    }
    budgets.into_iter().collect()
}

fn all_valid_queries() -> Vec<QueryRequest> {
    let canonical_state = CacheState::authoritative_leader_base_state();
    let mut seen = HashSet::new();
    let mut queries = Vec::new();
    for lower_bound in CacheState::slots().iter().copied() {
        for upper_bound in CacheState::slots()
            .iter()
            .copied()
            .filter(|slot| *slot >= lower_bound)
        {
            for start_exclusive in -1_i8..=4 {
                for limit in 1_usize..=4 {
                    for byte_budget in query_budget_space() {
                        for only_even in [false, true] {
                            for direction in [QueryDirection::Forward, QueryDirection::Reverse] {
                                for target in all_query_targets() {
                                    let query = query_request(
                                        TransitionRange::new(lower_bound, upper_bound),
                                        start_exclusive,
                                        limit,
                                        byte_budget,
                                        only_even,
                                        direction,
                                        target,
                                    );
                                    if query.is_valid() {
                                        let fingerprint = format!(
                                            "{:?}|{}|{}|{}|{:?}",
                                            canonical_state.candidate_slots(&query),
                                            query.limit,
                                            query.byte_budget,
                                            query.only_even,
                                            query.target
                                        );
                                        if seen.insert(fingerprint) {
                                            queries.push(query);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    queries
}

fn representative_queries() -> Vec<QueryRequest> {
    let mut queries = Vec::new();
    for target in all_query_targets() {
        for direction in [QueryDirection::Forward, QueryDirection::Reverse] {
            let full_start = match direction {
                QueryDirection::Forward => -1,
                QueryDirection::Reverse => 4,
            };
            let right_partial_start = match direction {
                QueryDirection::Forward => 2,
                QueryDirection::Reverse => 3,
            };

            for only_even in [false, true] {
                queries.push(query_request(
                    TransitionRange::new(0, 1),
                    full_start,
                    1,
                    1,
                    only_even,
                    direction,
                    target,
                ));
                queries.push(query_request(
                    TransitionRange::new(0, 1),
                    full_start,
                    2,
                    384,
                    only_even,
                    direction,
                    target,
                ));
                queries.push(query_request(
                    TransitionRange::new(0, 1),
                    full_start,
                    4,
                    1280,
                    only_even,
                    direction,
                    target,
                ));
                queries.push(query_request(
                    TransitionRange::new(2, 3),
                    right_partial_start,
                    2,
                    1024,
                    only_even,
                    direction,
                    target,
                ));
            }
        }
    }

    queries
        .into_iter()
        .filter(|query| query.is_valid())
        .collect()
}

fn assert_read_invariants_for_state(state: &CacheState, queries: &[QueryRequest]) {
    let requested = BTreeSet::from([0_u8, 1, 2, 3]);
    let fresh_epoch = state.fresh_request_epoch();
    let stale_epoch = state.stale_request_epoch();
    let BatchGetPlan { served_keys, .. } = state.batch_get_plan(false, &requested);
    let expected_served: BTreeSet<_> = requested
        .iter()
        .copied()
        .filter(|slot| state.can_serve_eventual_get(*slot))
        .collect();

    assert!(state.is_valid());
    assert_eq!(served_keys, expected_served);
    let strong_batch = state.batch_get_decision(true, &requested, fresh_epoch);
    assert_eq!(
        strong_batch.served_keys,
        requested
            .iter()
            .copied()
            .filter(|slot| state.can_serve_strong_get(*slot))
            .collect()
    );

    for slot in CacheState::slots().iter().copied() {
        if state.can_serve_eventual_get(slot) {
            assert!(state.eventual_get_matches_source(slot));
        }
        assert_eq!(
            state.strong_get_decision(slot, fresh_epoch),
            if state.can_serve_strong_get(slot) {
                CacheReadOutcome::ServeCache
            } else {
                CacheReadOutcome::FallbackDb
            }
        );
        assert_eq!(
            state.eventual_get_decision(slot, stale_epoch),
            CacheReadOutcome::FallbackDb
        );
    }

    for query in queries {
        let plan = state.cache_plan(query);
        let decision = state.query_decision(query, false, fresh_epoch);

        for slot in &plan.cache_evaluated_keys {
            assert!(
                state
                    .serving_current_schema_covered_slots(query)
                    .contains(slot)
            );
        }

        if plan.serve_whole_page {
            if query.target.is_gsi() {
                assert!(state.manifest_order_current(query));
            }
            assert_eq!(
                state.cache_returned_page(query),
                state.source_returned_page(query)
            );
            assert_eq!(decision.outcome, CacheReadOutcome::ServeCache);
        } else if plan.cache_evaluated_keys.is_empty() {
            assert_eq!(decision.outcome, CacheReadOutcome::FallbackDb);
        } else {
            assert_eq!(decision.outcome, CacheReadOutcome::Mixed);
        }

        assert_eq!(
            state.query_decision(query, false, stale_epoch).outcome,
            CacheReadOutcome::FallbackDb
        );

        if query.target.is_gsi() && state.request_route(fresh_epoch) == CacheRouteOutcome::Ok {
            assert_eq!(
                state.query_decision(query, true, fresh_epoch).outcome,
                CacheReadOutcome::InvalidGsiStrong
            );
        } else {
            assert_eq!(
                state.query_decision(query, true, fresh_epoch).outcome,
                CacheReadOutcome::FallbackDb
            );
        }

        if state.request_route(fresh_epoch) != CacheRouteOutcome::Ok {
            assert!(!decision.serve_whole_page);
        }
    }
}

fn assert_txn_invariants_for_state(state: &TxnState) {
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

fn explore_read_model(max_depth: u8, queries: &[QueryRequest]) -> usize {
    let transitions = model_transition_space();
    let mut seen = HashSet::from([format!(
        "{:?}",
        CacheState::authoritative_leader_base_state()
    )]);
    let mut frontier = VecDeque::from([(CacheState::authoritative_leader_base_state(), 0_u8)]);
    let mut explored = 0usize;

    while let Some((state, depth)) = frontier.pop_front() {
        explored += 1;
        assert_read_invariants_for_state(&state, queries);

        if depth == max_depth {
            continue;
        }

        for transition in &transitions {
            if let Some(next) = state.try_apply(*transition) {
                let fingerprint = format!("{next:?}");
                if seen.insert(fingerprint) {
                    frontier.push_back((next, depth + 1));
                }
            }
        }
    }

    explored
}

#[test]
fn bounded_read_model_state_exploration_preserves_invariants() {
    let explored = explore_read_model(3, &representative_queries());
    assert!(explored > 1);
}

#[test]
#[ignore = "long-running exhaustive proof sweep over read-model states and query shapes"]
fn deeper_read_model_state_exploration_preserves_invariants() {
    let explored = explore_read_model(6, &all_valid_queries());
    assert!(explored > 1);
}

#[test]
fn transaction_state_fixpoint_exploration_preserves_invariants() {
    let transitions = txn_transition_space();
    let initial = TxnState::initial();
    let mut seen = HashSet::from([format!("{initial:?}")]);
    let mut frontier = VecDeque::from([initial]);
    let mut explored = 0usize;

    while let Some(state) = frontier.pop_front() {
        explored += 1;
        assert_txn_invariants_for_state(&state);

        for transition in transitions {
            if let Some(next) = state.try_apply(transition) {
                let fingerprint = format!("{next:?}");
                if seen.insert(fingerprint) {
                    frontier.push_back(next);
                }
            }
        }
    }

    assert!(explored > 1);
}
