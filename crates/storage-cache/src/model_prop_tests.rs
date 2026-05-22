use std::collections::BTreeSet;

use proptest::prelude::*;

use crate::{
    BatchGetPlan, CacheReadOutcome, CacheState, GsiOrderVersion, GsiQuerySpace, PartitionId,
    QueryDirection, QueryRequest, QueryTarget, Transition, TransitionRange,
};

fn mask_to_set(mask: u8) -> BTreeSet<u8> {
    CacheState::slots()
        .iter()
        .copied()
        .filter(|slot| mask & (1_u8 << *slot) != 0)
        .collect()
}

fn edge_case_start_exclusive_strategy() -> impl Strategy<Value = i8> {
    prop_oneof![
        5 => Just(-1_i8),
        3 => Just(0_i8),
        3 => Just(1_i8),
        3 => Just(2_i8),
        3 => Just(3_i8),
        3 => Just(4_i8),
        1 => -1_i8..=4,
    ]
}

fn edge_case_limit_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![5 => Just(1_usize), 4 => Just(2), 4 => Just(4), 1 => 1_usize..=4,]
}

fn edge_case_byte_budget_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![
        5 => Just(1_usize),
        3 => Just(96),
        3 => Just(128),
        4 => Just(160),
        5 => Just(224),
        6 => Just(256),
        6 => Just(320),
        8 => Just(384),
        6 => Just(640),
        6 => Just(768),
        6 => Just(1024),
        6 => Just(1280),
        1 => 1_usize..=1280,
    ]
}

fn query_strategy() -> impl Strategy<Value = QueryRequest> {
    (
        0_u8..=3,
        0_u8..=3,
        edge_case_start_exclusive_strategy(),
        edge_case_limit_strategy(),
        edge_case_byte_budget_strategy(),
        any::<bool>(),
        prop_oneof![Just(QueryDirection::Forward), Just(QueryDirection::Reverse)],
        prop_oneof![
            Just(QueryTarget::Base),
            Just(QueryTarget::Gsi(GsiQuerySpace::Primary)),
            Just(QueryTarget::Gsi(GsiQuerySpace::Alternate))
        ],
    )
        .prop_map(
            |(
                lower_seed,
                upper_seed,
                start_exclusive,
                limit,
                byte_budget,
                only_even,
                direction,
                target,
            )| {
                let lower_bound = lower_seed.min(upper_seed);
                let upper_bound = lower_seed.max(upper_seed);
                QueryRequest {
                    lower_bound,
                    upper_bound,
                    start_exclusive,
                    limit,
                    byte_budget,
                    only_even,
                    direction,
                    target,
                    partition: PartitionId::infer(lower_bound),
                }
            },
        )
        .prop_filter("valid query", |query| query.is_valid())
}

fn transition_catalog() -> Vec<Transition> {
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

    for slot in 0_u8..=3 {
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

    for synced_slot_mask in 0_u8..=15 {
        transitions.push(Transition::PartialSyncFollower { synced_slot_mask });
    }

    for lower_seed in 0_u8..=3 {
        for upper_seed in 0_u8..=3 {
            let range =
                TransitionRange::new(lower_seed.min(upper_seed), lower_seed.max(upper_seed));
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

fn transition_strategy() -> impl Strategy<Value = Transition> {
    prop::sample::select(transition_catalog())
}

fn valid_transitions(state: &CacheState) -> Vec<Transition> {
    transition_catalog()
        .into_iter()
        .filter(|transition| state.try_apply(*transition).is_some())
        .collect()
}

fn node_strategy() -> impl Strategy<Value = crate::model::NodeId> {
    prop_oneof![
        Just(crate::model::NodeId::Leader),
        Just(crate::model::NodeId::Follower)
    ]
}

fn generated_state_strategy() -> impl Strategy<Value = CacheState> {
    (
        (
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
            0_u8..=15,
        ),
        (
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        ),
        (
            any::<bool>(),
            node_strategy(),
            0_u8..=1,
            0_u8..=1,
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        ),
    )
        .prop_map(
            |(
                (
                    db_mask,
                    primary_mask_seed,
                    alternate_mask_seed,
                    leader_covered_mask,
                    follower_covered_mask,
                    leader_payload_mask,
                    follower_payload_mask,
                    leader_negative_mask,
                    follower_negative_mask,
                    leader_gsi_covered_mask,
                    follower_gsi_covered_mask,
                    prepared_mask,
                ),
                (
                    actual_primary_order_seed,
                    actual_alternate_order_seed,
                    leader_primary_order_seed,
                    follower_primary_order_seed,
                    leader_alternate_order_seed,
                    follower_alternate_order_seed,
                ),
                (
                    serving_follower,
                    actual_leader_node,
                    actual_epoch,
                    cache_epoch_seed,
                    cached_writes_only,
                    item_authority_seed,
                    query_authority_seed,
                    gsi_query_authority_seed,
                    manifest_rebuilding,
                    continuity_broken,
                    shard_busted,
                    shard_assigned_seed,
                ),
            )| {
                let db_present = mask_to_set(db_mask);
                let primary_candidates =
                    CacheState::intersection(&mask_to_set(primary_mask_seed), &db_present);
                let alternate_candidates =
                    CacheState::intersection(&mask_to_set(alternate_mask_seed), &db_present);
                let gsi_present: BTreeSet<_> = primary_candidates.iter().copied().collect();
                let gsi_alt_present: BTreeSet<_> = alternate_candidates
                    .difference(&gsi_present)
                    .copied()
                    .collect();

                let prepared_puts = mask_to_set(prepared_mask & 0b0101);
                let prepared_deletes = mask_to_set((prepared_mask & 0b1010) >> 1);
                let stale_slots = CacheState::union(&prepared_puts, &prepared_deletes);

                let leader_base_covered = mask_to_set(leader_covered_mask);
                let follower_base_covered = mask_to_set(follower_covered_mask);
                let leader_primary_covered = mask_to_set(leader_gsi_covered_mask);
                let follower_primary_covered = mask_to_set(follower_gsi_covered_mask);
                let leader_alt_covered =
                    CacheState::difference(&db_present, &leader_primary_covered);
                let follower_alt_covered =
                    CacheState::difference(&db_present, &follower_primary_covered);

                let mut state = CacheState::authoritative_leader_base_state();
                state.db_present = db_present.clone();
                state.gsi_present = gsi_present.clone();
                state.gsi_alt_present = gsi_alt_present.clone();
                state.prepared_puts = prepared_puts;
                state.prepared_deletes = prepared_deletes;
                state.serving_node = if serving_follower {
                    crate::model::NodeId::Follower
                } else {
                    crate::model::NodeId::Leader
                };
                state.actual_leader_node = actual_leader_node;
                state.actual_epoch = actual_epoch;
                state.actual_primary_gsi_order_version = if actual_primary_order_seed {
                    GsiOrderVersion::V1
                } else {
                    GsiOrderVersion::V0
                };
                state.actual_alternate_gsi_order_version = if actual_alternate_order_seed {
                    GsiOrderVersion::V1
                } else {
                    GsiOrderVersion::V0
                };
                state.leader_primary_gsi_order_version = if leader_primary_order_seed {
                    GsiOrderVersion::V1
                } else {
                    GsiOrderVersion::V0
                };
                state.follower_primary_gsi_order_version = if follower_primary_order_seed {
                    GsiOrderVersion::V1
                } else {
                    GsiOrderVersion::V0
                };
                state.leader_alternate_gsi_order_version = if leader_alternate_order_seed {
                    GsiOrderVersion::V1
                } else {
                    GsiOrderVersion::V0
                };
                state.follower_alternate_gsi_order_version = if follower_alternate_order_seed {
                    GsiOrderVersion::V1
                } else {
                    GsiOrderVersion::V0
                };
                state.cached_writes_only = cached_writes_only;
                state.manifest_rebuilding = manifest_rebuilding;
                state.continuity_broken = continuity_broken;
                state.shard_busted = shard_busted;
                state.shard_assigned = shard_assigned_seed || !shard_busted;
                state.item_authority = item_authority_seed && !shard_busted && state.shard_assigned;
                state.query_authority =
                    state.item_authority && query_authority_seed && !shard_busted;
                state.gsi_query_authority =
                    state.item_authority && gsi_query_authority_seed && !shard_busted;
                state.cache_epoch = if state.item_authority {
                    state.actual_epoch
                } else {
                    cache_epoch_seed
                };

                state.leader.items.covered_slots = leader_base_covered.clone();
                state.leader.items.current_schema_covered_slots = CacheState::intersection(
                    &leader_base_covered,
                    &mask_to_set(leader_payload_mask),
                );
                state.leader.items.manifest_keys =
                    CacheState::intersection(&db_present, &leader_base_covered);
                state.leader.items.payload_keys = CacheState::intersection(
                    &CacheState::difference(&db_present, &stale_slots),
                    &mask_to_set(leader_payload_mask),
                );
                state.leader.items.negative_keys = CacheState::intersection(
                    &CacheState::difference(&CacheState::absent_slots(&db_present), &stale_slots),
                    &mask_to_set(leader_negative_mask),
                );

                state.follower.items.covered_slots = follower_base_covered.clone();
                state.follower.items.current_schema_covered_slots = CacheState::intersection(
                    &follower_base_covered,
                    &mask_to_set(follower_payload_mask),
                );
                state.follower.items.manifest_keys =
                    CacheState::intersection(&db_present, &follower_base_covered);
                state.follower.items.payload_keys = CacheState::intersection(
                    &CacheState::difference(&db_present, &stale_slots),
                    &mask_to_set(follower_payload_mask),
                );
                state.follower.items.negative_keys = CacheState::intersection(
                    &CacheState::difference(&CacheState::absent_slots(&db_present), &stale_slots),
                    &mask_to_set(follower_negative_mask),
                );

                state.leader.primary_gsi.covered_slots = leader_primary_covered.clone();
                state.leader.primary_gsi.current_schema_covered_slots = CacheState::intersection(
                    &leader_primary_covered,
                    &mask_to_set(leader_payload_mask),
                );
                state.leader.primary_gsi.manifest_keys =
                    CacheState::intersection(&gsi_present, &leader_primary_covered);
                state.leader.alternate_gsi.covered_slots = leader_alt_covered.clone();
                state.leader.alternate_gsi.current_schema_covered_slots = CacheState::intersection(
                    &leader_alt_covered,
                    &mask_to_set(leader_negative_mask),
                );
                state.leader.alternate_gsi.manifest_keys =
                    CacheState::intersection(&gsi_alt_present, &leader_alt_covered);

                state.follower.primary_gsi.covered_slots = follower_primary_covered.clone();
                state.follower.primary_gsi.current_schema_covered_slots = CacheState::intersection(
                    &follower_primary_covered,
                    &mask_to_set(follower_payload_mask),
                );
                state.follower.primary_gsi.manifest_keys =
                    CacheState::intersection(&gsi_present, &follower_primary_covered);
                state.follower.alternate_gsi.covered_slots = follower_alt_covered.clone();
                state.follower.alternate_gsi.current_schema_covered_slots =
                    CacheState::intersection(
                        &follower_alt_covered,
                        &mask_to_set(follower_negative_mask),
                    );
                state.follower.alternate_gsi.manifest_keys =
                    CacheState::intersection(&gsi_alt_present, &follower_alt_covered);

                state
            },
        )
        .prop_filter("valid generated state", CacheState::is_valid)
}

fn assert_query_invariants(state: &CacheState, query: &QueryRequest) {
    let plan = state.cache_plan(query);
    for slot in &plan.cache_evaluated_keys {
        assert!(
            state
                .serving_current_schema_covered_slots(query)
                .contains(slot)
        );
    }
    if plan.serve_whole_page {
        assert_eq!(
            state.cache_returned_page(query),
            state.source_returned_page(query)
        );
        assert!(!state.query_touches_unresolved_intent(query));
    }
    if query.target.is_gsi() && plan.serve_whole_page {
        assert!(state.manifest_order_current(query));
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn generated_valid_states_preserve_read_invariants(
        state in generated_state_strategy(),
        query in query_strategy(),
    ) {
        prop_assert!(state.is_valid());
        assert_query_invariants(&state, &query);

        let requested = BTreeSet::from([0_u8, 1, 2, 3]);
        let BatchGetPlan { served_keys, .. } = state.batch_get_plan(false, &requested);
        let expected_served: BTreeSet<_> = requested
            .iter()
            .copied()
            .filter(|slot| state.can_serve_eventual_get(*slot))
            .collect();
        prop_assert_eq!(served_keys.clone(), expected_served);
        for slot in &served_keys {
            prop_assert!(state.eventual_get_matches_source(*slot));
        }
    }

    #[test]
    fn transition_sequences_preserve_model_invariants(
        operations in prop::collection::vec(transition_strategy(), 1..80),
        query in query_strategy(),
    ) {
        let mut state = CacheState::authoritative_leader_base_state();
        for operation in operations {
            if let Some(next) = state.try_apply(operation) {
                state = next;
            }

            prop_assert!(state.is_valid());
            assert_query_invariants(&state, &query);

            if state.slot_has_unresolved_intent(0) {
                prop_assert!(!state.can_serve_eventual_get(0));
            }

            let stale_epoch = state.stale_request_epoch();
            let base_query = QueryRequest {
                lower_bound: 0,
                upper_bound: 1,
                start_exclusive: -1,
                limit: 2,
                byte_budget: 1024,
                only_even: false,
                direction: QueryDirection::Forward,
                target: QueryTarget::Base,
                partition: PartitionId::Left,
            };
            let stale_decision = state.query_decision(&base_query, false, stale_epoch);
            prop_assert_eq!(stale_decision.outcome, CacheReadOutcome::FallbackDb);
            prop_assert!(!stale_decision.serve_whole_page);
        }
    }

    #[test]
    fn state_aware_transition_sequences_preserve_model_invariants(
        selectors in prop::collection::vec(any::<u16>(), 1..120),
        query in query_strategy(),
    ) {
        let mut state = CacheState::authoritative_leader_base_state();
        for selector in selectors {
            let valid = valid_transitions(&state);
            prop_assert!(!valid.is_empty());

            let operation = valid[usize::from(selector) % valid.len()];
            state = state
                .try_apply(operation)
                .expect("state-aware transition should be valid");

            prop_assert!(state.is_valid());
            assert_query_invariants(&state, &query);

            if state.slot_has_unresolved_intent(0) {
                prop_assert!(!state.can_serve_eventual_get(0));
            }

            let stale_epoch = state.stale_request_epoch();
            let edge_query = QueryRequest {
                lower_bound: 0,
                upper_bound: 3,
                start_exclusive: -1,
                limit: 4,
                byte_budget: 384,
                only_even: false,
                direction: QueryDirection::Forward,
                target: QueryTarget::Base,
                partition: PartitionId::Left,
            };
            let stale_decision = state.query_decision(&edge_query, false, stale_epoch);
            prop_assert_eq!(stale_decision.outcome, CacheReadOutcome::FallbackDb);
            prop_assert!(!stale_decision.serve_whole_page);
        }
    }
}
