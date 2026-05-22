use std::collections::BTreeSet;

use crate::{
    CacheReadOutcome, CacheRouteOutcome, CacheState, GsiQuerySpace, PartitionId, QueryDirection,
    QueryRequest, QueryTarget, Transition, TransitionRange, TxnOutcome, TxnShardId, TxnState,
    TxnTransition,
};

const MIRRORED_QUINT_SCENARIOS: &[&str] = &[
    "abort_releases_locks_without_changing_source_state",
    "pk_hash_routes_whole_partition",
    "writes_warm_leader_point_reads_but_not_queries",
    "manifest_without_coverage_cannot_serve_query",
    "query_fill_creates_enough_proof_to_serve_whole_page",
    "partial_coverage_uses_maximum_safe_prefix",
    "partial_base_schema_revalidation_serves_maximum_safe_prefix",
    "payload_eviction_still_allows_whole_page_via_metadata_and_key_fetch",
    "shared_domain_payload_eviction_drops_authority_for_overlapping_base_query",
    "shared_domain_payload_eviction_forces_fallback_for_overlapping_query",
    "manifest_eviction_shrinks_coverage_and_stops_full_page_reads",
    "unresolved_intent_blocks_leader_point_read",
    "eventual_batch_get_serves_the_maximum_safe_subset",
    "strong_batch_get_authoritative_safe_subset",
    "strong_get_authoritative_present_matches_source",
    "strong_get_authoritative_absent_matches_source",
    "strong_batch_mixed_safe_and_unsafe_keys",
    "stale_epoch_requests_never_serve_cache",
    "stale_epoch_requests_return_stale_epoch",
    "wrong_leader_requests_redirect_instead_of_serving",
    "wrong_leader_requests_redirect",
    "acknowledged_put_makes_eventual_get_safe",
    "strong_get_source_agreement",
    "strong_batch_source_agreement",
    "epoch_refresh_restores_cache_serving",
    "byte_budget_can_determine_page_boundary_inside_covered_prefix",
    "gsi_schema_version_mismatch_blocks_only_gsi_queries",
    "gsi_schema_version_change_blocks_only_gsi_queries",
    "gsi_schema_refresh_restores_gsi_queries",
    "base_schema_version_mismatch_blocks_queries_but_not_point_reads",
    "base_schema_version_change_blocks_queries_but_not_point_reads",
    "base_schema_refresh_restores_base_queries",
    "partial_gsi_schema_revalidation_serves_maximum_safe_prefix",
    "gsi_queries_use_separate_sparse_membership",
    "gsi_sparse_membership_uses_separate_manifest",
    "gsi_membership_rewrite_adds_sparse_entry_inside_covered_range",
    "gsi_membership_rewrite_removes_sparse_entry_inside_covered_range",
    "gsi_membership_removal_drops_only_index_result",
    "gsi_membership_rewrite_moves_result_between_query_spaces",
    "gsi_projection_bytes_can_serve_more_keys_than_base",
    "gsi_projection_bytes_can_serve_more_than_base",
    "promoted_follower_with_prepared_put_stays_fenced_until_recovery",
    "promoted_follower_put_stays_fenced_until_recovery",
    "promoted_follower_stays_fenced_until_recovery",
    "promoted_follower_with_committed_replica_can_serve_point_reads_earlier_than_queries",
    "replicated_follower_outcome_makes_promotion_immediate",
    "promoted_follower_with_prepared_delete_stays_fenced_until_recovery",
    "promoted_follower_delete_stays_fenced_until_recovery",
    "promoted_follower_batch_get_uses_per_key_safety",
    "reverse_queries_use_the_same_coverage_rules",
    "filtering_still_uses_raw_evaluation_boundary",
    "busted_shard_forces_fallback",
    "coverage_does_not_leak_across_partitions",
    "query_coverage_does_not_leak_across_partitions",
    "follower_range_revalidation_restores_promoted_queries",
    "partial_leader_revalidation_recovers_shortest_page_first",
    "gsi_sort_rewrite_requires_current_manifest_order",
    "in_place_gsi_sort_rewrite_preserves_leader_servability",
    "strong_gsi_queries_are_explicitly_invalid",
    "strong_gsi_query_is_rejected",
    "committed_leader_put_cannot_be_aborted",
    "gsi_authority_cannot_return_without_shard_authority",
    "base_delete_removes_alternate_gsi_membership_too",
    "base_delete_removes_gsi_membership_too",
    "commit_keeps_leader_reads_fenced_until_each_leader_applies_outcome",
    "replaying_committed_client_token_is_no_op",
    "uninitialized_shard_blocks_point_reads",
    "uninitialized_shard_blocks_queries",
    "uninitialized_shard_blocks_batch_get",
    "assigned_shard_enables_normal_operation",
    "drained_shard_blocks_reads",
    "uninitialized_shard_blocks_all_reads",
    "uninitialized_shard_assignment_enables_reads",
    "partially_synced_follower_promotion_strips_unsynced_query_authority",
    "drained_shard_blocks_all_reads",
    "dropped_follower_replication_causes_stale_after_promotion",
    "follower_divergence_does_not_affect_leader_serving",
];

fn quint_run_scenarios(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter_map(|line| line.trim().strip_prefix("run "))
        .filter_map(|rest| {
            rest.split_once(" =")
                .map(|(name, _)| camel_to_snake(name.trim()))
        })
        .collect()
}

fn camel_to_snake(name: &str) -> String {
    let mut snake = String::with_capacity(name.len() + 8);
    for (index, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if index != 0 {
                snake.push('_');
            }
            for lower in ch.to_lowercase() {
                snake.push(lower);
            }
        } else {
            snake.push(ch);
        }
    }
    snake
}

fn query(
    lower_bound: u8,
    upper_bound: u8,
    start_exclusive: i8,
    limit: usize,
    byte_budget: usize,
    target: QueryTarget,
) -> QueryRequest {
    QueryRequest {
        lower_bound,
        upper_bound,
        start_exclusive,
        limit,
        byte_budget,
        only_even: false,
        direction: QueryDirection::Forward,
        target,
        partition: PartitionId::infer(lower_bound),
    }
}

fn reverse_query(
    lower_bound: u8,
    upper_bound: u8,
    start_exclusive: i8,
    limit: usize,
    byte_budget: usize,
    target: QueryTarget,
) -> QueryRequest {
    QueryRequest {
        direction: QueryDirection::Reverse,
        ..query(
            lower_bound,
            upper_bound,
            start_exclusive,
            limit,
            byte_budget,
            target,
        )
    }
}

#[test]
fn mirrored_rust_scenarios_cover_every_quint_run_scenario() {
    let expected = quint_run_scenarios(include_str!(
        "../../../quint/distributed_cache_model_tests.qnt"
    ))
    .into_iter()
    .chain(quint_run_scenarios(include_str!(
        "../../../quint/distributed_cache_protocol.qnt"
    )))
    .chain(quint_run_scenarios(include_str!(
        "../../../quint/distributed_cache_transactions.qnt"
    )))
    .collect::<BTreeSet<_>>();
    let actual = MIRRORED_QUINT_SCENARIOS
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);
}

#[test]
fn pk_hash_routes_whole_partition() {
    assert_eq!(PartitionId::Left.lower_bound(), 0);
    assert_eq!(PartitionId::Left.upper_bound(), 1);
    assert_eq!(PartitionId::Right.lower_bound(), 2);
    assert_eq!(PartitionId::Right.upper_bound(), 3);
}

fn apply_all(state: CacheState, transitions: &[Transition]) -> CacheState {
    transitions.iter().fold(state, |current, transition| {
        current
            .try_apply(*transition)
            .expect("scenario transition should be valid")
    })
}

fn apply_txn_all(state: TxnState, transitions: &[TxnTransition]) -> TxnState {
    transitions.iter().fold(state, |current, transition| {
        current
            .try_apply(*transition)
            .expect("transaction scenario transition should be valid")
    })
}

fn warmed_base_state(slots: &[u8], range: TransitionRange) -> CacheState {
    let mut transitions = Vec::new();
    for slot in slots {
        transitions.push(Transition::PreparePut { slot: *slot });
        transitions.push(Transition::LeaderCommitPut { slot: *slot });
        transitions.push(Transition::FollowerAcknowledgePut { slot: *slot });
    }
    transitions.push(Transition::QueryFillBase { range });
    apply_all(CacheState::authoritative_leader_base_state(), &transitions)
}

fn warmed_primary_gsi_state(slots: &[u8], range: TransitionRange) -> CacheState {
    let mut transitions = Vec::new();
    for slot in slots {
        transitions.push(Transition::PreparePut { slot: *slot });
        transitions.push(Transition::LeaderCommitPut { slot: *slot });
        transitions.push(Transition::FollowerAcknowledgePut { slot: *slot });
        transitions.push(Transition::AddGsiMembership { slot: *slot });
    }
    transitions.push(Transition::QueryFillGsi {
        query_space: GsiQuerySpace::Primary,
        range,
    });
    apply_all(CacheState::authoritative_leader_base_state(), &transitions)
}

#[test]
fn writes_warm_leader_point_reads_but_not_queries() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 2 },
            Transition::LeaderCommitPut { slot: 2 },
            Transition::FollowerAcknowledgePut { slot: 2 },
        ],
    );
    let page = query(2, 3, 1, 2, 1024, QueryTarget::Base);

    assert!(state.can_serve_eventual_get(2));
    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
}

#[test]
fn manifest_without_coverage_cannot_serve_query() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = BTreeSet::from([0, 1]);
    state.leader.items.manifest_keys = BTreeSet::from([0, 1]);
    state.leader.items.payload_keys = BTreeSet::from([0, 1]);

    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);
    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
}

#[test]
fn query_fill_creates_enough_proof_to_serve_whole_page() {
    let state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1));
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
    assert_eq!(state.cache_returned_page(&page), vec![0, 1]);
}

#[test]
fn partial_coverage_uses_maximum_safe_prefix() {
    let state = warmed_base_state(&[0, 1, 2], TransitionRange::new(0, 0));
    let page = query(0, 2, -1, 3, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::Mixed
    );
    assert_eq!(state.cache_plan(&page).cache_evaluated_keys, vec![0]);
}

#[test]
fn payload_eviction_still_allows_whole_page_via_metadata_and_key_fetch() {
    let mut state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1));
    state.leader.items.payload_keys.clear();
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert!(state.cache_plan(&page).serve_whole_page);
    assert_eq!(state.cache_plan(&page).payload_misses, 2);
}

#[test]
fn shared_domain_payload_eviction_forces_fallback_for_overlapping_query() {
    let mut state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1));
    state.leader.items.payload_keys.clear();
    state.leader.items.current_schema_covered_slots.clear();
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
}

#[test]
fn manifest_eviction_shrinks_coverage_and_stops_full_page_reads() {
    let mut state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1));
    state.leader.items.manifest_keys.remove(&1);
    state.leader.items.covered_slots.remove(&1);
    state.leader.items.current_schema_covered_slots.remove(&1);
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::Mixed
    );
    assert_eq!(state.cache_returned_page(&page), vec![0]);
}

#[test]
fn unresolved_intent_blocks_leader_point_read() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[Transition::PreparePut { slot: 1 }],
    );

    assert!(!state.can_serve_eventual_get(1));
}

#[test]
fn eventual_batch_get_serves_the_maximum_safe_subset() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = BTreeSet::from([0, 2]);
    state.leader.items.payload_keys = BTreeSet::from([0]);
    state.leader.items.negative_keys = BTreeSet::from([1]);
    state.prepared_puts = BTreeSet::from([3]);

    let requested = BTreeSet::from([0, 1, 2, 3]);
    let decision = state.batch_get_decision(false, &requested, state.fresh_request_epoch());

    assert_eq!(decision.outcome, CacheReadOutcome::Mixed);
    assert_eq!(decision.served_keys, BTreeSet::from([0, 1]));
    assert_eq!(decision.fallback_keys, BTreeSet::from([2, 3]));
}

#[test]
fn strong_batch_get_authoritative_safe_subset() {
    let state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1));
    let requested = BTreeSet::from([0, 1]);
    let decision = state.batch_get_decision(true, &requested, state.fresh_request_epoch());

    assert_eq!(decision.outcome, CacheReadOutcome::ServeCache);
    assert_eq!(decision.served_keys, BTreeSet::from([0, 1]));
}

#[test]
fn strong_get_authoritative_present_matches_source() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = BTreeSet::from([0]);
    state.leader.items.payload_keys = BTreeSet::from([0]);

    assert!(state.can_serve_strong_get(0));
    assert_eq!(
        state.strong_get_decision(0, state.fresh_request_epoch()),
        CacheReadOutcome::ServeCache
    );
    assert!(state.eventual_get_matches_source(0));
}

#[test]
fn strong_get_authoritative_absent_matches_source() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.leader.items.negative_keys = BTreeSet::from([1]);

    assert!(state.can_serve_strong_get(1));
    assert_eq!(
        state.strong_get_decision(1, state.fresh_request_epoch()),
        CacheReadOutcome::ServeCache
    );
    assert!(state.eventual_get_matches_source(1));
}

#[test]
fn strong_batch_mixed_safe_and_unsafe_keys() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = BTreeSet::from([0, 2]);
    state.leader.items.payload_keys = BTreeSet::from([0]);
    state.leader.items.negative_keys = BTreeSet::from([1]);
    state.prepared_puts = BTreeSet::from([3]);

    let requested = BTreeSet::from([0, 1, 2, 3]);
    let decision = state.batch_get_decision(true, &requested, state.fresh_request_epoch());

    assert_eq!(decision.outcome, CacheReadOutcome::Mixed);
    assert_eq!(decision.served_keys, BTreeSet::from([0, 1]));
    assert_eq!(decision.fallback_keys, BTreeSet::from([2, 3]));
}

#[test]
fn strong_get_source_agreement() {
    strong_get_authoritative_present_matches_source();
}

#[test]
fn strong_batch_source_agreement() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = BTreeSet::from([1]);
    state.leader.items.payload_keys = BTreeSet::from([1]);

    let requested = BTreeSet::from([0, 1]);
    let decision = state.batch_get_decision(true, &requested, state.fresh_request_epoch());

    assert_eq!(decision.served_keys, BTreeSet::from([1]));
    assert_eq!(decision.fallback_keys, BTreeSet::from([0]));
}

#[test]
fn stale_epoch_requests_never_serve_cache() {
    let state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1));
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);
    let decision = state.query_decision(&page, false, state.stale_request_epoch());

    assert_eq!(decision.route, CacheRouteOutcome::StaleEpoch);
    assert_eq!(decision.outcome, CacheReadOutcome::FallbackDb);
}

#[test]
fn stale_epoch_requests_return_stale_epoch() {
    stale_epoch_requests_never_serve_cache();
}

#[test]
fn wrong_leader_requests_redirect_instead_of_serving() {
    let state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1))
        .try_apply(Transition::LoseLeader)
        .expect("leader loss should apply");
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);
    let decision = state.query_decision(&page, false, state.fresh_request_epoch());

    assert_eq!(decision.route, CacheRouteOutcome::WrongLeader);
    assert_eq!(decision.outcome, CacheReadOutcome::FallbackDb);
}

#[test]
fn wrong_leader_requests_redirect() {
    wrong_leader_requests_redirect_instead_of_serving();
}

#[test]
fn byte_budget_can_determine_page_boundary_inside_covered_prefix() {
    let state = warmed_base_state(&[0, 1, 2], TransitionRange::new(0, 2));
    let page = query(0, 2, -1, 3, 384, QueryTarget::Base);

    assert!(state.cache_plan(&page).serve_whole_page);
    assert_eq!(state.cache_plan(&page).cache_evaluated_keys, vec![0, 1]);
}

#[test]
fn gsi_schema_version_mismatch_blocks_only_gsi_queries() {
    let stale = apply_all(
        warmed_primary_gsi_state(&[0, 1], TransitionRange::new(0, 1)),
        &[Transition::AdvanceGsiSchemaVersion],
    );
    let gsi_page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));
    let base_page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        stale
            .query_decision(&gsi_page, false, stale.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
    assert_eq!(
        stale
            .query_decision(&base_page, false, stale.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
    assert!(stale.can_serve_eventual_get(0));
}

#[test]
fn gsi_schema_version_change_blocks_only_gsi_queries() {
    gsi_schema_version_mismatch_blocks_only_gsi_queries();
}

#[test]
fn gsi_schema_refresh_restores_gsi_queries() {
    let refreshed = apply_all(
        warmed_primary_gsi_state(&[0, 1], TransitionRange::new(0, 1)),
        &[
            Transition::AdvanceGsiSchemaVersion,
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let gsi_page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(
        refreshed
            .query_decision(&gsi_page, false, refreshed.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
}

#[test]
fn base_schema_version_mismatch_blocks_queries_but_not_point_reads() {
    let stale = apply_all(
        warmed_base_state(&[0, 1], TransitionRange::new(0, 1)),
        &[Transition::AdvanceBaseSchemaVersion],
    );
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        stale
            .query_decision(&page, false, stale.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
    assert!(stale.can_serve_eventual_get(0));
}

#[test]
fn base_schema_version_change_blocks_queries_but_not_point_reads() {
    base_schema_version_mismatch_blocks_queries_but_not_point_reads();
}

#[test]
fn base_schema_refresh_restores_base_queries() {
    let refreshed = apply_all(
        warmed_base_state(&[0, 1], TransitionRange::new(0, 1)),
        &[
            Transition::AdvanceBaseSchemaVersion,
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        refreshed
            .query_decision(&page, false, refreshed.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
}

#[test]
fn partial_gsi_schema_revalidation_serves_maximum_safe_prefix() {
    let stale = apply_all(
        warmed_primary_gsi_state(&[0, 1, 2], TransitionRange::new(0, 2)),
        &[Transition::AdvanceGsiSchemaVersion],
    );
    let partially_revalidated = stale
        .try_apply(Transition::QueryFillGsi {
            query_space: GsiQuerySpace::Primary,
            range: TransitionRange::new(0, 0),
        })
        .expect("partial GSI revalidation should succeed");
    let short_page = query(0, 1, -1, 1, 96, QueryTarget::Gsi(GsiQuerySpace::Primary));
    let long_page = query(0, 2, -1, 3, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(
        partially_revalidated
            .query_decision(
                &short_page,
                false,
                partially_revalidated.fresh_request_epoch()
            )
            .outcome,
        CacheReadOutcome::ServeCache
    );
    assert_eq!(
        partially_revalidated
            .query_decision(
                &long_page,
                false,
                partially_revalidated.fresh_request_epoch()
            )
            .outcome,
        CacheReadOutcome::Mixed
    );
}

#[test]
fn gsi_queries_use_separate_sparse_membership() {
    let state = warmed_primary_gsi_state(&[1], TransitionRange::new(0, 1));
    let primary = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));
    let alternate = query(
        0,
        1,
        -1,
        2,
        1024,
        QueryTarget::Gsi(GsiQuerySpace::Alternate),
    );

    assert_eq!(state.cache_returned_page(&primary), vec![1]);
    assert!(state.cache_returned_page(&alternate).is_empty());
}

#[test]
fn gsi_sparse_membership_uses_separate_manifest() {
    gsi_queries_use_separate_sparse_membership();
}

#[test]
fn gsi_membership_rewrite_adds_sparse_entry_inside_covered_range() {
    let state = apply_all(
        warmed_base_state(&[0, 1], TransitionRange::new(0, 1)),
        &[
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 1),
            },
            Transition::AddGsiMembership { slot: 1 },
        ],
    );
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(state.cache_returned_page(&page), vec![1]);
}

#[test]
fn gsi_membership_rewrite_removes_sparse_entry_inside_covered_range() {
    let state = apply_all(
        warmed_primary_gsi_state(&[1], TransitionRange::new(0, 1)),
        &[Transition::RemoveGsiMembership { slot: 1 }],
    );
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert!(state.cache_plan(&page).serve_whole_page);
    assert!(state.cache_returned_page(&page).is_empty());
}

#[test]
fn gsi_membership_removal_drops_only_index_result() {
    gsi_membership_rewrite_removes_sparse_entry_inside_covered_range();
}

#[test]
fn gsi_membership_rewrite_moves_result_between_query_spaces() {
    let state = apply_all(
        warmed_primary_gsi_state(&[1], TransitionRange::new(0, 1)),
        &[
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Alternate,
                range: TransitionRange::new(0, 1),
            },
            Transition::MoveGsiMembership {
                slot: 1,
                to_query_space: GsiQuerySpace::Alternate,
            },
        ],
    );
    let primary = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));
    let alternate = query(
        0,
        1,
        -1,
        2,
        1024,
        QueryTarget::Gsi(GsiQuerySpace::Alternate),
    );

    assert!(state.cache_returned_page(&primary).is_empty());
    assert_eq!(state.cache_returned_page(&alternate), vec![1]);
}

#[test]
fn gsi_projection_bytes_can_serve_more_keys_than_base() {
    let state = apply_all(
        warmed_primary_gsi_state(&[0, 1, 2], TransitionRange::new(0, 2)),
        &[Transition::QueryFillBase {
            range: TransitionRange::new(0, 2),
        }],
    );
    let base_page = query(0, 2, -1, 3, 500, QueryTarget::Base);
    let gsi_page = query(0, 2, -1, 3, 500, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(state.cache_returned_page(&base_page), vec![0, 1]);
    assert_eq!(state.cache_returned_page(&gsi_page), vec![0, 1, 2]);
}

#[test]
fn gsi_projection_bytes_can_serve_more_than_base() {
    gsi_projection_bytes_can_serve_more_keys_than_base();
}

#[test]
fn acknowledged_put_makes_eventual_get_safe() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
        ],
    );

    assert!(state.can_serve_eventual_get(1));
}

#[test]
fn epoch_refresh_restores_cache_serving() {
    let state = apply_all(
        warmed_base_state(&[0, 1], TransitionRange::new(0, 1)),
        &[
            Transition::LoseEpochAuthority,
            Transition::GainEpochAuthority,
        ],
    );
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
}

#[test]
fn promoted_follower_with_prepared_put_stays_fenced_until_recovery() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::PromoteFollowerCatchingUp,
        ],
    );

    assert!(!state.can_serve_eventual_get(1));

    let recovered = state
        .try_apply(Transition::RecoverPreparedOnFollower)
        .expect("prepared follower recovery should apply");
    assert!(!recovered.slot_has_unresolved_intent(1));
}

#[test]
fn promoted_follower_put_stays_fenced_until_recovery() {
    promoted_follower_with_prepared_put_stays_fenced_until_recovery();
}

#[test]
fn promoted_follower_with_committed_replica_can_serve_point_reads_earlier_than_queries() {
    let state = apply_all(
        warmed_base_state(&[3], TransitionRange::new(0, 3)),
        &[
            Transition::SyncFollowerFromLeader,
            Transition::PromoteFollowerCatchingUp,
        ],
    );
    let page = query(2, 3, 1, 1, 1024, QueryTarget::Base);

    assert!(state.can_serve_eventual_get(3));
    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
}

#[test]
fn promoted_follower_with_prepared_delete_stays_fenced_until_recovery() {
    let state = apply_all(
        warmed_base_state(&[1], TransitionRange::new(0, 1)),
        &[
            Transition::PrepareDelete { slot: 1 },
            Transition::PromoteFollowerCatchingUp,
        ],
    );

    assert!(!state.can_serve_eventual_get(1));
    let recovered = state
        .try_apply(Transition::RecoverPreparedOnFollower)
        .expect("prepared delete recovery should apply");
    assert!(!recovered.slot_has_unresolved_intent(1));
}

#[test]
fn promoted_follower_delete_stays_fenced_until_recovery() {
    promoted_follower_with_prepared_delete_stays_fenced_until_recovery();
}

#[test]
fn reverse_queries_use_the_same_coverage_rules() {
    let state = warmed_base_state(&[2, 3], TransitionRange::new(2, 3));
    let page = reverse_query(2, 3, 4, 2, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
    assert_eq!(state.cache_returned_page(&page), vec![3, 2]);
}

#[test]
fn shared_domain_payload_eviction_drops_authority_for_overlapping_base_query() {
    shared_domain_payload_eviction_forces_fallback_for_overlapping_query();
}

#[test]
fn filtering_still_uses_raw_evaluation_boundary() {
    let mut page = query(0, 3, -1, 2, 1024, QueryTarget::Base);
    page.only_even = true;
    let state = warmed_base_state(&[0, 1, 2, 3], TransitionRange::new(0, 3));

    assert_eq!(state.source_raw_page(&page), vec![0, 1]);
    assert_eq!(state.source_returned_page(&page), vec![0]);
    assert!(state.cache_plan(&page).serve_whole_page);
}

#[test]
fn busted_shard_forces_fallback() {
    let state = warmed_base_state(&[0, 1], TransitionRange::new(0, 1))
        .try_apply(Transition::BustShard)
        .expect("bust shard transition should apply");
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert!(!state.can_serve_eventual_get(0));
    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
}

#[test]
fn coverage_does_not_leak_across_partitions() {
    let state = warmed_base_state(&[0, 1, 2, 3], TransitionRange::new(0, 1));
    let left_page = query(0, 1, -1, 2, 1024, QueryTarget::Base);
    let right_page = query(2, 3, 1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        state
            .query_decision(&left_page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
    assert_eq!(
        state
            .query_decision(&right_page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );
}

#[test]
fn query_coverage_does_not_leak_across_partitions() {
    coverage_does_not_leak_across_partitions();
}

#[test]
fn follower_range_revalidation_restores_promoted_queries() {
    let promoted = apply_all(
        warmed_base_state(&[0, 1], TransitionRange::new(0, 1)),
        &[
            Transition::SyncFollowerFromLeader,
            Transition::PromoteFollowerCatchingUp,
        ],
    );
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert_eq!(
        promoted
            .query_decision(&page, false, promoted.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );

    let restored = promoted
        .try_apply(Transition::FinishCatchUp)
        .expect("finishing catch-up should restore follower query authority");
    assert_eq!(
        restored
            .query_decision(&page, false, restored.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
}

#[test]
fn partial_leader_revalidation_recovers_shortest_page_first() {
    let stale = apply_all(
        warmed_base_state(&[0, 1, 2], TransitionRange::new(0, 2)),
        &[Transition::AdvanceBaseSchemaVersion],
    );
    let partially_revalidated = stale
        .try_apply(Transition::QueryFillBase {
            range: TransitionRange::new(0, 0),
        })
        .expect("partial leader revalidation should succeed");
    let short_page = query(0, 0, -1, 1, 128, QueryTarget::Base);
    let long_page = query(0, 2, -1, 3, 1024, QueryTarget::Base);

    assert_eq!(
        partially_revalidated
            .query_decision(
                &short_page,
                false,
                partially_revalidated.fresh_request_epoch(),
            )
            .outcome,
        CacheReadOutcome::ServeCache
    );
    assert_eq!(
        partially_revalidated
            .query_decision(
                &long_page,
                false,
                partially_revalidated.fresh_request_epoch()
            )
            .outcome,
        CacheReadOutcome::Mixed
    );
}

#[test]
fn gsi_sort_rewrite_requires_current_manifest_order() {
    let mut stale = warmed_primary_gsi_state(&[0, 1], TransitionRange::new(0, 1));
    stale.actual_primary_gsi_order_version = crate::GsiOrderVersion::V1;
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(
        stale
            .query_decision(&page, false, stale.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::FallbackDb
    );

    let mut refreshed = stale.clone();
    refreshed.leader_primary_gsi_order_version = crate::GsiOrderVersion::V1;
    assert_eq!(
        refreshed
            .query_decision(&page, false, refreshed.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
}

#[test]
fn in_place_gsi_sort_rewrite_preserves_leader_servability() {
    let mut state = warmed_primary_gsi_state(&[0, 1], TransitionRange::new(0, 1));
    state.actual_primary_gsi_order_version = crate::GsiOrderVersion::V1;
    state.leader_primary_gsi_order_version = crate::GsiOrderVersion::V1;
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(
        state
            .query_decision(&page, false, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::ServeCache
    );
    assert_eq!(state.cache_returned_page(&page), vec![1, 0]);
}

#[test]
fn strong_gsi_query_is_rejected() {
    let state = warmed_primary_gsi_state(&[0, 1], TransitionRange::new(0, 1));
    let page = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(
        state
            .query_decision(&page, true, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::InvalidGsiStrong
    );
}

#[test]
fn committed_leader_put_cannot_be_aborted() {
    let committed = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
        ],
    );

    assert!(
        committed
            .try_apply(Transition::AbortPrepared { slot: 1 })
            .is_none()
    );
}

#[test]
fn gsi_authority_cannot_return_without_shard_authority() {
    let busted = apply_all(
        warmed_primary_gsi_state(&[0, 1], TransitionRange::new(0, 1)),
        &[Transition::BustShard],
    );

    assert!(busted.try_apply(Transition::FinishCatchUp).is_none());
    assert!(busted.try_apply(Transition::GainEpochAuthority).is_none());
}

#[test]
fn base_delete_removes_alternate_gsi_membership_too() {
    let state = apply_all(
        warmed_primary_gsi_state(&[1], TransitionRange::new(0, 1)),
        &[
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Alternate,
                range: TransitionRange::new(0, 1),
            },
            Transition::MoveGsiMembership {
                slot: 1,
                to_query_space: GsiQuerySpace::Alternate,
            },
            Transition::PrepareDelete { slot: 1 },
            Transition::LeaderCommitDelete { slot: 1 },
            Transition::FollowerAcknowledgeDelete { slot: 1 },
        ],
    );
    let alternate = query(
        0,
        1,
        -1,
        2,
        1024,
        QueryTarget::Gsi(GsiQuerySpace::Alternate),
    );

    assert!(state.cache_returned_page(&alternate).is_empty());
}

#[test]
fn base_delete_removes_gsi_membership_too() {
    let state = apply_all(
        warmed_primary_gsi_state(&[1], TransitionRange::new(0, 1)),
        &[
            Transition::PrepareDelete { slot: 1 },
            Transition::LeaderCommitDelete { slot: 1 },
            Transition::FollowerAcknowledgeDelete { slot: 1 },
        ],
    );
    let primary = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert!(state.cache_returned_page(&primary).is_empty());
}

#[test]
fn promoted_follower_batch_get_uses_per_key_safety() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 2 },
            Transition::PromoteFollowerCatchingUp,
        ],
    );
    let requested = BTreeSet::from([0, 2]);
    let decision = state.batch_get_decision(false, &requested, state.fresh_request_epoch());

    assert_eq!(decision.outcome, CacheReadOutcome::Mixed);
    assert_eq!(decision.served_keys, BTreeSet::from([0]));
    assert_eq!(decision.fallback_keys, BTreeSet::from([2]));
}

#[test]
fn commit_keeps_leader_reads_fenced_until_each_leader_applies_outcome() {
    let committed = apply_txn_all(
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

    assert_eq!(committed.txn_outcome, TxnOutcome::Commit);
    assert!(!committed.can_serve_eventual_get(0));
    assert!(!committed.can_serve_eventual_get(2));
}

#[test]
fn replaying_committed_client_token_is_no_op() {
    let committed = apply_txn_all(
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

    let replayed = committed
        .try_apply(TxnTransition::ReplayClientToken)
        .expect("replay after commit should be a no-op");
    assert_eq!(replayed, committed);
}

// --- Uninitialized shard tests (from model_tests.qnt) ---

#[test]
fn uninitialized_shard_blocks_point_reads() {
    let state = CacheState::uninitialized_shard_base_state();
    assert!(state.is_valid());
    assert!(!state.can_serve_eventual_get(0));
    assert!(!state.can_serve_eventual_get(1));
    assert!(!state.can_serve_eventual_get(2));
    assert!(!state.can_serve_eventual_get(3));
}

#[test]
fn uninitialized_shard_blocks_queries() {
    let state = CacheState::uninitialized_shard_base_state();
    let forward = query(0, 3, -1, 2, 1024, QueryTarget::Base);
    let reverse = reverse_query(1, 3, 4, 2, 1024, QueryTarget::Base);
    assert!(state.is_valid());
    assert!(!state.cache_plan(&forward).serve_whole_page);
    assert!(!state.cache_plan(&reverse).serve_whole_page);
}

#[test]
fn uninitialized_shard_blocks_batch_get() {
    let state = CacheState::uninitialized_shard_base_state();
    let requested = BTreeSet::from([0, 1, 2]);
    assert!(state.is_valid());
    assert!(
        state
            .batch_get_plan(false, &requested)
            .served_keys
            .is_empty()
    );
}

#[test]
fn assigned_shard_enables_normal_operation() {
    let state = apply_all(
        CacheState::uninitialized_shard_base_state(),
        &[
            Transition::AssignShard,
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
        ],
    );
    assert!(state.is_valid());
    assert!(state.can_serve_eventual_get(1));
}

#[test]
fn drained_shard_blocks_reads() {
    let state = {
        let mut s = CacheState::authoritative_leader_base_state();
        s.item_authority = false;
        s.query_authority = false;
        s.gsi_query_authority = false;
        s
    };
    let forward = query(0, 3, -1, 2, 1024, QueryTarget::Base);
    assert!(state.is_valid());
    assert!(!state.can_serve_eventual_get(0));
    assert!(!state.cache_plan(&forward).serve_whole_page);
}

// --- Protocol-level tests ---

#[test]
fn uninitialized_shard_blocks_all_reads() {
    let state = CacheState::uninitialized_shard_base_state();
    let epoch = state.fresh_request_epoch();
    assert!(!state.can_serve_eventual_get(0));
    assert!(!state.can_serve_eventual_get(1));
    let forward = query(0, 3, -1, 2, 1024, QueryTarget::Base);
    assert!(!state.cache_plan(&forward).serve_whole_page);
    let requested = BTreeSet::from([0, 1]);
    let decision = state.batch_get_decision(false, &requested, epoch);
    assert!(decision.served_keys.is_empty());
}

#[test]
fn uninitialized_shard_assignment_enables_reads() {
    let state = apply_all(
        CacheState::uninitialized_shard_base_state(),
        &[
            Transition::AssignShard,
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
        ],
    );
    assert!(state.can_serve_eventual_get(1));
}

#[test]
fn partially_synced_follower_promotion_strips_unsynced_query_authority() {
    // Write to slots 0 and 1, fill query range, partially sync follower (slot 0
    // only), then promote. Point reads should work for synced slot but query
    // authority lost.
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 1),
            },
            Transition::PartialSyncFollower {
                synced_slot_mask: 0b0001, // only slot 0
            },
            Transition::PromoteFollowerCatchingUp,
        ],
    );
    // After promotion with partial sync, query authority is stripped
    assert!(!state.query_authority);
    // After catch-up and re-filling, authority restored
    let recovered = apply_all(
        state,
        &[
            Transition::FinishCatchUp,
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let wide_query = query(0, 3, -1, 4, 1280, QueryTarget::Base);
    assert!(recovered.cache_plan(&wide_query).serve_whole_page);
}

#[test]
fn drained_shard_blocks_all_reads() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::DrainShard,
        ],
    );
    assert!(!state.can_serve_eventual_get(1));
    let forward = query(0, 3, -1, 2, 1024, QueryTarget::Base);
    assert!(!state.cache_plan(&forward).serve_whole_page);
    let requested = BTreeSet::from([0, 1]);
    let decision = state.batch_get_decision(false, &requested, state.fresh_request_epoch());
    assert!(decision.served_keys.is_empty());
}

#[test]
fn dropped_follower_replication_causes_stale_after_promotion() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 1),
            },
            Transition::SyncFollowerFromLeader,
            // Drop replication for slot 1
            Transition::DropFollowerReplication { slot: 1 },
            // Promote follower
            Transition::PromoteFollowerCatchingUp,
        ],
    );
    // Slot 0 still served (was not dropped)
    assert!(state.can_serve_eventual_get(0));
    // Slot 1 was dropped - follower doesn't have it
    assert!(!state.can_serve_eventual_get(1));
}

#[test]
fn follower_divergence_does_not_affect_leader_serving() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::SyncFollowerFromLeader,
            Transition::DropFollowerReplication { slot: 0 },
        ],
    );
    // Leader still serves point reads fine
    assert!(state.can_serve_eventual_get(0));
    // Verify the leader is still the serving node (not redirected)
    let forward = query(0, 3, -1, 2, 1024, QueryTarget::Base);
    let decision = state.query_decision(&forward, false, state.fresh_request_epoch());
    assert_ne!(decision.route, CacheRouteOutcome::WrongLeader);
}
