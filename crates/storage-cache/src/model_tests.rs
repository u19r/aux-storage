use std::collections::BTreeSet;

use crate::{
    CacheReadOutcome, CacheState, GsiOrderVersion, GsiQuerySpace, PartitionId, QueryDirection,
    QueryRequest, QueryTarget, Transition, TransitionRange,
};

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

fn apply_all(state: CacheState, transitions: &[Transition]) -> CacheState {
    transitions.iter().fold(state, |current, transition| {
        current
            .try_apply(*transition)
            .expect("scenario transition should be valid")
    })
}

#[test]
fn writes_warm_point_reads_but_not_queries() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 2 },
            Transition::LeaderCommitPut { slot: 2 },
            Transition::FollowerAcknowledgePut { slot: 2 },
        ],
    );
    let page = query(2, 3, 1, 2, 1024, QueryTarget::Base);

    assert!(state.is_valid());
    assert!(state.can_serve_eventual_get(2));
    assert!(!state.cache_plan(&page).serve_whole_page);
    assert!(state.cache_plan(&page).cache_evaluated_keys.is_empty());
}

#[test]
fn partial_base_schema_revalidation_serves_maximum_safe_prefix() {
    let stale = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::PreparePut { slot: 2 },
            Transition::LeaderCommitPut { slot: 2 },
            Transition::FollowerAcknowledgePut { slot: 2 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 3),
            },
            Transition::AdvanceBaseSchemaVersion,
        ],
    );
    let partially_revalidated = stale
        .try_apply(Transition::QueryFillBase {
            range: TransitionRange::new(0, 0),
        })
        .expect("partial revalidation should succeed");
    let short_page = query(0, 1, -1, 2, 128, QueryTarget::Base);
    let long_page = query(0, 1, -1, 2, 1024, QueryTarget::Base);

    assert!(partially_revalidated.is_valid());
    assert!(
        partially_revalidated
            .cache_plan(&short_page)
            .serve_whole_page
    );
    assert_eq!(
        partially_revalidated
            .cache_plan(&short_page)
            .cache_evaluated_keys,
        vec![0]
    );
    assert!(
        !partially_revalidated
            .cache_plan(&long_page)
            .serve_whole_page
    );
    assert_eq!(
        partially_revalidated
            .cache_plan(&long_page)
            .cache_evaluated_keys,
        vec![0]
    );
    assert!(
        partially_revalidated
            .cache_plan(&long_page)
            .db_suffix_needed
    );
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
                partially_revalidated.fresh_request_epoch(),
            )
            .outcome,
        CacheReadOutcome::Mixed
    );
}

#[test]
fn gsi_membership_rewrite_moves_between_query_spaces() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 1),
            },
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

    assert!(state.is_valid());
    assert!(state.cache_plan(&primary).serve_whole_page);
    assert!(state.cache_plan(&alternate).serve_whole_page);
    assert!(state.cache_plan(&primary).cache_evaluated_keys.is_empty());
    assert_eq!(state.cache_plan(&alternate).cache_evaluated_keys, vec![1]);
}

#[test]
fn query_coverage_does_not_leak_across_partitions() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::PreparePut { slot: 2 },
            Transition::LeaderCommitPut { slot: 2 },
            Transition::FollowerAcknowledgePut { slot: 2 },
            Transition::PreparePut { slot: 3 },
            Transition::LeaderCommitPut { slot: 3 },
            Transition::FollowerAcknowledgePut { slot: 3 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let left_page = QueryRequest {
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
    let right_page = QueryRequest {
        lower_bound: 2,
        upper_bound: 3,
        start_exclusive: 1,
        limit: 2,
        byte_budget: 1024,
        only_even: false,
        direction: QueryDirection::Forward,
        target: QueryTarget::Base,
        partition: PartitionId::Right,
    };

    assert!(state.cache_plan(&left_page).serve_whole_page);
    assert_eq!(
        state.cache_plan(&left_page).cache_evaluated_keys,
        vec![0, 1]
    );
    assert!(!state.cache_plan(&right_page).serve_whole_page);
    assert!(
        state
            .cache_plan(&right_page)
            .cache_evaluated_keys
            .is_empty()
    );
}

#[test]
fn gsi_sort_rewrite_requires_current_manifest_order() {
    let warm_state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::AddGsiMembership { slot: 0 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let gsi_page = QueryRequest {
        lower_bound: 0,
        upper_bound: 1,
        start_exclusive: -1,
        limit: 2,
        byte_budget: 1024,
        only_even: false,
        direction: QueryDirection::Forward,
        target: QueryTarget::Gsi(GsiQuerySpace::Primary),
        partition: PartitionId::Left,
    };
    let stale_order_state = CacheState {
        actual_primary_gsi_order_version: GsiOrderVersion::V1,
        ..warm_state.clone()
    };
    let refreshed_order_state = CacheState {
        leader_primary_gsi_order_version: GsiOrderVersion::V1,
        ..stale_order_state.clone()
    };

    assert!(warm_state.cache_plan(&gsi_page).serve_whole_page);
    assert_eq!(
        warm_state.cache_plan(&gsi_page).cache_evaluated_keys,
        vec![0, 1]
    );
    assert!(!stale_order_state.cache_plan(&gsi_page).serve_whole_page);
    assert!(
        stale_order_state
            .cache_plan(&gsi_page)
            .cache_evaluated_keys
            .is_empty()
    );
    assert!(refreshed_order_state.cache_plan(&gsi_page).serve_whole_page);
    assert_eq!(
        refreshed_order_state
            .cache_plan(&gsi_page)
            .cache_evaluated_keys,
        vec![1, 0]
    );
}

#[test]
fn promoted_follower_can_serve_point_reads_before_queries() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 3 },
            Transition::LeaderCommitPut { slot: 3 },
            Transition::FollowerAcknowledgePut { slot: 3 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 3),
            },
            Transition::SyncFollowerFromLeader,
            Transition::PromoteFollowerCatchingUp,
        ],
    );
    let single_item_page = query(2, 3, 1, 1, 1024, QueryTarget::Base);

    assert!(state.is_valid());
    assert!(state.can_serve_eventual_get(3));
    assert!(!state.cache_plan(&single_item_page).serve_whole_page);
}

#[test]
fn strong_gsi_queries_are_explicitly_invalid() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 3),
            },
        ],
    );
    let gsi_query = query(0, 1, -1, 2, 1024, QueryTarget::Gsi(GsiQuerySpace::Primary));

    assert_eq!(
        state
            .query_decision(&gsi_query, true, state.fresh_request_epoch())
            .outcome,
        CacheReadOutcome::InvalidGsiStrong
    );
}

#[test]
fn eventual_batch_get_serves_maximum_safe_subset() {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = BTreeSet::from([0, 2]);
    state.leader.items.payload_keys = BTreeSet::from([0]);
    state.leader.items.negative_keys = BTreeSet::from([1]);
    state.prepared_puts = BTreeSet::from([3]);

    assert!(state.is_valid());

    let requested = BTreeSet::from([0, 1, 2, 3]);
    let plan = state.batch_get_plan(false, &requested);

    assert_eq!(plan.served_keys, BTreeSet::from([0, 1]));
    assert_eq!(plan.fallback_keys, BTreeSet::from([2, 3]));
    assert_eq!(plan.cache_payload_keys, BTreeSet::from([0]));
    assert_eq!(plan.cache_negative_keys, BTreeSet::from([1]));
}
