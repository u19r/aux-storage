use std::collections::BTreeSet;

use crate::{
    CacheReadOutcome, CacheState, GsiQuerySpace, ObservedRead, PartitionId, QueryDirection,
    QueryRequest, QueryTarget, ReadRequest, Transition, TransitionRange, compare_observed_read,
};

fn apply_all(state: CacheState, transitions: &[Transition]) -> CacheState {
    transitions.iter().fold(state, |current, transition| {
        current
            .try_apply(*transition)
            .expect("differential scenario transition should be valid")
    })
}

#[test]
fn expected_read_matches_self_for_get_batch_and_query() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 3),
            },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 3),
            },
        ],
    );

    let get_request = ReadRequest::Get {
        slot: 1,
        strong: false,
        request_epoch: state.fresh_request_epoch(),
    };
    let batch_request = ReadRequest::BatchGet {
        requested_keys: BTreeSet::from([0_u8, 1, 2]),
        strong: false,
        request_epoch: state.fresh_request_epoch(),
    };
    let query_request = ReadRequest::Query {
        query: QueryRequest {
            lower_bound: 0,
            upper_bound: 1,
            start_exclusive: -1,
            limit: 2,
            byte_budget: 1024,
            only_even: false,
            direction: QueryDirection::Forward,
            target: QueryTarget::Base,
            partition: PartitionId::Left,
        },
        strong: false,
        request_epoch: state.fresh_request_epoch(),
    };

    assert_eq!(
        compare_observed_read(&state, &get_request, &state.expected_read(&get_request)),
        Ok(())
    );
    assert_eq!(
        compare_observed_read(&state, &batch_request, &state.expected_read(&batch_request)),
        Ok(())
    );
    assert_eq!(
        compare_observed_read(&state, &query_request, &state.expected_read(&query_request)),
        Ok(())
    );
}

#[test]
fn differential_compare_reports_first_mismatch_field() {
    let state = apply_all(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 2 },
            Transition::LeaderCommitPut { slot: 2 },
            Transition::FollowerAcknowledgePut { slot: 2 },
        ],
    );
    let request = ReadRequest::Get {
        slot: 2,
        strong: false,
        request_epoch: state.fresh_request_epoch(),
    };
    let observed = ObservedRead::Get {
        outcome: CacheReadOutcome::FallbackDb,
        slot_present: true,
    };

    let mismatch = compare_observed_read(&state, &request, &observed)
        .expect_err("mismatched observed read should fail");

    assert_eq!(mismatch.field, "outcome");
    assert_eq!(mismatch.expected, "ServeCache");
    assert_eq!(mismatch.observed, "FallbackDb");
}

#[test]
fn differential_compare_catches_query_page_mismatches() {
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
                range: TransitionRange::new(0, 3),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: QueryRequest {
            lower_bound: 0,
            upper_bound: 1,
            start_exclusive: -1,
            limit: 2,
            byte_budget: 1024,
            only_even: false,
            direction: QueryDirection::Forward,
            target: QueryTarget::Base,
            partition: PartitionId::Left,
        },
        strong: false,
        request_epoch: state.fresh_request_epoch(),
    };
    let observed = ObservedRead::Query {
        outcome: CacheReadOutcome::ServeCache,
        serve_whole_page: true,
        cache_evaluated_keys: vec![0, 1],
        returned_page: vec![1, 0],
    };

    let mismatch = compare_observed_read(&state, &request, &observed)
        .expect_err("bad returned page should be reported");

    assert_eq!(mismatch.field, "returned_page");
}

#[test]
fn expected_query_read_reports_mixed_for_safe_cached_prefix() {
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
                range: TransitionRange::new(0, 0),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: QueryRequest {
            lower_bound: 0,
            upper_bound: 1,
            start_exclusive: -1,
            limit: 2,
            byte_budget: 1024,
            only_even: false,
            direction: QueryDirection::Forward,
            target: QueryTarget::Base,
            partition: PartitionId::Left,
        },
        strong: false,
        request_epoch: state.fresh_request_epoch(),
    };

    assert_eq!(
        state.expected_read(&request),
        ObservedRead::Query {
            outcome: CacheReadOutcome::Mixed,
            serve_whole_page: false,
            cache_evaluated_keys: vec![0],
            returned_page: vec![0, 1],
        }
    );
}
