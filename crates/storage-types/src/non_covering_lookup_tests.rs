use alloc_counter::AllocationGuard;

use crate::{
    AttributeMap, AttributeValue, KeyAttributes, NonCoveringLookupAttachment,
    NonCoveringLookupCandidate, NonCoveringLookupError, NonCoveringLookupJoinMode,
    merge_non_covering_lookup_items, plan_non_covering_lookup,
};

const ALLOCATION_PROFILE_ITERATIONS: usize = 256;

#[test]
fn given_duplicate_child_keys_when_planning_then_backend_fetch_is_deduped() {
    let plan = plan_non_covering_lookup(
        [
            candidate(0, "child-1"),
            candidate(1, "child-1"),
            candidate(2, "child-2"),
        ],
        10,
    )
    .expect("lookup plan");

    assert_eq!(plan.parent_count, 3);
    assert_eq!(plan.fetches.len(), 2);
    assert_eq!(plan.fetches[0].parent_indexes, vec![0, 1]);
    assert_eq!(plan.fetches[1].parent_indexes, vec![2]);
}

#[test]
fn given_shared_child_key_when_merging_then_item_attaches_to_each_parent_in_order() {
    let plan = plan_non_covering_lookup([candidate(1, "child-1"), candidate(0, "child-1")], 10)
        .expect("lookup plan");

    let attachments = merge_non_covering_lookup_items(
        &plan,
        vec![Some(item("child-1"))],
        NonCoveringLookupJoinMode::LeftOne,
    )
    .expect("merged attachments");

    assert_eq!(
        attachments,
        vec![
            NonCoveringLookupAttachment::Item(item("child-1")),
            NonCoveringLookupAttachment::Item(item("child-1")),
        ]
    );
}

#[test]
fn given_array_join_when_duplicate_edges_exist_then_attachment_list_preserves_edges() {
    let plan = plan_non_covering_lookup([candidate(0, "child-1"), candidate(0, "child-1")], 10)
        .expect("lookup plan");

    let attachments = merge_non_covering_lookup_items(
        &plan,
        vec![Some(item("child-1"))],
        NonCoveringLookupJoinMode::Array,
    )
    .expect("merged attachments");

    assert_eq!(
        attachments,
        vec![NonCoveringLookupAttachment::Items(vec![
            item("child-1"),
            item("child-1")
        ])]
    );
}

#[test]
fn given_missing_item_when_left_join_then_parent_has_missing_attachment() {
    let plan = plan_non_covering_lookup([candidate(0, "missing")], 10).expect("lookup plan");

    let attachments =
        merge_non_covering_lookup_items(&plan, vec![None], NonCoveringLookupJoinMode::LeftOne)
            .expect("merged attachments");

    assert_eq!(attachments, vec![NonCoveringLookupAttachment::Missing]);
}

#[test]
fn given_missing_item_when_required_join_then_merge_fails() {
    let plan = plan_non_covering_lookup([candidate(0, "missing")], 10).expect("lookup plan");

    assert_eq!(
        merge_non_covering_lookup_items(&plan, vec![None], NonCoveringLookupJoinMode::RequiredOne),
        Err(NonCoveringLookupError::RequiredItemMissing { parent_index: 0 })
    );
}

#[test]
fn given_missing_item_when_inner_join_then_parent_is_dropped() {
    let plan = plan_non_covering_lookup([candidate(0, "missing")], 10).expect("lookup plan");

    let attachments =
        merge_non_covering_lookup_items(&plan, vec![None], NonCoveringLookupJoinMode::InnerOne)
            .expect("merged attachments");

    assert_eq!(attachments, vec![NonCoveringLookupAttachment::Dropped]);
}

#[test]
fn given_missing_item_when_array_join_then_parent_has_empty_items() {
    let plan = plan_non_covering_lookup([candidate(0, "missing")], 10).expect("lookup plan");

    let attachments =
        merge_non_covering_lookup_items(&plan, vec![None], NonCoveringLookupJoinMode::Array)
            .expect("merged attachments");

    assert_eq!(
        attachments,
        vec![NonCoveringLookupAttachment::Items(Vec::new())]
    );
}

#[test]
fn given_too_many_candidates_when_planning_then_cap_is_enforced_before_fetching() {
    assert!(matches!(
        plan_non_covering_lookup([candidate(0, "child-1"), candidate(1, "child-2")], 1),
        Err(NonCoveringLookupError::CandidateLimitExceeded {
            actual: 2,
            limit: 1
        })
    ));
}

#[test]
fn given_mismatched_fetch_results_when_merging_then_error_names_expected_count() {
    let plan = plan_non_covering_lookup([candidate(0, "child-1")], 10).expect("lookup plan");

    assert_eq!(
        merge_non_covering_lookup_items(&plan, Vec::new(), NonCoveringLookupJoinMode::LeftOne),
        Err(NonCoveringLookupError::FetchedItemCountMismatch {
            expected: 1,
            actual: 0
        })
    );
}

#[test]
fn non_covering_lookup_key_mapping_allocation_baseline_tests() {
    let report = measure_key_mapping_allocations();

    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn non_covering_lookup_merge_allocation_baseline_tests() {
    let report = measure_merge_allocations();

    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

fn measure_key_mapping_allocations() -> alloc_counter::AllocationReport<'static> {
    let candidates = (0..32)
        .map(|index| candidate(index, &format!("child-{}", index % 4)))
        .collect::<Vec<_>>();
    let guard = AllocationGuard::start(
        module_path!(),
        "non_covering_lookup_key_mapping_allocation_profile_tests",
        file!(),
        line!(),
        Some("key_mapping"),
    );

    for _ in 0..ALLOCATION_PROFILE_ITERATIONS {
        let plan = plan_non_covering_lookup(candidates.clone(), 64).expect("lookup plan");
        std::hint::black_box(plan);
    }

    guard.finish()
}

fn measure_merge_allocations() -> alloc_counter::AllocationReport<'static> {
    let plan = plan_non_covering_lookup((0..32).map(|index| candidate(index, "child-1")), 64)
        .expect("lookup plan");
    let fetched = vec![Some(item("child-1"))];
    let guard = AllocationGuard::start(
        module_path!(),
        "non_covering_lookup_merge_allocation_profile_tests",
        file!(),
        line!(),
        Some("merge"),
    );

    for _ in 0..ALLOCATION_PROFILE_ITERATIONS {
        let attachments = merge_non_covering_lookup_items(
            &plan,
            fetched.clone(),
            NonCoveringLookupJoinMode::LeftOne,
        )
        .expect("merged attachments");
        std::hint::black_box(attachments);
    }

    guard.finish()
}

fn candidate(parent_index: usize, child_id: &str) -> NonCoveringLookupCandidate {
    NonCoveringLookupCandidate {
        parent_index,
        key: KeyAttributes::from([("pk".to_string(), AttributeValue::S(child_id.to_string()))]),
    }
}

fn item(child_id: &str) -> AttributeMap {
    [("pk".to_string(), AttributeValue::S(child_id.to_string()))]
        .into_iter()
        .collect()
}
