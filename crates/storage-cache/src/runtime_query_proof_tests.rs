use std::collections::{BTreeMap, HashMap};

use storage_types::{AttributeValue, StorageError};

use crate::runtime_query_proof::{
    RuntimeCoverageRange, RuntimeOwnedQueryBounds, RuntimePageCoverageContext,
    RuntimePreparedQueryExecution, RuntimePreparedQueryRead, RuntimePreparedQueryReadOutcome,
    RuntimeQueryBounds, RuntimeQueryCoverageSemantics, RuntimeQueryDirection,
    RuntimeQueryProofMaterializedPage, RuntimeQueryProofReadPlan, RuntimeQueryReadBlockReason,
    RuntimeSortCondition, chain_covering_range_indexes, covering_current_schema_range_indexes,
    covering_current_schema_range_indexes_for_bounds, decide_query_page_plan, decide_query_read,
    derive_page_coverage_range, matching_order_key_positions,
    matching_order_key_positions_for_bounds, materialized_page_shape,
    parse_partition_key_condition, parse_runtime_sort_condition, plan_runtime_query_execution,
    prepare_runtime_query_proof_page_plan, prepare_runtime_query_read,
    prepare_runtime_query_window, query_sort_clause, range_contains_key, ranges_exhaust_request,
    ranges_exhaust_request_for_bounds, runtime_page_witness, runtime_page_witness_key,
    runtime_query_coverage_semantics, witnessed_page_len,
};

fn range(
    start_after_exclusive: Option<&str>,
    start_inclusive: Option<&str>,
    end_inclusive: Option<&str>,
) -> RuntimeCoverageRange<String> {
    RuntimeCoverageRange {
        start_after_exclusive: start_after_exclusive.map(str::to_string),
        start_inclusive: start_inclusive.map(str::to_string),
        end_inclusive: end_inclusive.map(str::to_string),
    }
}

#[test]
fn forward_range_chain_uses_end_to_start_linking() {
    let ranges = vec![
        range(None, Some("001"), Some("002")),
        range(Some("002"), Some("003"), None),
    ];

    let indexes = chain_covering_range_indexes(
        RuntimeQueryDirection::Forward,
        &ranges,
        None,
        Some(&"001".to_string()),
        None,
    );

    assert_eq!(indexes, vec![0, 1]);
    assert!(ranges_exhaust_request(
        RuntimeQueryDirection::Forward,
        &[ranges[0].clone(), ranges[1].clone()],
        Some(&"001".to_string()),
        None,
    ));
}

#[test]
fn reverse_range_chain_uses_start_to_start_after_linking() {
    let ranges = vec![
        range(None, Some("003"), Some("004")),
        range(Some("003"), None, Some("002")),
    ];

    let indexes = chain_covering_range_indexes(
        RuntimeQueryDirection::Reverse,
        &ranges,
        None,
        None,
        Some(&"004".to_string()),
    );

    assert_eq!(indexes, vec![0, 1]);
    assert!(ranges_exhaust_request(
        RuntimeQueryDirection::Reverse,
        &[ranges[0].clone(), ranges[1].clone()],
        None,
        Some(&"004".to_string()),
    ));
}

#[test]
fn range_contains_key_checks_inclusive_interval() {
    let range = range(Some("002"), Some("003"), Some("005"));

    assert!(!range_contains_key(&range, &"002".to_string()));
    assert!(range_contains_key(&range, &"003".to_string()));
    assert!(range_contains_key(&range, &"004".to_string()));
    assert!(range_contains_key(&range, &"005".to_string()));
    assert!(!range_contains_key(&range, &"006".to_string()));
}

#[test]
fn covering_current_schema_ranges_excludes_stale_gap_even_if_covered_history_exists() {
    let covered_ranges = vec![
        range(None, Some("001"), Some("002")),
        range(Some("002"), Some("003"), Some("004")),
        range(Some("004"), Some("005"), None),
    ];
    let current_schema_ranges = vec![covered_ranges[0].clone(), covered_ranges[2].clone()];

    let indexes = covering_current_schema_range_indexes(
        RuntimeQueryDirection::Forward,
        &covered_ranges,
        &current_schema_ranges,
        None,
        Some(&"001".to_string()),
        None,
    );

    assert_eq!(indexes, vec![0]);
}

#[test]
fn witnessed_boundary_can_serve_whole_page_without_exact_byte_math() {
    let plan = decide_query_page_plan(3, Some(2), 10, false);

    assert_eq!(plan.cache_candidate_count, 2);
    assert!(plan.page_boundary_witnessed);
    assert!(plan.would_serve_whole_page);

    let shape = materialized_page_shape(3, 10, false, plan);
    assert_eq!(shape.returned_count, 2);
    assert!(shape.has_more);
    assert!(shape.needs_resume_token);
    assert!(shape.page_complete);
}

#[test]
fn partial_prefix_needs_resume_token_even_without_page_witness() {
    let plan = decide_query_page_plan(2, None, 5, false);

    assert_eq!(plan.cache_candidate_count, 2);
    assert!(!plan.page_boundary_witnessed);
    assert!(!plan.would_serve_whole_page);

    let shape = materialized_page_shape(2, 5, false, plan);
    assert_eq!(shape.returned_count, 2);
    assert!(!shape.has_more);
    assert!(shape.needs_resume_token);
    assert!(!shape.page_complete);
}

#[test]
fn matching_order_key_positions_respects_bounds_coverage_and_reverse_order() {
    let ordered_sort_keys = vec![
        Some("001".to_string()),
        Some("002".to_string()),
        Some("003".to_string()),
        Some("004".to_string()),
    ];
    let coverage_ranges = vec![range(None, Some("002"), Some("004"))];

    let forward = matching_order_key_positions(
        &ordered_sort_keys,
        &coverage_ranges,
        Some(&"001".to_string()),
        Some(&"002".to_string()),
        Some(&"004".to_string()),
        RuntimeQueryDirection::Forward,
    );
    assert_eq!(forward, vec![1, 2, 3]);

    let reverse = matching_order_key_positions(
        &ordered_sort_keys,
        &coverage_ranges,
        Some(&"001".to_string()),
        Some(&"002".to_string()),
        Some(&"004".to_string()),
        RuntimeQueryDirection::Reverse,
    );
    assert_eq!(reverse, vec![3, 2, 1]);
}

#[test]
fn bounds_helpers_wrap_matching_and_exhaustion_rules() {
    let ordered_sort_keys = [
        Some("001".to_string()),
        Some("002".to_string()),
        Some("003".to_string()),
    ];
    let coverage_ranges = [range(None, Some("002"), Some("003"))];
    let ordered_sort_key_refs = ordered_sort_keys
        .iter()
        .map(|value| value.as_ref())
        .collect::<Vec<_>>();
    let coverage_range_refs = coverage_ranges
        .iter()
        .map(|range| RuntimeCoverageRange {
            start_after_exclusive: range.start_after_exclusive.as_ref(),
            start_inclusive: range.start_inclusive.as_ref(),
            end_inclusive: range.end_inclusive.as_ref(),
        })
        .collect::<Vec<_>>();
    let bounds = RuntimeQueryBounds {
        direction: RuntimeQueryDirection::Forward,
        start_exclusive: Some(&"001".to_string()),
        lower_inclusive: Some(&"002".to_string()),
        upper_inclusive: Some(&"003".to_string()),
    };

    assert_eq!(
        matching_order_key_positions_for_bounds(
            bounds,
            &ordered_sort_key_refs,
            &coverage_range_refs
        ),
        vec![1, 2]
    );
    assert!(ranges_exhaust_request_for_bounds(
        bounds,
        &coverage_range_refs
    ));
    let coverage_bounds = RuntimeQueryBounds {
        direction: RuntimeQueryDirection::Forward,
        start_exclusive: None,
        lower_inclusive: Some(&"002".to_string()),
        upper_inclusive: Some(&"003".to_string()),
    };
    assert_eq!(
        covering_current_schema_range_indexes_for_bounds(
            coverage_bounds,
            &coverage_range_refs,
            &coverage_range_refs,
        ),
        vec![0]
    );
}

#[test]
fn derive_page_coverage_range_handles_forward_and_reverse_boundaries() {
    let forward = derive_page_coverage_range(
        RuntimeQueryBounds {
            direction: RuntimeQueryDirection::Forward,
            start_exclusive: None,
            lower_inclusive: Some("002"),
            upper_inclusive: Some("005"),
        },
        Some("003".to_string()),
        Some("004".to_string()),
        RuntimePageCoverageContext {
            starts_at_partition_start: false,
            starts_at_partition_end: false,
            exhaustive_start_is_partition_start: false,
            exhaustive_end_is_partition_end: false,
            has_more: false,
        },
    );
    assert_eq!(
        forward,
        RuntimeCoverageRange {
            start_after_exclusive: None,
            start_inclusive: Some("002".to_string()),
            end_inclusive: Some("005".to_string()),
        }
    );

    let reverse = derive_page_coverage_range(
        RuntimeQueryBounds {
            direction: RuntimeQueryDirection::Reverse,
            start_exclusive: Some("009"),
            lower_inclusive: Some("002"),
            upper_inclusive: Some("009"),
        },
        Some("007".to_string()),
        Some("003".to_string()),
        RuntimePageCoverageContext {
            starts_at_partition_start: false,
            starts_at_partition_end: false,
            exhaustive_start_is_partition_start: false,
            exhaustive_end_is_partition_end: false,
            has_more: false,
        },
    );
    assert_eq!(
        reverse,
        RuntimeCoverageRange {
            start_after_exclusive: Some("009".to_string()),
            start_inclusive: Some("002".to_string()),
            end_inclusive: Some("007".to_string()),
        }
    );
}

#[test]
fn page_witness_helpers_round_trip_page_length() {
    let bounds = RuntimeQueryBounds {
        direction: RuntimeQueryDirection::Forward,
        start_exclusive: Some("001"),
        lower_inclusive: Some("002"),
        upper_inclusive: Some("005"),
    };
    let witness = runtime_page_witness(bounds, Some(3), 3, true).expect("witness");
    let key = runtime_page_witness_key(bounds, Some(3));
    let mut witnesses = BTreeMap::new();
    witnesses.insert(key.clone(), witness);

    assert_eq!(witnessed_page_len(&witnesses, &key), Some(3));
}

#[test]
fn owned_query_bounds_borrow_to_str_bounds() {
    let owned = RuntimeOwnedQueryBounds {
        direction: RuntimeQueryDirection::Reverse,
        start_exclusive: Some("009".to_string()),
        lower_inclusive: Some("002".to_string()),
        upper_inclusive: Some("009".to_string()),
    };

    assert_eq!(
        owned.as_str_bounds(),
        RuntimeQueryBounds {
            direction: RuntimeQueryDirection::Reverse,
            start_exclusive: Some("009"),
            lower_inclusive: Some("002"),
            upper_inclusive: Some("009"),
        }
    );
}

#[test]
fn prepare_runtime_query_window_combines_witness_coverage_and_matching_indexes() {
    let bounds = RuntimeQueryBounds {
        direction: RuntimeQueryDirection::Forward,
        start_exclusive: None,
        lower_inclusive: Some("002"),
        upper_inclusive: Some("005"),
    };
    let ordered_sort_keys = vec![Some("001"), Some("002"), Some("003"), Some("004")];
    let covered_ranges = vec![RuntimeCoverageRange {
        start_after_exclusive: None,
        start_inclusive: Some("002"),
        end_inclusive: Some("004"),
    }];
    let mut witnesses = BTreeMap::new();
    witnesses.insert(
        runtime_page_witness_key(bounds, Some(3)),
        runtime_page_witness(bounds, Some(3), 3, true).expect("witness"),
    );

    let prepared = prepare_runtime_query_window(
        bounds,
        Some(3),
        &ordered_sort_keys,
        &covered_ranges,
        &covered_ranges,
        &witnesses,
    )
    .expect("prepared window");

    assert_eq!(prepared.covered_range_indexes, vec![0]);
    assert_eq!(prepared.matching_indexes, vec![1, 2, 3]);
    assert_eq!(prepared.witnessed_page_len, Some(3));
    assert!(!prepared.request_exhausted);
}

#[test]
fn runtime_query_coverage_semantics_match_sort_condition_shape() {
    assert_eq!(
        runtime_query_coverage_semantics::<String>(None),
        RuntimeQueryCoverageSemantics {
            coverage_supported: true,
            starts_at_partition_start: true,
            starts_at_partition_end: true,
            exhaustive_start_is_partition_start: true,
            exhaustive_end_is_partition_end: true,
        }
    );

    assert_eq!(
        runtime_query_coverage_semantics(Some(&RuntimeSortCondition::Between {
            min: "002".to_string(),
            max: "005".to_string(),
        })),
        RuntimeQueryCoverageSemantics {
            coverage_supported: true,
            starts_at_partition_start: false,
            starts_at_partition_end: false,
            exhaustive_start_is_partition_start: false,
            exhaustive_end_is_partition_end: false,
        }
    );

    assert_eq!(
        runtime_query_coverage_semantics(Some(&RuntimeSortCondition::<String>::BeginsWith)),
        RuntimeQueryCoverageSemantics {
            coverage_supported: false,
            starts_at_partition_start: false,
            starts_at_partition_end: false,
            exhaustive_start_is_partition_start: false,
            exhaustive_end_is_partition_end: false,
        }
    );
}

#[test]
fn decide_query_read_returns_explicit_block_reason_before_page_math() {
    let decision = decide_query_read(
        Some(RuntimeQueryReadBlockReason::SchemaMismatch),
        10,
        Some(2),
        5,
        true,
    );

    assert!(!decision.would_serve_whole_page);
    assert_eq!(
        decision.block_reason,
        Some(RuntimeQueryReadBlockReason::SchemaMismatch)
    );
    assert_eq!(decision.cache_candidate_count, 0);
    assert!(!decision.page_boundary_witnessed);
}

#[test]
fn decide_query_read_reports_page_boundary_unknown_for_partial_prefix() {
    let decision = decide_query_read(None, 2, None, 5, false);

    assert!(!decision.would_serve_whole_page);
    assert_eq!(
        decision.block_reason,
        Some(RuntimeQueryReadBlockReason::PageBoundaryUnknown)
    );
    assert_eq!(decision.cache_candidate_count, 2);
}

#[test]
fn prepare_runtime_query_proof_page_plan_combines_plan_and_shape() {
    let prepared = prepare_runtime_query_proof_page_plan(None, 3, Some(2), 5, false);

    assert_eq!(
        prepared.plan,
        RuntimeQueryProofReadPlan {
            would_serve_whole_page: true,
            fallback_reason: None,
            cache_candidate_count: 2,
            page_boundary_witnessed: true,
        }
    );
    assert_eq!(
        prepared.page_shape,
        Some(crate::runtime_query_proof::RuntimeMaterializedPageShape {
            returned_count: 2,
            has_more: true,
            needs_resume_token: true,
            page_complete: true,
        })
    );
}

#[test]
fn prepare_runtime_query_proof_page_plan_omits_shape_for_blocked_read() {
    let prepared = prepare_runtime_query_proof_page_plan(
        Some(RuntimeQueryReadBlockReason::SchemaMismatch),
        3,
        Some(2),
        5,
        false,
    );

    assert_eq!(
        prepared.plan.fallback_reason,
        Some(crate::runtime_query_proof::RuntimeQueryProofFallbackReason::SchemaMismatch)
    );
    assert_eq!(prepared.page_shape, None);
}

#[test]
fn prepare_runtime_query_read_returns_whole_page_hit_partial_when_payloads_needed_refresh() {
    let prepared = prepare_runtime_query_read(
        &RuntimeQueryProofReadPlan {
            would_serve_whole_page: true,
            fallback_reason: None,
            cache_candidate_count: 2,
            page_boundary_witnessed: true,
        },
        RuntimeQueryProofMaterializedPage {
            primary_keys: vec!["pk1".to_string(), "pk2".to_string()],
            last_evaluated_key: Some("lek".to_string()),
            page_complete: true,
        },
        vec!["item1".to_string(), "item2".to_string()],
        true,
    );

    assert_eq!(
        prepared,
        RuntimePreparedQueryRead::WholePage {
            items: vec!["item1".to_string(), "item2".to_string()],
            last_evaluated_key: Some("lek".to_string()),
            outcome: RuntimePreparedQueryReadOutcome::HitPartial,
        }
    );
}

#[test]
fn prepare_runtime_query_read_returns_prefix_for_safe_cached_prefix() {
    let prepared = prepare_runtime_query_read(
        &RuntimeQueryProofReadPlan {
            would_serve_whole_page: false,
            fallback_reason: Some(
                crate::runtime_query_proof::RuntimeQueryProofFallbackReason::PageBoundaryUnknown,
            ),
            cache_candidate_count: 2,
            page_boundary_witnessed: false,
        },
        RuntimeQueryProofMaterializedPage {
            primary_keys: vec!["pk1".to_string(), "pk2".to_string()],
            last_evaluated_key: Some("resume".to_string()),
            page_complete: false,
        },
        vec!["item1".to_string(), "item2".to_string()],
        false,
    );

    assert_eq!(
        prepared,
        RuntimePreparedQueryRead::Prefix {
            items: vec!["item1".to_string(), "item2".to_string()],
            resume_token: "resume".to_string(),
        }
    );
}

#[test]
fn plan_runtime_query_execution_collapses_whole_page_and_prefix_paths() {
    let whole = plan_runtime_query_execution(
        RuntimePreparedQueryRead::WholePage {
            items: vec!["item1".to_string(), "item2".to_string()],
            last_evaluated_key: Some("lek".to_string()),
            outcome: RuntimePreparedQueryReadOutcome::Hit,
        },
        Some(2),
    );
    assert_eq!(
        whole,
        RuntimePreparedQueryExecution::WholePage {
            items: vec!["item1".to_string(), "item2".to_string()],
            last_evaluated_key: Some("lek".to_string()),
        }
    );

    let prefix_with_suffix = plan_runtime_query_execution(
        RuntimePreparedQueryRead::Prefix {
            items: vec!["item1".to_string(), "item2".to_string()],
            resume_token: "resume".to_string(),
        },
        Some(5),
    );
    assert_eq!(
        prefix_with_suffix,
        RuntimePreparedQueryExecution::PrefixWithDbSuffix {
            prefix_items: vec!["item1".to_string(), "item2".to_string()],
            resume_token: "resume".to_string(),
            remaining_limit: Some(3),
        }
    );

    let prefix_only = plan_runtime_query_execution(
        RuntimePreparedQueryRead::Prefix {
            items: vec!["item1".to_string(), "item2".to_string()],
            resume_token: "resume".to_string(),
        },
        Some(2),
    );
    assert_eq!(
        prefix_only,
        RuntimePreparedQueryExecution::PrefixOnly {
            items: vec!["item1".to_string(), "item2".to_string()],
            last_evaluated_key: "resume".to_string(),
        }
    );
}

#[test]
fn parse_partition_key_condition_resolves_expression_names() {
    let names = HashMap::from([("#pk".to_string(), "tenant_pk".to_string())]);

    let parsed =
        parse_partition_key_condition("#pk = :pk", Some(&names)).expect("partition condition");

    assert_eq!(parsed, ("tenant_pk".to_string(), ":pk".to_string()));
}

#[test]
fn parse_runtime_sort_condition_handles_between_and_begins_with() {
    let values = HashMap::from([
        (":min".to_string(), AttributeValue::S("001".to_string())),
        (":max".to_string(), AttributeValue::S("009".to_string())),
        (":prefix".to_string(), AttributeValue::S("sk#".to_string())),
    ]);

    let between = parse_runtime_sort_condition(
        query_sort_clause("pk = :pk AND sk BETWEEN :min AND :max"),
        None,
        &values,
        Some("sk"),
        |value| {
            value
                .inner_str()
                .map(|inner| format!("s:{inner}"))
                .map_err(|err| {
                    StorageError::internal(&format!("format sort value for test: {err}"))
                })
        },
    )
    .expect("between parse");
    assert_eq!(
        between,
        Some(RuntimeSortCondition::Between {
            min: "s:001".to_string(),
            max: "s:009".to_string(),
        })
    );

    let begins_with = parse_runtime_sort_condition(
        query_sort_clause("pk = :pk AND begins_with(sk, :prefix)"),
        None,
        &values,
        Some("sk"),
        |_value| Ok("unused".to_string()),
    )
    .expect("begins_with parse");
    assert_eq!(begins_with, Some(RuntimeSortCondition::BeginsWith));
}
