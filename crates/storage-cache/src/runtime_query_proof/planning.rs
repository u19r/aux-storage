use std::{cmp::Ordering, collections::BTreeMap};

use crate::runtime_query_proof::{
    RuntimeCoverageRange, RuntimeMaterializedPageShape, RuntimePageCoverageContext,
    RuntimePageWitness, RuntimePageWitnessKey, RuntimeParsedQueryShape,
    RuntimePreparedQueryExecution, RuntimePreparedQueryProofPagePlan,
    RuntimePreparedQueryProofRead, RuntimePreparedQueryRead, RuntimePreparedQueryReadOutcome,
    RuntimePreparedQueryWindow, RuntimeQueryBounds, RuntimeQueryCoverageSemantics,
    RuntimeQueryDirection, RuntimeQueryPagePlan, RuntimeQueryProofFallbackReason,
    RuntimeQueryProofMaterializedPage, RuntimeQueryProofReadPlan, RuntimeQueryReadBlockReason,
    RuntimeQueryReadDecision, RuntimeSortCondition,
};

#[must_use]
pub fn chain_covering_range_indexes<T: Ord>(
    direction: RuntimeQueryDirection,
    ranges: &[RuntimeCoverageRange<T>],
    start_exclusive: Option<&T>,
    lower_inclusive: Option<&T>,
    upper_inclusive: Option<&T>,
) -> Vec<usize> {
    let Some(first_index) = ranges.iter().position(|range| {
        range_covers_start(
            direction,
            range,
            start_exclusive,
            lower_inclusive,
            upper_inclusive,
        )
    }) else {
        return Vec::new();
    };

    let mut chained = vec![first_index];
    loop {
        let Some(last_index) = chained.last().copied() else {
            return Vec::new();
        };
        let last_range = &ranges[last_index];
        let next_after = match direction {
            RuntimeQueryDirection::Forward => last_range.end_inclusive.as_ref(),
            RuntimeQueryDirection::Reverse => last_range.start_inclusive.as_ref(),
        };
        let Some(next_after) = next_after else {
            break;
        };

        let Some(next_index) = ranges
            .iter()
            .enumerate()
            .filter(|(index, range)| {
                !chained.contains(index) && range.start_after_exclusive.as_ref() == Some(next_after)
            })
            .max_by(|(_, left), (_, right)| compare_range_coverage(direction, left, right))
            .map(|(index, _)| index)
        else {
            break;
        };
        chained.push(next_index);
    }

    chained
}

#[must_use]
pub fn covering_current_schema_range_indexes<T: Ord + Eq>(
    direction: RuntimeQueryDirection,
    covered_ranges: &[RuntimeCoverageRange<T>],
    current_schema_ranges: &[RuntimeCoverageRange<T>],
    start_exclusive: Option<&T>,
    lower_inclusive: Option<&T>,
    upper_inclusive: Option<&T>,
) -> Vec<usize> {
    let eligible_indexes = covered_ranges
        .iter()
        .enumerate()
        .filter_map(|(index, range)| current_schema_ranges.contains(range).then_some(index))
        .collect::<Vec<_>>();
    let eligible_ranges = eligible_indexes
        .iter()
        .map(|index| RuntimeCoverageRange {
            start_after_exclusive: covered_ranges[*index].start_after_exclusive.as_ref(),
            start_inclusive: covered_ranges[*index].start_inclusive.as_ref(),
            end_inclusive: covered_ranges[*index].end_inclusive.as_ref(),
        })
        .collect::<Vec<_>>();

    chain_covering_range_indexes(
        direction,
        &eligible_ranges,
        start_exclusive.as_ref(),
        lower_inclusive.as_ref(),
        upper_inclusive.as_ref(),
    )
    .into_iter()
    .map(|eligible_index| eligible_indexes[eligible_index])
    .collect()
}

#[must_use]
pub fn covering_current_schema_range_indexes_for_bounds<T: Ord + Eq + ?Sized>(
    bounds: RuntimeQueryBounds<&T>,
    covered_ranges: &[RuntimeCoverageRange<&T>],
    current_schema_ranges: &[RuntimeCoverageRange<&T>],
) -> Vec<usize> {
    covering_current_schema_range_indexes(
        bounds.direction,
        covered_ranges,
        current_schema_ranges,
        bounds.start_exclusive.as_ref(),
        bounds.lower_inclusive.as_ref(),
        bounds.upper_inclusive.as_ref(),
    )
}

#[must_use]
pub fn ranges_exhaust_request<T: Ord>(
    direction: RuntimeQueryDirection,
    ranges: &[RuntimeCoverageRange<T>],
    lower_inclusive: Option<&T>,
    upper_inclusive: Option<&T>,
) -> bool {
    let Some(last_range) = ranges.last() else {
        return false;
    };

    match direction {
        RuntimeQueryDirection::Forward => {
            match (upper_inclusive, last_range.end_inclusive.as_ref()) {
                (Some(upper), Some(end)) => end >= upper,
                (Some(_), None) => false,
                (None, None) => true,
                (None, Some(_)) => false,
            }
        }
        RuntimeQueryDirection::Reverse => {
            match (lower_inclusive, last_range.start_inclusive.as_ref()) {
                (Some(lower), Some(start)) => start <= lower,
                (Some(_), None) => false,
                (None, None) => true,
                (None, Some(_)) => false,
            }
        }
    }
}

#[must_use]
pub fn ranges_exhaust_request_for_bounds<T: Ord + ?Sized>(
    bounds: RuntimeQueryBounds<&T>,
    ranges: &[RuntimeCoverageRange<&T>],
) -> bool {
    ranges_exhaust_request(
        bounds.direction,
        ranges,
        bounds.lower_inclusive.as_ref(),
        bounds.upper_inclusive.as_ref(),
    )
}

#[must_use]
pub fn derive_page_coverage_range(
    bounds: RuntimeQueryBounds<&str>,
    natural_first: Option<String>,
    natural_last: Option<String>,
    context: RuntimePageCoverageContext,
) -> RuntimeCoverageRange<String> {
    let natural_low = if bounds.direction == RuntimeQueryDirection::Forward {
        natural_first.clone()
    } else {
        natural_last.clone()
    };
    let natural_high = if bounds.direction == RuntimeQueryDirection::Forward {
        natural_last
    } else {
        natural_first
    };

    let (start_inclusive, end_inclusive) = if bounds.direction == RuntimeQueryDirection::Forward {
        let start_inclusive = if bounds.start_exclusive.is_some() {
            natural_low
        } else if let Some(lower) = bounds.lower_inclusive {
            Some(lower.to_string())
        } else if context.starts_at_partition_start {
            None
        } else {
            natural_low
        };

        let end_inclusive = if context.has_more {
            natural_high
        } else if let Some(upper) = bounds.upper_inclusive {
            Some(upper.to_string())
        } else if context.exhaustive_end_is_partition_end {
            None
        } else {
            natural_high
        };
        (start_inclusive, end_inclusive)
    } else {
        let start_inclusive = if context.has_more {
            natural_low
        } else if let Some(lower) = bounds.lower_inclusive {
            Some(lower.to_string())
        } else if context.exhaustive_start_is_partition_start {
            None
        } else {
            natural_low
        };

        let end_inclusive = if bounds.start_exclusive.is_some() {
            natural_high
        } else if let Some(upper) = bounds.upper_inclusive {
            Some(upper.to_string())
        } else if context.starts_at_partition_end {
            None
        } else {
            natural_high
        };
        (start_inclusive, end_inclusive)
    };

    RuntimeCoverageRange {
        start_after_exclusive: bounds.start_exclusive.map(str::to_string),
        start_inclusive,
        end_inclusive,
    }
}

#[must_use]
pub fn derive_runtime_page_coverage_range(
    shape: &RuntimeParsedQueryShape<String>,
    page_sort_keys: &[Option<String>],
    has_more: bool,
) -> Option<RuntimeCoverageRange<String>> {
    if !shape.coverage_semantics.coverage_supported {
        return None;
    }

    if !shape.has_sort_key {
        return if has_more {
            None
        } else {
            Some(RuntimeCoverageRange {
                start_after_exclusive: None,
                start_inclusive: None,
                end_inclusive: None,
            })
        };
    }

    if page_sort_keys.is_empty() {
        return if has_more || shape.bounds.start_exclusive.is_some() {
            None
        } else {
            Some(RuntimeCoverageRange {
                start_after_exclusive: None,
                start_inclusive: shape.bounds.lower_inclusive.clone(),
                end_inclusive: shape.bounds.upper_inclusive.clone(),
            })
        };
    }

    Some(derive_page_coverage_range(
        shape.bounds.as_str_bounds(),
        page_sort_keys.first().cloned().flatten(),
        page_sort_keys.last().cloned().flatten(),
        RuntimePageCoverageContext {
            starts_at_partition_start: shape.coverage_semantics.starts_at_partition_start,
            starts_at_partition_end: shape.coverage_semantics.starts_at_partition_end,
            exhaustive_start_is_partition_start: shape
                .coverage_semantics
                .exhaustive_start_is_partition_start,
            exhaustive_end_is_partition_end: shape
                .coverage_semantics
                .exhaustive_end_is_partition_end,
            has_more,
        },
    ))
}

#[must_use]
pub fn runtime_query_coverage_semantics<T>(
    sort_condition: Option<&RuntimeSortCondition<T>>,
) -> RuntimeQueryCoverageSemantics {
    RuntimeQueryCoverageSemantics {
        coverage_supported: sort_condition
            .is_none_or(|condition| !matches!(condition, RuntimeSortCondition::BeginsWith)),
        starts_at_partition_start: sort_condition.is_none_or(|condition| {
            matches!(
                condition,
                RuntimeSortCondition::LessThan | RuntimeSortCondition::LessThanEqual { .. }
            )
        }),
        starts_at_partition_end: sort_condition.is_none_or(|condition| {
            matches!(
                condition,
                RuntimeSortCondition::GreaterThan | RuntimeSortCondition::GreaterThanEqual { .. }
            )
        }),
        exhaustive_start_is_partition_start: sort_condition.is_none_or(|condition| {
            matches!(
                condition,
                RuntimeSortCondition::LessThan | RuntimeSortCondition::LessThanEqual { .. }
            )
        }),
        exhaustive_end_is_partition_end: sort_condition.is_none_or(|condition| {
            matches!(
                condition,
                RuntimeSortCondition::GreaterThan | RuntimeSortCondition::GreaterThanEqual { .. }
            )
        }),
    }
}

#[must_use]
pub fn runtime_page_witness_key(
    bounds: RuntimeQueryBounds<&str>,
    limit: Option<usize>,
) -> RuntimePageWitnessKey<String> {
    RuntimePageWitnessKey {
        direction: bounds.direction,
        start_exclusive: bounds.start_exclusive.map(str::to_string),
        lower_inclusive: bounds.lower_inclusive.map(str::to_string),
        upper_inclusive: bounds.upper_inclusive.map(str::to_string),
        limit,
    }
}

#[must_use]
pub fn runtime_page_witness(
    bounds: RuntimeQueryBounds<&str>,
    limit: Option<usize>,
    returned_count: usize,
    has_more: bool,
) -> Option<RuntimePageWitness<String>> {
    if !has_more || returned_count == 0 {
        return None;
    }

    Some(RuntimePageWitness {
        key: runtime_page_witness_key(bounds, limit),
        returned_count,
    })
}

#[must_use]
pub fn witnessed_page_len<T: Ord>(
    page_witnesses: &BTreeMap<RuntimePageWitnessKey<T>, RuntimePageWitness<T>>,
    key: &RuntimePageWitnessKey<T>,
) -> Option<usize> {
    page_witnesses
        .get(key)
        .map(|witness| witness.returned_count)
}

#[must_use]
pub fn prepare_runtime_query_window(
    bounds: RuntimeQueryBounds<&str>,
    limit: Option<usize>,
    ordered_sort_keys: &[Option<&str>],
    covered_ranges: &[RuntimeCoverageRange<&str>],
    current_schema_ranges: &[RuntimeCoverageRange<&str>],
    page_witnesses: &BTreeMap<RuntimePageWitnessKey<String>, RuntimePageWitness<String>>,
) -> Option<RuntimePreparedQueryWindow> {
    let covered_range_indexes = covering_current_schema_range_indexes_for_bounds(
        bounds,
        covered_ranges,
        current_schema_ranges,
    );
    if covered_range_indexes.is_empty() {
        return None;
    }

    let selected_ranges = covered_range_indexes
        .iter()
        .map(|index| covered_ranges[*index].clone())
        .collect::<Vec<_>>();

    Some(RuntimePreparedQueryWindow {
        matching_indexes: matching_order_key_positions_for_bounds(
            bounds,
            ordered_sort_keys,
            &selected_ranges,
        ),
        witnessed_page_len: witnessed_page_len(
            page_witnesses,
            &runtime_page_witness_key(bounds, limit),
        ),
        request_exhausted: ranges_exhaust_request_for_bounds(bounds, &selected_ranges),
        covered_range_indexes,
    })
}

#[must_use]
pub fn range_contains_key<T: Ord>(range: &RuntimeCoverageRange<T>, key: &T) -> bool {
    if let Some(start) = range.start_inclusive.as_ref()
        && key < start
    {
        return false;
    }
    if let Some(end) = range.end_inclusive.as_ref()
        && key > end
    {
        return false;
    }
    true
}

#[must_use]
pub fn decide_query_page_plan(
    matching_count: usize,
    witnessed_page_len: Option<usize>,
    limit: usize,
    request_exhausted: bool,
) -> RuntimeQueryPagePlan {
    let page_boundary_witnessed =
        witnessed_page_len.is_some_and(|returned_count| matching_count >= returned_count);
    let cache_candidate_count = witnessed_page_len
        .filter(|returned_count| matching_count >= *returned_count)
        .unwrap_or(matching_count.min(limit));

    RuntimeQueryPagePlan {
        would_serve_whole_page: page_boundary_witnessed
            || cache_candidate_count >= limit
            || request_exhausted,
        cache_candidate_count,
        page_boundary_witnessed,
    }
}

#[must_use]
pub fn materialized_page_shape(
    total_matching_entries: usize,
    limit: usize,
    request_exhausted: bool,
    plan: RuntimeQueryPagePlan,
) -> RuntimeMaterializedPageShape {
    let returned_count = total_matching_entries.min(plan.cache_candidate_count);
    let has_more = if plan.page_boundary_witnessed {
        true
    } else {
        returned_count == limit && (total_matching_entries > limit || !request_exhausted)
    };

    RuntimeMaterializedPageShape {
        returned_count,
        has_more,
        needs_resume_token: has_more || (!plan.would_serve_whole_page && returned_count > 0),
        page_complete: plan.would_serve_whole_page,
    }
}

#[must_use]
pub fn matching_order_key_positions<T: Ord>(
    ordered_sort_keys: &[Option<T>],
    coverage_ranges: &[RuntimeCoverageRange<T>],
    start_exclusive: Option<&T>,
    lower_inclusive: Option<&T>,
    upper_inclusive: Option<&T>,
    direction: RuntimeQueryDirection,
) -> Vec<usize> {
    let mut indexes = ordered_sort_keys
        .iter()
        .enumerate()
        .filter_map(|(index, sort_key)| {
            if !order_key_matches_bounds(
                sort_key.as_ref(),
                start_exclusive,
                lower_inclusive,
                upper_inclusive,
            ) {
                return None;
            }

            coverage_ranges
                .iter()
                .any(|range| range_contains_order_key(range, sort_key.as_ref()))
                .then_some(index)
        })
        .collect::<Vec<_>>();

    if direction == RuntimeQueryDirection::Reverse {
        indexes.reverse();
    }
    indexes
}

#[must_use]
pub fn matching_order_key_positions_for_bounds<T: Ord + ?Sized>(
    bounds: RuntimeQueryBounds<&T>,
    ordered_sort_keys: &[Option<&T>],
    coverage_ranges: &[RuntimeCoverageRange<&T>],
) -> Vec<usize> {
    matching_order_key_positions(
        ordered_sort_keys,
        coverage_ranges,
        bounds.start_exclusive.as_ref(),
        bounds.lower_inclusive.as_ref(),
        bounds.upper_inclusive.as_ref(),
        bounds.direction,
    )
}

#[must_use]
pub fn decide_query_read(
    block_reason: Option<RuntimeQueryReadBlockReason>,
    matching_count: usize,
    witnessed_page_len: Option<usize>,
    limit: usize,
    request_exhausted: bool,
) -> RuntimeQueryReadDecision {
    let Some(_) = block_reason else {
        let page_plan =
            decide_query_page_plan(matching_count, witnessed_page_len, limit, request_exhausted);
        return RuntimeQueryReadDecision {
            would_serve_whole_page: page_plan.would_serve_whole_page,
            block_reason: if page_plan.would_serve_whole_page {
                None
            } else {
                Some(RuntimeQueryReadBlockReason::PageBoundaryUnknown)
            },
            cache_candidate_count: page_plan.cache_candidate_count,
            page_boundary_witnessed: page_plan.page_boundary_witnessed,
        };
    };

    RuntimeQueryReadDecision {
        would_serve_whole_page: false,
        block_reason,
        cache_candidate_count: 0,
        page_boundary_witnessed: false,
    }
}

#[must_use]
pub fn runtime_query_proof_fallback_reason(
    reason: RuntimeQueryReadBlockReason,
) -> RuntimeQueryProofFallbackReason {
    match reason {
        RuntimeQueryReadBlockReason::CacheDisabled => {
            RuntimeQueryProofFallbackReason::CacheDisabled
        }
        RuntimeQueryReadBlockReason::StrongReadBypass => {
            RuntimeQueryProofFallbackReason::StrongReadBypass
        }
        RuntimeQueryReadBlockReason::UnsupportedKeyCondition => {
            RuntimeQueryProofFallbackReason::UnsupportedKeyCondition
        }
        RuntimeQueryReadBlockReason::MissingPartition => {
            RuntimeQueryProofFallbackReason::MissingPartition
        }
        RuntimeQueryReadBlockReason::ContinuityBroken => {
            RuntimeQueryProofFallbackReason::ContinuityBroken
        }
        RuntimeQueryReadBlockReason::Rebuilding => RuntimeQueryProofFallbackReason::Rebuilding,
        RuntimeQueryReadBlockReason::SchemaMismatch => {
            RuntimeQueryProofFallbackReason::SchemaMismatch
        }
        RuntimeQueryReadBlockReason::MissingCoverage => {
            RuntimeQueryProofFallbackReason::MissingCoverage
        }
        RuntimeQueryReadBlockReason::StartNotCovered => {
            RuntimeQueryProofFallbackReason::StartNotCovered
        }
        RuntimeQueryReadBlockReason::PageBoundaryUnknown => {
            RuntimeQueryProofFallbackReason::PageBoundaryUnknown
        }
    }
}

#[must_use]
pub fn runtime_query_proof_read_plan(
    decision: RuntimeQueryReadDecision,
) -> RuntimeQueryProofReadPlan {
    RuntimeQueryProofReadPlan {
        would_serve_whole_page: decision.would_serve_whole_page,
        fallback_reason: decision
            .block_reason
            .map(runtime_query_proof_fallback_reason),
        cache_candidate_count: decision.cache_candidate_count,
        page_boundary_witnessed: decision.page_boundary_witnessed,
    }
}

#[must_use]
pub fn prepare_runtime_query_proof_page_plan(
    block_reason: Option<RuntimeQueryReadBlockReason>,
    matching_count: usize,
    witnessed_page_len: Option<usize>,
    limit: usize,
    request_exhausted: bool,
) -> RuntimePreparedQueryProofPagePlan {
    let decision = decide_query_read(
        block_reason,
        matching_count,
        witnessed_page_len,
        limit,
        request_exhausted,
    );
    let plan = runtime_query_proof_read_plan(decision);
    let page_shape = if block_reason.is_none() && matching_count > 0 {
        Some(materialized_page_shape(
            matching_count,
            limit,
            request_exhausted,
            decide_query_page_plan(matching_count, witnessed_page_len, limit, request_exhausted),
        ))
    } else {
        None
    };

    RuntimePreparedQueryProofPagePlan { plan, page_shape }
}

#[must_use]
pub fn blocked_runtime_query_proof_read<Key>(
    reason: RuntimeQueryReadBlockReason,
) -> RuntimePreparedQueryProofRead<Key> {
    RuntimePreparedQueryProofRead {
        plan: prepare_runtime_query_proof_page_plan(Some(reason), 0, None, 0, false).plan,
        materialized_page: None,
    }
}

#[must_use]
pub fn prepare_runtime_query_read<Item, Key>(
    query_proof_plan: &RuntimeQueryProofReadPlan,
    materialized_page: RuntimeQueryProofMaterializedPage<Key>,
    items: Vec<Item>,
    had_cache_miss: bool,
) -> RuntimePreparedQueryRead<Item> {
    if materialized_page.page_complete {
        let outcome = if had_cache_miss {
            RuntimePreparedQueryReadOutcome::HitPartial
        } else {
            RuntimePreparedQueryReadOutcome::Hit
        };
        return RuntimePreparedQueryRead::WholePage {
            items,
            last_evaluated_key: materialized_page.last_evaluated_key,
            outcome,
        };
    }

    if !query_proof_plan.would_serve_whole_page
        && !items.is_empty()
        && let Some(resume_token) = materialized_page.last_evaluated_key
    {
        return RuntimePreparedQueryRead::Prefix {
            items,
            resume_token,
        };
    }

    RuntimePreparedQueryRead::None
}

#[must_use]
pub fn plan_runtime_query_execution<Item>(
    prepared: RuntimePreparedQueryRead<Item>,
    requested_limit: Option<u32>,
) -> RuntimePreparedQueryExecution<Item> {
    match prepared {
        RuntimePreparedQueryRead::WholePage {
            items,
            last_evaluated_key,
            ..
        } => RuntimePreparedQueryExecution::WholePage {
            items,
            last_evaluated_key,
        },
        RuntimePreparedQueryRead::Prefix {
            items,
            resume_token,
        } => {
            let remaining_limit =
                requested_limit.map(|limit| limit.saturating_sub(items.len() as u32));
            if remaining_limit == Some(0) {
                RuntimePreparedQueryExecution::PrefixOnly { items }
            } else {
                RuntimePreparedQueryExecution::PrefixWithDbSuffix {
                    prefix_items: items,
                    resume_token,
                    remaining_limit,
                }
            }
        }
        RuntimePreparedQueryRead::None => RuntimePreparedQueryExecution::None,
    }
}

fn range_covers_start<T: Ord>(
    direction: RuntimeQueryDirection,
    range: &RuntimeCoverageRange<T>,
    start_exclusive: Option<&T>,
    lower_inclusive: Option<&T>,
    upper_inclusive: Option<&T>,
) -> bool {
    match direction {
        RuntimeQueryDirection::Forward => match (
            start_exclusive,
            range.start_after_exclusive.as_ref(),
            range.start_inclusive.as_ref(),
        ) {
            (Some(start), Some(range_after), Some(range_start)) => {
                range_after == start && range_start > start
            }
            (Some(_), _, None) => false,
            (Some(_), None, Some(_)) => false,
            (None, _, Some(range_start)) => {
                lower_inclusive.is_none_or(|lower| range_start <= lower)
                    && range.start_after_exclusive.is_none()
            }
            (None, _, None) => range.start_after_exclusive.is_none(),
        },
        RuntimeQueryDirection::Reverse => match (
            start_exclusive,
            range.start_after_exclusive.as_ref(),
            range.end_inclusive.as_ref(),
        ) {
            (Some(start), Some(range_after), Some(range_end)) => {
                range_after == start && range_end < start
            }
            (Some(_), _, None) => false,
            (Some(_), None, Some(_)) => false,
            (None, _, Some(range_end)) => {
                upper_inclusive.is_none_or(|upper| range_end >= upper)
                    && range.start_after_exclusive.is_none()
            }
            (None, _, None) => range.start_after_exclusive.is_none(),
        },
    }
}

fn order_key_matches_bounds<T: Ord>(
    sort_key: Option<&T>,
    start_exclusive: Option<&T>,
    lower_inclusive: Option<&T>,
    upper_inclusive: Option<&T>,
) -> bool {
    let Some(sort_key) = sort_key else {
        return true;
    };
    if let Some(start_exclusive) = start_exclusive
        && sort_key <= start_exclusive
    {
        return false;
    }
    if let Some(lower_inclusive) = lower_inclusive
        && sort_key < lower_inclusive
    {
        return false;
    }
    if let Some(upper_inclusive) = upper_inclusive
        && sort_key > upper_inclusive
    {
        return false;
    }
    true
}

fn range_contains_order_key<T: Ord>(range: &RuntimeCoverageRange<T>, sort_key: Option<&T>) -> bool {
    let Some(sort_key) = sort_key else {
        return range.start_inclusive.is_none() && range.end_inclusive.is_none();
    };
    range_contains_key(range, sort_key)
}

fn compare_range_coverage<T: Ord>(
    direction: RuntimeQueryDirection,
    left: &RuntimeCoverageRange<T>,
    right: &RuntimeCoverageRange<T>,
) -> Ordering {
    match direction {
        RuntimeQueryDirection::Forward => match (&left.end_inclusive, &right.end_inclusive) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(left_end), Some(right_end)) => left_end.cmp(right_end),
        },
        RuntimeQueryDirection::Reverse => match (&left.start_inclusive, &right.start_inclusive) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(left_start), Some(right_start)) => right_start.cmp(left_start),
        },
    }
}
