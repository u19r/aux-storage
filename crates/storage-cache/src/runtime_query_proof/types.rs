use crate::runtime_query_proof::runtime_query_coverage_semantics;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RuntimeQueryDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCoverageRange<T> {
    pub start_after_exclusive: Option<T>,
    pub start_inclusive: Option<T>,
    pub end_inclusive: Option<T>,
}

#[must_use]
pub fn borrow_runtime_coverage_ranges<T: AsRef<str>>(
    ranges: &[RuntimeCoverageRange<T>],
) -> Vec<RuntimeCoverageRange<&str>> {
    ranges
        .iter()
        .map(|range| RuntimeCoverageRange {
            start_after_exclusive: range.start_after_exclusive.as_ref().map(AsRef::as_ref),
            start_inclusive: range.start_inclusive.as_ref().map(AsRef::as_ref),
            end_inclusive: range.end_inclusive.as_ref().map(AsRef::as_ref),
        })
        .collect()
}

pub fn push_unique_runtime_coverage_range<T: PartialEq>(
    ranges: &mut Vec<RuntimeCoverageRange<T>>,
    next: RuntimeCoverageRange<T>,
) {
    if ranges.iter().any(|range| range == &next) {
        return;
    }
    ranges.push(next);
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RuntimePageWitnessKey<T> {
    pub direction: RuntimeQueryDirection,
    pub start_exclusive: Option<T>,
    pub lower_inclusive: Option<T>,
    pub upper_inclusive: Option<T>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimePageWitness<T> {
    pub key: RuntimePageWitnessKey<T>,
    pub returned_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeQueryBounds<T> {
    pub direction: RuntimeQueryDirection,
    pub start_exclusive: Option<T>,
    pub lower_inclusive: Option<T>,
    pub upper_inclusive: Option<T>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOwnedQueryBounds<T> {
    pub direction: RuntimeQueryDirection,
    pub start_exclusive: Option<T>,
    pub lower_inclusive: Option<T>,
    pub upper_inclusive: Option<T>,
}

impl<T> RuntimeOwnedQueryBounds<T> {
    pub fn as_borrowed(&self) -> RuntimeQueryBounds<&T> {
        RuntimeQueryBounds {
            direction: self.direction,
            start_exclusive: self.start_exclusive.as_ref(),
            lower_inclusive: self.lower_inclusive.as_ref(),
            upper_inclusive: self.upper_inclusive.as_ref(),
        }
    }
}

impl<T: AsRef<str>> RuntimeOwnedQueryBounds<T> {
    pub fn as_str_bounds(&self) -> RuntimeQueryBounds<&str> {
        RuntimeQueryBounds {
            direction: self.direction,
            start_exclusive: self.start_exclusive.as_ref().map(AsRef::as_ref),
            lower_inclusive: self.lower_inclusive.as_ref().map(AsRef::as_ref),
            upper_inclusive: self.upper_inclusive.as_ref().map(AsRef::as_ref),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeQueryPagePlan {
    pub would_serve_whole_page: bool,
    pub cache_candidate_count: usize,
    pub page_boundary_witnessed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeMaterializedPageShape {
    pub returned_count: usize,
    pub has_more: bool,
    pub needs_resume_token: bool,
    pub page_complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeQueryReadBlockReason {
    CacheDisabled,
    StrongReadBypass,
    UnsupportedKeyCondition,
    MissingPartition,
    ContinuityBroken,
    Rebuilding,
    SchemaMismatch,
    MissingCoverage,
    StartNotCovered,
    PageBoundaryUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeQueryReadDecision {
    pub would_serve_whole_page: bool,
    pub block_reason: Option<RuntimeQueryReadBlockReason>,
    pub cache_candidate_count: usize,
    pub page_boundary_witnessed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeQueryProofFallbackReason {
    CacheDisabled,
    StrongReadBypass,
    MissingPartition,
    UnsupportedKeyCondition,
    UnsupportedStartKey,
    ReverseQueryUnsupported,
    ContinuityBroken,
    Rebuilding,
    SchemaMismatch,
    MissingCoverage,
    StartNotCovered,
    PageBoundaryUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeQueryProofReadPlan {
    pub would_serve_whole_page: bool,
    pub fallback_reason: Option<RuntimeQueryProofFallbackReason>,
    pub cache_candidate_count: usize,
    pub page_boundary_witnessed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimePreparedQueryProofRead<Key> {
    pub plan: RuntimeQueryProofReadPlan,
    pub materialized_page: Option<RuntimeQueryProofMaterializedPage<Key>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimePreparedQueryWindow {
    pub covered_range_indexes: Vec<usize>,
    pub matching_indexes: Vec<usize>,
    pub witnessed_page_len: Option<usize>,
    pub request_exhausted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimePreparedQueryProofPagePlan {
    pub plan: RuntimeQueryProofReadPlan,
    pub page_shape: Option<RuntimeMaterializedPageShape>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimePageCoverageContext {
    pub starts_at_partition_start: bool,
    pub starts_at_partition_end: bool,
    pub exhaustive_start_is_partition_start: bool,
    pub exhaustive_end_is_partition_end: bool,
    pub has_more: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeQueryCoverageSemantics {
    pub coverage_supported: bool,
    pub starts_at_partition_start: bool,
    pub starts_at_partition_end: bool,
    pub exhaustive_start_is_partition_start: bool,
    pub exhaustive_end_is_partition_end: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeParsedQueryShape<T> {
    pub has_sort_key: bool,
    pub bounds: RuntimeOwnedQueryBounds<T>,
    pub coverage_semantics: RuntimeQueryCoverageSemantics,
    pub limit: Option<usize>,
}

impl<T> RuntimeParsedQueryShape<T> {
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(usize::MAX)
    }

    #[must_use]
    pub fn limit_option(&self) -> Option<usize> {
        self.limit
    }
}

impl<T: AsRef<str>> RuntimeParsedQueryShape<T> {
    #[must_use]
    pub fn runtime_bounds(&self) -> RuntimeQueryBounds<&str> {
        self.bounds.as_str_bounds()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeSortCondition<T> {
    Equal { value: T },
    GreaterThan,
    GreaterThanEqual { value: T },
    LessThan,
    LessThanEqual { value: T },
    Between { min: T, max: T },
    BeginsWith,
}

impl<T> RuntimeSortCondition<T> {
    pub fn lower_inclusive(&self) -> Option<&T> {
        match self {
            Self::Equal { value }
            | Self::GreaterThanEqual { value }
            | Self::Between { min: value, .. } => Some(value),
            Self::GreaterThan | Self::LessThan | Self::LessThanEqual { .. } | Self::BeginsWith => {
                None
            }
        }
    }

    pub fn upper_inclusive(&self) -> Option<&T> {
        match self {
            Self::Equal { value }
            | Self::LessThanEqual { value }
            | Self::Between { max: value, .. } => Some(value),
            Self::GreaterThan
            | Self::GreaterThanEqual { .. }
            | Self::LessThan
            | Self::BeginsWith => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeQueryProofMaterializedPage<Key> {
    pub primary_keys: Vec<Key>,
    pub last_evaluated_key: Option<String>,
    pub page_complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimePreparedQueryReadOutcome {
    Hit,
    HitPartial,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimePreparedQueryRead<Item> {
    WholePage {
        items: Vec<Item>,
        last_evaluated_key: Option<String>,
        outcome: RuntimePreparedQueryReadOutcome,
    },
    Prefix {
        items: Vec<Item>,
        resume_token: String,
    },
    None,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimePreparedQueryExecution<Item> {
    WholePage {
        items: Vec<Item>,
        last_evaluated_key: Option<String>,
    },
    PrefixWithDbSuffix {
        prefix_items: Vec<Item>,
        resume_token: String,
        remaining_limit: Option<u32>,
    },
    PrefixOnly {
        items: Vec<Item>,
        last_evaluated_key: String,
    },
    None,
}

#[must_use]
pub fn prepare_runtime_query_shape<T: Clone>(
    has_sort_key: bool,
    start_exclusive: Option<T>,
    sort_condition: Option<&RuntimeSortCondition<T>>,
    direction: RuntimeQueryDirection,
    limit: Option<usize>,
) -> RuntimeParsedQueryShape<T> {
    RuntimeParsedQueryShape {
        has_sort_key,
        bounds: RuntimeOwnedQueryBounds {
            direction,
            start_exclusive,
            lower_inclusive: sort_condition
                .and_then(RuntimeSortCondition::lower_inclusive)
                .cloned(),
            upper_inclusive: sort_condition
                .and_then(RuntimeSortCondition::upper_inclusive)
                .cloned(),
        },
        coverage_semantics: runtime_query_coverage_semantics(sort_condition),
        limit,
    }
}
