use serde::{Deserialize, Serialize};
use storage_types::{ReadSequenceConsistency, ReadSequenceNodeId, ReadSequencePlan};

/// Physical operation shape proved by a backend-specific key codec.
///
/// Mapped lowering accepts the two directional pairs supported by
/// FoundationDB's `get_mapped_range`: a partition range feeding a point item
/// (`Query -> Get`) and an exact point item feeding a partition range
/// (`Get -> Query`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadSequencePhysicalOperation {
    Point,
    PrefixRange,
    Other,
}

/// The small amount of physical metadata required before a mapped edge can
/// be selected.  Callers must obtain this from the owning backend codec; the
/// public request and selector are never interpreted as physical bytes here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSequencePhysicalDescriptor {
    pub operation: ReadSequencePhysicalOperation,
    pub tuple_schema: bool,
    /// The physical key and mapper have the same Tuple type/null layout.
    pub tuple_types_match: bool,
    /// The owning store can express its tenant/subspace prefix in the mapper.
    pub tuple_prefix_safe: bool,
    pub selector_physical: bool,
    pub unsupported_projection: bool,
    pub secondary_limit_safe: bool,
    pub continuation_safe: bool,
    pub read_your_writes: bool,
    pub estimated_miss_cost_high: bool,
    pub latency_benefit: bool,
    pub expected_saved_waves: u16,
    pub expected_saved_requests: u16,
}

impl Default for ReadSequencePhysicalDescriptor {
    fn default() -> Self {
        Self {
            operation: ReadSequencePhysicalOperation::Other,
            tuple_schema: false,
            tuple_types_match: true,
            tuple_prefix_safe: false,
            selector_physical: false,
            unsupported_projection: false,
            secondary_limit_safe: true,
            continuation_safe: true,
            read_your_writes: false,
            estimated_miss_cost_high: false,
            latency_benefit: true,
            expected_saved_waves: 1,
            expected_saved_requests: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadSequenceMappedRejectionReason {
    NotFoundationdb,
    ApiVersion,
    Disabled,
    SourceNotRange,
    ChildOperation,
    MultipleDataParents,
    SelectorNotPhysical,
    NonTupleSource,
    NonTupleTarget,
    TupleTypeMismatch,
    ProjectionSemantics,
    SecondaryLimit,
    Continuation,
    Consistency,
    ReadYourWrites,
    EstimatedMissCost,
    NoLatencyBenefit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSequenceMappedEdgeAssessment {
    pub parent: ReadSequenceNodeId,
    pub child: ReadSequenceNodeId,
    pub input_name: String,
    pub reason: Option<ReadSequenceMappedRejectionReason>,
    pub saved_waves: u16,
    pub saved_requests: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSequenceMappedEdge {
    pub parent: ReadSequenceNodeId,
    pub child: ReadSequenceNodeId,
    pub input_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSequenceMappedSelection {
    pub assessments: Vec<ReadSequenceMappedEdgeAssessment>,
    pub selected: Vec<ReadSequenceMappedEdge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSequenceMappedOptions {
    pub foundationdb: bool,
    pub api_version: u32,
    pub enabled: bool,
    pub consistency: ReadSequenceConsistency,
}

struct MappingContext<'a> {
    plan: &'a ReadSequencePlan,
    descriptors: &'a [(ReadSequenceNodeId, ReadSequencePhysicalDescriptor)],
    options: ReadSequenceMappedOptions,
}

impl Default for ReadSequenceMappedOptions {
    fn default() -> Self {
        Self {
            foundationdb: false,
            api_version: 0,
            enabled: false,
            consistency: ReadSequenceConsistency::Eventual,
        }
    }
}

/// Inspect every direct input edge and choose the maximal deterministic set of
/// non-overlapping mapped operations.  The greedy order is bounded by the
/// graph limit and is an exact maximality proof under the stated ownership
/// rule: a parent may occur in at most one selected edge and a selected child
/// cannot itself become another mapped parent in this call.
pub fn select_read_sequence_mapped_edges(
    plan: &ReadSequencePlan,
    descriptors: &[(ReadSequenceNodeId, ReadSequencePhysicalDescriptor)],
    options: ReadSequenceMappedOptions,
) -> ReadSequenceMappedSelection {
    let context = MappingContext {
        plan,
        descriptors,
        options,
    };
    let assessments = assess_mapped_edges(&context);
    let selected = select_mapped_edges(&assessments);
    ReadSequenceMappedSelection {
        assessments,
        selected,
    }
}

fn assess_mapped_edges(context: &MappingContext<'_>) -> Vec<ReadSequenceMappedEdgeAssessment> {
    context
        .plan
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(child_index, child)| {
            let child_id = ReadSequenceNodeId::from_index(child_index);
            child.inputs().iter().map(move |(input_name, input)| {
                assess_mapped_edge(context, child_id, input_name, input)
            })
        })
        .collect()
}

fn assess_mapped_edge(
    context: &MappingContext<'_>,
    child_id: ReadSequenceNodeId,
    input_name: &str,
    input: &storage_types::ReadSequenceNodeInput,
) -> ReadSequenceMappedEdgeAssessment {
    let Some(parent_id) = context
        .plan
        .graph
        .node_names
        .iter()
        .position(|name| name == &input.from.node)
        .map(ReadSequenceNodeId::from_index)
    else {
        return rejected(
            child_id,
            child_id,
            input_name,
            ReadSequenceMappedRejectionReason::MultipleDataParents,
        );
    };
    let parent = descriptor(context.descriptors, parent_id);
    let child = descriptor(context.descriptors, child_id);
    let (saved_waves, saved_requests) = mapped_savings(parent, child);
    ReadSequenceMappedEdgeAssessment {
        parent: parent_id,
        child: child_id,
        input_name: input_name.to_string(),
        reason: eligibility_reason(context, child_id, parent, child),
        saved_waves,
        saved_requests,
    }
}

fn descriptor(
    descriptors: &[(ReadSequenceNodeId, ReadSequencePhysicalDescriptor)],
    node: ReadSequenceNodeId,
) -> Option<&ReadSequencePhysicalDescriptor> {
    descriptors
        .iter()
        .find_map(|(candidate, descriptor)| (*candidate == node).then_some(descriptor))
}

fn mapped_savings(
    parent: Option<&ReadSequencePhysicalDescriptor>,
    child: Option<&ReadSequencePhysicalDescriptor>,
) -> (u16, u16) {
    child
        .zip(parent)
        .map(|(child, parent)| {
            (
                parent.expected_saved_waves.max(child.expected_saved_waves),
                parent
                    .expected_saved_requests
                    .max(child.expected_saved_requests),
            )
        })
        .unwrap_or((0, 0))
}

fn select_mapped_edges(
    assessments: &[ReadSequenceMappedEdgeAssessment],
) -> Vec<ReadSequenceMappedEdge> {
    let mut ranked = assessments
        .iter()
        .filter(|assessment| assessment.reason.is_none())
        .collect::<Vec<_>>();
    ranked.sort_by(compare_assessments);
    let mut selected = Vec::new();
    let mut used_nodes = 0u16;
    for assessment in ranked {
        let edge_nodes = node_bit(assessment.parent) | node_bit(assessment.child);
        if used_nodes & edge_nodes != 0 {
            continue;
        }
        used_nodes |= edge_nodes;
        selected.push(ReadSequenceMappedEdge {
            parent: assessment.parent,
            child: assessment.child,
            input_name: assessment.input_name.clone(),
        });
    }
    selected.sort_by(|left, right| {
        left.parent
            .cmp(&right.parent)
            .then_with(|| left.child.cmp(&right.child))
            .then_with(|| left.input_name.cmp(&right.input_name))
    });
    selected
}

fn node_bit(node: ReadSequenceNodeId) -> u16 {
    1u16 << node.index()
}

fn compare_assessments(
    left: &&ReadSequenceMappedEdgeAssessment,
    right: &&ReadSequenceMappedEdgeAssessment,
) -> std::cmp::Ordering {
    right
        .saved_waves
        .cmp(&left.saved_waves)
        .then_with(|| right.saved_requests.cmp(&left.saved_requests))
        .then_with(|| left.parent.cmp(&right.parent))
        .then_with(|| left.child.cmp(&right.child))
        .then_with(|| left.input_name.cmp(&right.input_name))
}

fn eligibility_reason(
    context: &MappingContext<'_>,
    child_id: ReadSequenceNodeId,
    parent: Option<&ReadSequencePhysicalDescriptor>,
    child: Option<&ReadSequencePhysicalDescriptor>,
) -> Option<ReadSequenceMappedRejectionReason> {
    capability_reason(context.options)
        .or_else(|| descriptor_presence_reason(parent, child))
        .or_else(|| operation_reason(parent, child))
        .or_else(|| tuple_reason(parent, child))
        .or_else(|| semantic_reason(parent, child))
        .or_else(|| data_parent_reason(context.plan, child_id))
}

fn capability_reason(
    options: ReadSequenceMappedOptions,
) -> Option<ReadSequenceMappedRejectionReason> {
    if !options.foundationdb {
        return Some(ReadSequenceMappedRejectionReason::NotFoundationdb);
    }
    if options.api_version < 710 {
        return Some(ReadSequenceMappedRejectionReason::ApiVersion);
    }
    if !options.enabled {
        return Some(ReadSequenceMappedRejectionReason::Disabled);
    }
    (options.consistency != ReadSequenceConsistency::Eventual)
        .then_some(ReadSequenceMappedRejectionReason::Consistency)
}

fn descriptor_presence_reason(
    parent: Option<&ReadSequencePhysicalDescriptor>,
    child: Option<&ReadSequencePhysicalDescriptor>,
) -> Option<ReadSequenceMappedRejectionReason> {
    if parent.is_none() {
        return Some(ReadSequenceMappedRejectionReason::NonTupleSource);
    }
    child
        .is_none()
        .then_some(ReadSequenceMappedRejectionReason::NonTupleTarget)
}

fn tuple_reason(
    parent: Option<&ReadSequencePhysicalDescriptor>,
    child: Option<&ReadSequencePhysicalDescriptor>,
) -> Option<ReadSequenceMappedRejectionReason> {
    let (Some(parent), Some(child)) = (parent, child) else {
        return None;
    };
    if !parent.tuple_schema {
        return Some(ReadSequenceMappedRejectionReason::NonTupleSource);
    }
    if !child.tuple_schema {
        return Some(ReadSequenceMappedRejectionReason::NonTupleTarget);
    }
    if !parent.tuple_types_match || !child.tuple_types_match {
        return Some(ReadSequenceMappedRejectionReason::TupleTypeMismatch);
    }
    if !parent.tuple_prefix_safe {
        return Some(ReadSequenceMappedRejectionReason::NonTupleSource);
    }
    if !child.tuple_prefix_safe {
        return Some(ReadSequenceMappedRejectionReason::NonTupleTarget);
    }
    None
}

fn operation_reason(
    parent: Option<&ReadSequencePhysicalDescriptor>,
    child: Option<&ReadSequencePhysicalDescriptor>,
) -> Option<ReadSequenceMappedRejectionReason> {
    let (Some(parent), Some(child)) = (parent, child) else {
        return None;
    };
    match (parent.operation, child.operation) {
        (ReadSequencePhysicalOperation::PrefixRange, ReadSequencePhysicalOperation::Point)
        | (ReadSequencePhysicalOperation::Point, ReadSequencePhysicalOperation::PrefixRange) => {
            None
        }
        (ReadSequencePhysicalOperation::PrefixRange, _)
        | (ReadSequencePhysicalOperation::Point, _) => {
            Some(ReadSequenceMappedRejectionReason::ChildOperation)
        }
        (ReadSequencePhysicalOperation::Other, _) => {
            Some(ReadSequenceMappedRejectionReason::SourceNotRange)
        }
    }
}

fn semantic_reason(
    parent: Option<&ReadSequencePhysicalDescriptor>,
    child: Option<&ReadSequencePhysicalDescriptor>,
) -> Option<ReadSequenceMappedRejectionReason> {
    let (Some(parent), Some(child)) = (parent, child) else {
        return None;
    };
    if !parent.selector_physical || !child.selector_physical {
        return Some(ReadSequenceMappedRejectionReason::SelectorNotPhysical);
    }
    if parent.unsupported_projection || child.unsupported_projection {
        return Some(ReadSequenceMappedRejectionReason::ProjectionSemantics);
    }
    if !parent.secondary_limit_safe || !child.secondary_limit_safe {
        return Some(ReadSequenceMappedRejectionReason::SecondaryLimit);
    }
    if !parent.continuation_safe || !child.continuation_safe {
        return Some(ReadSequenceMappedRejectionReason::Continuation);
    }
    if parent.read_your_writes || child.read_your_writes {
        return Some(ReadSequenceMappedRejectionReason::ReadYourWrites);
    }
    if parent.estimated_miss_cost_high || child.estimated_miss_cost_high {
        return Some(ReadSequenceMappedRejectionReason::EstimatedMissCost);
    }
    if !parent.latency_benefit || !child.latency_benefit {
        return Some(ReadSequenceMappedRejectionReason::NoLatencyBenefit);
    }
    None
}

fn data_parent_reason(
    plan: &ReadSequencePlan,
    child_id: ReadSequenceNodeId,
) -> Option<ReadSequenceMappedRejectionReason> {
    let mut data_parents = plan.nodes[child_id.index()]
        .inputs()
        .values()
        .map(|input| input.from.node.as_str());
    let first = data_parents.next();
    data_parents
        .any(|parent| Some(parent) != first)
        .then_some(ReadSequenceMappedRejectionReason::MultipleDataParents)
}

fn rejected(
    node: ReadSequenceNodeId,
    child: ReadSequenceNodeId,
    input_name: &str,
    reason: ReadSequenceMappedRejectionReason,
) -> ReadSequenceMappedEdgeAssessment {
    ReadSequenceMappedEdgeAssessment {
        parent: node,
        child,
        input_name: input_name.to_string(),
        reason: Some(reason),
        saved_waves: 0,
        saved_requests: 0,
    }
}

#[cfg(test)]
mod read_sequence_mapping_tests;
