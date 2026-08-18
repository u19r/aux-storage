use crate::{
    ReadSequenceNode, ReadSequenceRequest, ReadSequenceValidationCapabilities,
    ReadSequenceValidationError,
    read_sequence_graph::{ReadSequenceGraphPlan, build_graph_plan},
};

/// The single validated representation shared by ordinary scheduling and
/// provider-specific whole-plan lowerings. The graph metadata alone is not
/// sufficient to execute a read: retaining the validated operations here
/// prevents a provider from reparsing the raw request or inventing a second
/// interpretation of the graph.
#[derive(Debug, Clone)]
pub struct ReadSequencePlan {
    pub nodes: Vec<ReadSequenceNode>,
    pub graph: ReadSequenceGraphPlan,
}

pub fn plan_read_sequence(
    request: &ReadSequenceRequest,
) -> Result<ReadSequencePlan, ReadSequenceValidationError> {
    plan_read_sequence_with_capabilities(request, ReadSequenceValidationCapabilities::default())
}

pub fn plan_read_sequence_with_capabilities(
    request: &ReadSequenceRequest,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<ReadSequencePlan, ReadSequenceValidationError> {
    request.validate_limits_and_capacity()?;
    Ok(ReadSequencePlan {
        nodes: request.nodes.clone(),
        graph: build_graph_plan(request, capabilities)?,
    })
}
