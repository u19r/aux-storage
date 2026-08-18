use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use utoipa::ToSchema;

use crate::{
    BatchGetItemRequest, GetItemRequest, QueryRequest,
    READ_SEQUENCE_DEFAULT_MAX_SELECTOR_BINDINGS_PER_STEP,
    READ_SEQUENCE_DEFAULT_MAX_SELECTOR_PATH_DEPTH, READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS,
    READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS, ReadSequenceConsistency, ReadSequenceRequest,
    ReadSequenceSelector, ReadSequenceValidationCapabilities, ReadSequenceValidationError,
    read_sequence_selector::validate_selector_depth,
};

mod markers;
mod validation;

pub use markers::{
    ReadSequenceStringTemplateError, ReadSequenceStringTemplatePart,
    ReadSequenceStringTemplateParts, read_sequence_input_literal, read_sequence_input_literal_name,
    read_sequence_input_marker, read_sequence_input_marker_name,
    read_sequence_operation_contains_literal_escape, read_sequence_string_template,
    read_sequence_string_template_name,
};
use validation::{
    build_adjacency, graph_digest, resolve_outputs, topological_schedule, validate_node_name,
    validate_operation, validate_operation_consistency, validate_operation_inputs,
    validate_output_closure,
};

static EMPTY_NODE_INPUTS: BTreeMap<String, ReadSequenceNodeInput> = BTreeMap::new();

/// An ordinal assigned from the request's node array.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(transparent)]
pub struct ReadSequenceNodeId(usize);

impl ReadSequenceNodeId {
    #[must_use]
    pub const fn from_index(index: usize) -> Self {
        Self(index)
    }

    #[must_use]
    pub const fn index(self) -> usize {
        self.0
    }
}

/// A graph node with one DynamoDB-shaped read operation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, TypedBuilder)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
#[builder(field_defaults(default))]
pub struct ReadSequenceNode {
    #[builder(!default, setter(into))]
    pub name: String,
    #[builder(!default)]
    pub operation: ReadSequenceNodeOperation,
    #[serde(default, skip_serializing_if = "node_inputs_are_empty")]
    #[builder(setter(strip_option))]
    pub inputs: Option<BTreeMap<String, ReadSequenceNodeInput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, into))]
    pub iterate: Option<String>,
    #[serde(default, skip_serializing_if = "node_after_is_empty")]
    #[builder(setter(strip_option))]
    pub after: Option<Vec<String>>,
}

fn node_inputs_are_empty(inputs: &Option<BTreeMap<String, ReadSequenceNodeInput>>) -> bool {
    inputs.as_ref().is_none_or(BTreeMap::is_empty)
}

fn node_after_is_empty(after: &Option<Vec<String>>) -> bool {
    after.as_ref().is_none_or(Vec::is_empty)
}

impl ReadSequenceNode {
    #[must_use]
    pub fn new(name: impl Into<String>, operation: ReadSequenceNodeOperation) -> Self {
        Self {
            name: name.into(),
            operation,
            inputs: None,
            iterate: None,
            after: None,
        }
    }

    #[must_use]
    pub fn inputs(&self) -> &BTreeMap<String, ReadSequenceNodeInput> {
        self.inputs.as_ref().unwrap_or(&EMPTY_NODE_INPUTS)
    }

    pub fn inputs_mut(&mut self) -> &mut BTreeMap<String, ReadSequenceNodeInput> {
        self.inputs.get_or_insert_default()
    }

    #[must_use]
    pub fn after(&self) -> &[String] {
        self.after.as_deref().unwrap_or_default()
    }
}

#[derive(Debug, Clone, ToSchema)]
pub enum ReadSequenceNodeOperation {
    Get(GetItemRequest),
    BatchGet(BatchGetItemRequest),
    Query(QueryRequest),
}

impl ReadSequenceNodeOperation {
    pub(crate) fn reads_gsi(&self) -> bool {
        matches!(self, Self::Query(query) if query.index_name.is_some())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceNodeInput {
    pub from: ReadSequenceFromInput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapped_key_source: Option<ReadSequenceMappedKeySource>,
    #[serde(default)]
    pub cardinality: ReadSequenceInputCardinality,
    #[serde(default)]
    pub on_missing: crate::ReadSequenceOnMissing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceMappedKeySource {
    attribute_name: String,
    indexer: u8,
}

impl ReadSequenceMappedKeySource {
    #[must_use]
    pub fn new(attribute_name: impl Into<String>, indexer: u8) -> Self {
        Self {
            attribute_name: attribute_name.into(),
            indexer,
        }
    }

    #[must_use]
    pub fn attribute_name(&self) -> &str {
        &self.attribute_name
    }

    #[must_use]
    pub const fn indexer(&self) -> u8 {
        self.indexer
    }
}

impl From<crate::single_table_entity::EntityIndexer> for ReadSequenceMappedKeySource {
    fn from(indexer: crate::single_table_entity::EntityIndexer) -> Self {
        Self::new(indexer.attribute_name(), indexer.ordinal())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceFromInput {
    pub node: String,
    pub select: ReadSequenceSelector,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReadSequenceInputCardinality {
    #[default]
    One,
    Many,
}

/// Immutable graph metadata consumed by schedulers and backend lowerings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSequenceGraphPlan {
    pub node_names: Vec<String>,
    pub topological_order: Vec<ReadSequenceNodeId>,
    pub waves: Vec<Vec<ReadSequenceNodeId>>,
    pub dependencies: Vec<Vec<ReadSequenceNodeId>>,
    pub consumers: Vec<Vec<ReadSequenceNodeId>>,
    pub outputs: Vec<ReadSequenceNodeId>,
    pub structural_digest: String,
}

impl ReadSequenceGraphPlan {
    #[must_use]
    pub fn node_name(&self, node: ReadSequenceNodeId) -> Option<&str> {
        self.node_names.get(node.0).map(String::as_str)
    }
}

pub(crate) fn build_graph_plan(
    request: &ReadSequenceRequest,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<ReadSequenceGraphPlan, ReadSequenceValidationError> {
    let nodes = &request.nodes;
    let requested_outputs = request.outputs.as_deref();
    let max_steps = request
        .max_sequence_steps
        .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS);
    if nodes.is_empty() {
        return Err(ReadSequenceValidationError::EmptySequence);
    }
    if nodes.len() > max_steps as usize || max_steps > READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS {
        return Err(ReadSequenceValidationError::NodeLimitExceeded {
            actual: nodes.len(),
            limit: max_steps.min(READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS),
        });
    }

    let names = collect_node_names(nodes)?;
    validate_node_shapes(request, nodes, &names, capabilities)?;
    let outputs = resolve_outputs(nodes, requested_outputs, &names)?;
    let (dependencies, consumers) = build_adjacency(nodes, &names)?;
    validate_output_closure(&dependencies, &outputs, nodes)?;
    let (topological_order, waves) = topological_schedule(&dependencies, &consumers, nodes)?;
    let structural_digest = graph_digest(nodes, requested_outputs);

    Ok(ReadSequenceGraphPlan {
        node_names: nodes.iter().map(|node| node.name.clone()).collect(),
        topological_order,
        waves,
        dependencies,
        consumers,
        outputs,
        structural_digest,
    })
}

fn collect_node_names(
    nodes: &[ReadSequenceNode],
) -> Result<BTreeMap<String, ReadSequenceNodeId>, ReadSequenceValidationError> {
    let mut names = BTreeMap::new();
    for (ordinal, node) in nodes.iter().enumerate() {
        validate_node_name(&node.name)?;
        if names
            .insert(node.name.clone(), ReadSequenceNodeId(ordinal))
            .is_some()
        {
            return Err(ReadSequenceValidationError::DuplicateNodeName {
                name: node.name.clone(),
            });
        }
    }
    Ok(names)
}

fn validate_node_shapes(
    request: &ReadSequenceRequest,
    nodes: &[ReadSequenceNode],
    names: &BTreeMap<String, ReadSequenceNodeId>,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<(), ReadSequenceValidationError> {
    let limits = NodeShapeLimits {
        binding_limit: request
            .max_selector_bindings_per_step
            .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SELECTOR_BINDINGS_PER_STEP),
        selector_depth: request
            .max_selector_path_depth
            .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SELECTOR_PATH_DEPTH),
        max_child_query_items: request
            .max_child_query_items_per_parent
            .unwrap_or(crate::READ_SEQUENCE_DEFAULT_MAX_CHILD_QUERY_ITEMS_PER_PARENT),
        consistency: request.read_consistency,
        capabilities,
    };
    for node in nodes {
        validate_node_shape(node, nodes, names, limits)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct NodeShapeLimits {
    binding_limit: u32,
    selector_depth: u32,
    max_child_query_items: u32,
    consistency: ReadSequenceConsistency,
    capabilities: ReadSequenceValidationCapabilities,
}

fn validate_node_shape(
    node: &ReadSequenceNode,
    nodes: &[ReadSequenceNode],
    names: &BTreeMap<String, ReadSequenceNodeId>,
    limits: NodeShapeLimits,
) -> Result<(), ReadSequenceValidationError> {
    validate_input_declarations(node, limits.binding_limit)?;
    validate_input_bindings(node, nodes, names, limits.selector_depth)?;
    validate_operation(node)?;
    validate_operation_inputs(node)?;
    validate_iterate_cardinality(node)?;
    validate_after_dependencies(node, names)?;
    validate_operation_consistency(
        limits.consistency,
        node.operation.reads_gsi(),
        limits.capabilities,
    )?;
    validate_child_query_limits(node, limits.max_child_query_items)
}

fn validate_input_declarations(
    node: &ReadSequenceNode,
    binding_limit: u32,
) -> Result<(), ReadSequenceValidationError> {
    if node.inputs().len() > binding_limit as usize {
        return Err(ReadSequenceValidationError::SelectorBindingLimitExceeded {
            node: node.name.clone(),
            actual: node.inputs().len(),
            limit: binding_limit,
        });
    }
    if node.iterate.is_some()
        && !node
            .inputs()
            .contains_key(node.iterate.as_deref().unwrap_or_default())
    {
        return Err(ReadSequenceValidationError::UnknownInput {
            node: node.name.clone(),
            input: node.iterate.clone().unwrap_or_default(),
        });
    }
    let many_inputs = node
        .inputs()
        .values()
        .filter(|input| input.cardinality == ReadSequenceInputCardinality::Many)
        .count();
    if many_inputs > 1 {
        return Err(ReadSequenceValidationError::MultipleIterationInputs {
            node: node.name.clone(),
        });
    }
    Ok(())
}

fn validate_input_bindings(
    node: &ReadSequenceNode,
    nodes: &[ReadSequenceNode],
    names: &BTreeMap<String, ReadSequenceNodeId>,
    selector_depth: u32,
) -> Result<(), ReadSequenceValidationError> {
    for (input_name, input) in node.inputs() {
        validate_node_name(input_name).map_err(|_| ReadSequenceValidationError::UnknownInput {
            node: node.name.clone(),
            input: input_name.clone(),
        })?;
        let Some(source_id) = names.get(&input.from.node).copied() else {
            return Err(ReadSequenceValidationError::UnknownNode {
                node: node.name.clone(),
                referenced: input.from.node.clone(),
            });
        };
        let parsed_selector = validate_selector_depth(&input.from.select, selector_depth)?;
        validate_selector_shape(
            &parsed_selector,
            &nodes[source_id.index()].operation,
            &input.from.select,
        )?;
        validate_input_cardinality(node, input_name, input)?;
        if let Some(source) = &input.mapped_key_source
            && (source.attribute_name.is_empty()
                || usize::from(source.indexer) >= usize::from(crate::MAX_INDEXERS_CAPACITY))
        {
            return Err(ReadSequenceValidationError::InvalidOperation {
                node: node.name.clone(),
                message: "MappedKeySource is invalid".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_input_cardinality(
    node: &ReadSequenceNode,
    input_name: &str,
    input: &ReadSequenceNodeInput,
) -> Result<(), ReadSequenceValidationError> {
    let invalid = input.cardinality == ReadSequenceInputCardinality::Many
        && (node.iterate.as_deref() != Some(input_name)
            || input.on_missing == crate::ReadSequenceOnMissing::Null);
    if invalid {
        return Err(ReadSequenceValidationError::InputCardinality {
            node: node.name.clone(),
            input: input_name.to_string(),
        });
    }
    Ok(())
}

fn validate_iterate_cardinality(
    node: &ReadSequenceNode,
) -> Result<(), ReadSequenceValidationError> {
    let Some(iterate) = &node.iterate else {
        return Ok(());
    };
    if node
        .inputs()
        .get(iterate)
        .is_some_and(|input| input.cardinality != ReadSequenceInputCardinality::Many)
    {
        return Err(ReadSequenceValidationError::InputCardinality {
            node: node.name.clone(),
            input: iterate.clone(),
        });
    }
    Ok(())
}

fn validate_after_dependencies(
    node: &ReadSequenceNode,
    names: &BTreeMap<String, ReadSequenceNodeId>,
) -> Result<(), ReadSequenceValidationError> {
    for dependency in node.after() {
        if !names.contains_key(dependency) {
            return Err(ReadSequenceValidationError::UnknownNode {
                node: node.name.clone(),
                referenced: dependency.clone(),
            });
        }
    }
    Ok(())
}

fn validate_child_query_limits(
    node: &ReadSequenceNode,
    max_child_query_items: u32,
) -> Result<(), ReadSequenceValidationError> {
    let ReadSequenceNodeOperation::Query(query) = &node.operation else {
        return Ok(());
    };
    if node.iterate.is_none() {
        return Ok(());
    }
    if query.limit.is_none() {
        return Err(ReadSequenceValidationError::ChildQueryLimitRequired {
            node: node.name.clone(),
        });
    }
    if query
        .limit
        .is_some_and(|limit| limit > max_child_query_items)
    {
        return Err(ReadSequenceValidationError::HardLimitExceeded {
            limit_name: "MaxChildQueryItemsPerParent",
            actual: query.limit.unwrap_or_default(),
            limit: max_child_query_items,
        });
    }
    Ok(())
}

fn validate_selector_shape(
    selector: &crate::ParsedReadSequenceSelector,
    operation: &ReadSequenceNodeOperation,
    raw: &ReadSequenceSelector,
) -> Result<(), ReadSequenceValidationError> {
    let segments = selector.segments();
    validate_selector_operation(segments.first(), operation, raw)?;
    let Some(crate::ReadSequenceSelectorSegment::Attribute(collection_name)) = segments.get(1)
    else {
        return Err(ReadSequenceValidationError::SelectorFailure {
            selector: raw.0.clone(),
        });
    };
    match operation {
        ReadSequenceNodeOperation::Get(_) if collection_name == "Item" => Ok(()),
        ReadSequenceNodeOperation::BatchGet(_) | ReadSequenceNodeOperation::Query(_)
            if collection_name == "Items" =>
        {
            match segments.get(2) {
                Some(crate::ReadSequenceSelectorSegment::Wildcard)
                | Some(crate::ReadSequenceSelectorSegment::Index(_))
                | None => Ok(()),
                _ => Err(ReadSequenceValidationError::SelectorFailure {
                    selector: raw.0.clone(),
                }),
            }
        }
        _ => Err(ReadSequenceValidationError::SelectorFailure {
            selector: raw.0.clone(),
        }),
    }
}

fn validate_selector_operation(
    segment: Option<&crate::ReadSequenceSelectorSegment>,
    operation: &ReadSequenceNodeOperation,
    raw: &ReadSequenceSelector,
) -> Result<(), ReadSequenceValidationError> {
    let expected = match operation {
        ReadSequenceNodeOperation::Get(_) => "Get",
        ReadSequenceNodeOperation::BatchGet(_) => "BatchGet",
        ReadSequenceNodeOperation::Query(_) => "Query",
    };
    match segment {
        Some(crate::ReadSequenceSelectorSegment::Attribute(name)) if name == expected => Ok(()),
        _ => Err(ReadSequenceValidationError::SelectorFailure {
            selector: raw.0.clone(),
        }),
    }
}
