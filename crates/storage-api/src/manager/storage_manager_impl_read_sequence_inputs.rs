use std::collections::{BTreeMap, HashMap};

use storage_types::{
    AttributeMap, AttributeValue, BatchGetItemRequest, ExclusiveStartKey, GetItemRequest,
    KeyAttributes, ParsedReadSequenceSelector, QueryRequest, ReadSequenceInputCardinality,
    ReadSequenceInputReference, ReadSequenceInvocationPayload, ReadSequenceNode,
    ReadSequenceNodeOperation, ReadSequenceNodeResult, ReadSequenceOnMissing, ReadSequenceSelector,
    ReadSequenceValidationError, read_sequence_input_marker_name,
    read_sequence_string_template_name,
};

mod string_template;

pub(super) use string_template::bind_string_template;

#[derive(Debug, Clone)]
pub(super) struct ResolvedInput {
    pub(super) value: AttributeValue,
    pub(super) reference: ReadSequenceInputReference,
}

pub(super) type ResolvedInputs = BTreeMap<String, Vec<ResolvedInput>>;

struct InputResolutionContext<'a> {
    node: &'a ReadSequenceNode,
    results: &'a [Option<ReadSequenceNodeResult>],
    node_names: &'a [String],
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PayloadKind {
    Get,
    BatchGet,
    Query,
}

#[derive(Clone, Copy)]
enum ItemSelection {
    Single,
    All,
    Index(usize),
}

struct PayloadSelector<'a> {
    raw: &'a str,
    kind: PayloadKind,
    selection: ItemSelection,
    item_path: ParsedReadSequenceSelector,
}

struct ValueCollector<'a> {
    source_node: &'a str,
    invocation_ordinal: u32,
    values: &'a mut Vec<ResolvedInput>,
}

pub(super) fn resolve_inputs(
    node: &ReadSequenceNode,
    results: &[Option<ReadSequenceNodeResult>],
    node_names: &[String],
) -> Result<ResolvedInputs, ReadSequenceValidationError> {
    let context = InputResolutionContext {
        node,
        results,
        node_names,
    };
    let mut resolved = BTreeMap::new();
    for (input_name, input) in node.inputs() {
        resolved.insert(
            input_name.clone(),
            resolve_input(&context, input_name, input)?,
        );
    }
    Ok(resolved)
}

fn resolve_input(
    context: &InputResolutionContext<'_>,
    input_name: &str,
    input: &storage_types::ReadSequenceNodeInput,
) -> Result<Vec<ResolvedInput>, ReadSequenceValidationError> {
    let source_index = context
        .node_names
        .iter()
        .position(|name| name == &input.from.node)
        .ok_or_else(|| ReadSequenceValidationError::UnknownNode {
            node: context.node.name.clone(),
            referenced: input.from.node.clone(),
        })?;
    let source = context
        .results
        .get(source_index)
        .and_then(Option::as_ref)
        .ok_or(ReadSequenceValidationError::GraphResolutionInvariant { remaining: 1 })?;
    let values = collect_input_values(source, input)?;
    normalize_input_values(context.node, input_name, input, values)
}

fn collect_input_values(
    source: &ReadSequenceNodeResult,
    input: &storage_types::ReadSequenceNodeInput,
) -> Result<Vec<ResolvedInput>, ReadSequenceValidationError> {
    let selector = parse_payload_selector(&input.from.select)?;
    let capacity = source
        .invocations
        .iter()
        .map(|invocation| invocation.result.item_count() as usize)
        .sum();
    let mut values = Vec::with_capacity(capacity);
    for invocation in &source.invocations {
        let mut collector = ValueCollector {
            source_node: &input.from.node,
            invocation_ordinal: invocation.ordinal,
            values: &mut values,
        };
        collect_payload_values(&invocation.result, &selector, &mut collector)?;
    }
    Ok(values)
}

fn normalize_input_values(
    node: &ReadSequenceNode,
    input_name: &str,
    input: &storage_types::ReadSequenceNodeInput,
    mut values: Vec<ResolvedInput>,
) -> Result<Vec<ResolvedInput>, ReadSequenceValidationError> {
    match input.cardinality {
        ReadSequenceInputCardinality::One => {
            normalize_single_input(node, input_name, input, &mut values)?
        }
        ReadSequenceInputCardinality::Many => {
            normalize_many_input(node, input_name, input, &values)?;
        }
    }
    Ok(values)
}

fn normalize_single_input(
    node: &ReadSequenceNode,
    input_name: &str,
    input: &storage_types::ReadSequenceNodeInput,
    values: &mut Vec<ResolvedInput>,
) -> Result<(), ReadSequenceValidationError> {
    if values.len() > 1 {
        return Err(input_resolution_error(
            node,
            input_name,
            "one",
            values.len(),
        ));
    }
    if values.is_empty() {
        match input.on_missing {
            ReadSequenceOnMissing::Error => {
                return Err(input_resolution_error(node, input_name, "one", 0));
            }
            ReadSequenceOnMissing::Skip => {}
            ReadSequenceOnMissing::Null => values.push(ResolvedInput {
                value: AttributeValue::NULL(true),
                reference: ReadSequenceInputReference {
                    node: input.from.node.clone(),
                    invocation_ordinal: 0,
                    item_ordinal: None,
                },
            }),
        }
    }
    Ok(())
}

fn normalize_many_input(
    node: &ReadSequenceNode,
    input_name: &str,
    input: &storage_types::ReadSequenceNodeInput,
    values: &[ResolvedInput],
) -> Result<(), ReadSequenceValidationError> {
    if values.is_empty() && matches!(input.on_missing, ReadSequenceOnMissing::Error) {
        return Err(input_resolution_error(node, input_name, "many", 0));
    }
    Ok(())
}

fn input_resolution_error(
    node: &ReadSequenceNode,
    input_name: &str,
    expected: &str,
    actual: usize,
) -> ReadSequenceValidationError {
    ReadSequenceValidationError::InputResolution {
        node: node.name.clone(),
        input: input_name.to_string(),
        expected: expected.to_string(),
        actual: if actual == 0 {
            "zero".to_string()
        } else {
            actual.to_string()
        },
    }
}

pub(super) fn bind_operation(
    operation: &ReadSequenceNodeOperation,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<ReadSequenceNodeOperation, ReadSequenceValidationError> {
    match operation {
        ReadSequenceNodeOperation::Get(request) => Ok(ReadSequenceNodeOperation::Get(
            bind_get_request(request, inputs)?,
        )),
        ReadSequenceNodeOperation::BatchGet(request) => Ok(ReadSequenceNodeOperation::BatchGet(
            bind_batch_get_request(request, inputs)?,
        )),
        ReadSequenceNodeOperation::Query(request) => Ok(ReadSequenceNodeOperation::Query(
            bind_query_request(request, inputs)?,
        )),
    }
}

fn bind_get_request(
    request: &GetItemRequest,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<GetItemRequest, ReadSequenceValidationError> {
    Ok(GetItemRequest {
        table_name: request.table_name.clone(),
        key: bind_key(&request.key, inputs)?,
        attributes_to_get: request.attributes_to_get.clone(),
        consistent_read: request.consistent_read,
        projection_expression: request.projection_expression.clone(),
        expression_attribute_names: request.expression_attribute_names.clone(),
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    })
}

fn bind_batch_get_request(
    request: &BatchGetItemRequest,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<BatchGetItemRequest, ReadSequenceValidationError> {
    let mut request_items = HashMap::with_capacity(request.request_items.len());
    for (table, keys) in &request.request_items {
        let bound_keys = keys
            .keys
            .iter()
            .map(|key| bind_key(key, inputs))
            .collect::<Result<_, _>>()?;
        request_items.insert(
            table.clone(),
            storage_types::KeysAndAttributes {
                keys: bound_keys,
                attributes_to_get: keys.attributes_to_get.clone(),
                projection_expression: keys.projection_expression.clone(),
                expression_attribute_names: keys.expression_attribute_names.clone(),
                consistent_read: keys.consistent_read,
            },
        );
    }
    Ok(BatchGetItemRequest {
        request_items,
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    })
}

fn bind_query_request(
    request: &QueryRequest,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<QueryRequest, ReadSequenceValidationError> {
    let expression_attribute_values = request
        .expression_attribute_values
        .as_ref()
        .map(|values| {
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), bind_value(value, inputs)?)))
                .collect::<Result<HashMap<_, _>, ReadSequenceValidationError>>()
        })
        .transpose()?;
    let exclusive_start_key = match request.exclusive_start_key.as_ref() {
        Some(ExclusiveStartKey::Key(key)) => Some(ExclusiveStartKey::Key(bind_key(key, inputs)?)),
        other => other.cloned(),
    };
    Ok(QueryRequest {
        table_name: request.table_name.clone(),
        index_name: request.index_name.clone(),
        key_condition_expression: request.key_condition_expression.clone(),
        attributes_to_get: request.attributes_to_get.clone(),
        conditional_operator: request.conditional_operator.clone(),
        filter_expression: request.filter_expression.clone(),
        query_filter: request.query_filter.clone(),
        projection_expression: request.projection_expression.clone(),
        expression_attribute_names: request.expression_attribute_names.clone(),
        expression_attribute_values,
        limit: request.limit,
        exclusive_start_key,
        return_consumed_capacity: request.return_consumed_capacity.clone(),
        consistent_read: request.consistent_read,
        scan_index_forward: request.scan_index_forward,
        select: request.select.clone(),
    })
}

fn bind_key(
    key: &KeyAttributes,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<KeyAttributes, ReadSequenceValidationError> {
    key.iter()
        .map(|(name, value)| Ok((name.to_string(), bind_value(value, inputs)?)))
        .collect()
}

fn bind_value(
    value: &AttributeValue,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<AttributeValue, ReadSequenceValidationError> {
    if let Some(literal) = storage_types::read_sequence_input_literal_name(value) {
        return Ok(AttributeValue::S(literal.to_string()));
    }
    if let Some(name) = read_sequence_input_marker_name(value) {
        return inputs
            .get(name)
            .map(|input| input.value.clone())
            .ok_or_else(|| ReadSequenceValidationError::UnknownInput {
                node: "<operation>".to_string(),
                input: name.to_string(),
            });
    }
    if let Some(template) = read_sequence_string_template_name(value) {
        return bind_string_template(template, inputs);
    }
    match value {
        AttributeValue::L(values) => values
            .iter()
            .map(|value| bind_value(value, inputs))
            .collect::<Result<Vec<_>, _>>()
            .map(AttributeValue::L),
        AttributeValue::M(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), bind_value(value, inputs)?)))
            .collect::<Result<HashMap<_, _>, ReadSequenceValidationError>>()
            .map(AttributeValue::M),
        _ => Ok(value.clone()),
    }
}

fn parse_payload_selector(
    selector: &ReadSequenceSelector,
) -> Result<PayloadSelector<'_>, ReadSequenceValidationError> {
    let mut parts = selector.0.splitn(4, '.');
    if parts.next() != Some("$") {
        return Err(selector_failure(selector));
    }
    let kind = match parts.next() {
        Some("Get") => PayloadKind::Get,
        Some("BatchGet") => PayloadKind::BatchGet,
        Some("Query") => PayloadKind::Query,
        _ => return Err(selector_failure(selector)),
    };
    let selection = parse_item_selection(kind, parts.next(), selector)?;
    let path = parts.next().map_or_else(
        || ReadSequenceSelector("$".to_string()),
        |path| ReadSequenceSelector(format!("$.{path}")),
    );
    Ok(PayloadSelector {
        raw: &selector.0,
        kind,
        selection,
        item_path: ParsedReadSequenceSelector::parse(&path)?,
    })
}

fn parse_item_selection(
    kind: PayloadKind,
    item_part: Option<&str>,
    selector: &ReadSequenceSelector,
) -> Result<ItemSelection, ReadSequenceValidationError> {
    if kind == PayloadKind::Get {
        return match item_part {
            Some("Item") => Ok(ItemSelection::Single),
            _ => Err(selector_failure(selector)),
        };
    }
    let Some(item_part) = item_part else {
        return Err(selector_failure(selector));
    };
    if item_part == "Items" || item_part == "Items[*]" {
        return Ok(ItemSelection::All);
    }
    let Some(index) = item_part
        .strip_prefix("Items[")
        .and_then(|part| part.strip_suffix(']'))
        .and_then(|part| part.parse::<usize>().ok())
    else {
        return Err(selector_failure(selector));
    };
    Ok(ItemSelection::Index(index))
}

fn selector_failure(selector: &ReadSequenceSelector) -> ReadSequenceValidationError {
    ReadSequenceValidationError::SelectorFailure {
        selector: selector.0.clone(),
    }
}

fn collect_payload_values(
    payload: &ReadSequenceInvocationPayload,
    selector: &PayloadSelector<'_>,
    collector: &mut ValueCollector<'_>,
) -> Result<(), ReadSequenceValidationError> {
    match (payload, selector.kind) {
        (ReadSequenceInvocationPayload::Get(response), PayloadKind::Get) => {
            if let Some(item) = &response.item {
                collector.collect(item, None, &selector.item_path)?;
            }
        }
        (ReadSequenceInvocationPayload::BatchGet(response), PayloadKind::BatchGet) => {
            if let Some(tables) = &response.responses {
                collect_batch_values(tables, selector, collector)?;
            }
        }
        (ReadSequenceInvocationPayload::Query(response), PayloadKind::Query) => {
            for (index, item) in response.items.iter().flatten().enumerate() {
                if selector.selection.includes(index) {
                    collector.collect(item, Some(index as u32), &selector.item_path)?;
                }
            }
        }
        _ => return Err(selector_failure_from_parsed(selector)),
    }
    Ok(())
}

fn collect_batch_values(
    tables: &HashMap<storage_types::TableName, Vec<AttributeMap>>,
    selector: &PayloadSelector<'_>,
    collector: &mut ValueCollector<'_>,
) -> Result<(), ReadSequenceValidationError> {
    let mut names = tables.keys().collect::<Vec<_>>();
    names.sort_unstable_by(|left, right| left.as_ref().cmp(right.as_ref()));
    for (index, item) in names
        .into_iter()
        .flat_map(|name| tables.get(name).into_iter().flatten())
        .enumerate()
    {
        if selector.selection.includes(index) {
            collector.collect(item, Some(index as u32), &selector.item_path)?;
        }
    }
    Ok(())
}

impl ItemSelection {
    fn includes(self, index: usize) -> bool {
        matches!(self, Self::Single | Self::All)
            || matches!(self, Self::Index(value) if value == index)
    }
}

impl ValueCollector<'_> {
    fn collect(
        &mut self,
        item: &AttributeMap,
        item_ordinal: Option<u32>,
        selector: &ParsedReadSequenceSelector,
    ) -> Result<(), ReadSequenceValidationError> {
        selector.for_each_item_value(item, |value| {
            self.values.push(ResolvedInput {
                value,
                reference: ReadSequenceInputReference {
                    node: self.source_node.to_string(),
                    invocation_ordinal: self.invocation_ordinal,
                    item_ordinal,
                },
            });
        })
    }
}

fn selector_failure_from_parsed(selector: &PayloadSelector<'_>) -> ReadSequenceValidationError {
    ReadSequenceValidationError::SelectorFailure {
        selector: selector.raw.to_string(),
    }
}
