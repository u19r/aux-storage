use std::collections::{BTreeMap, BTreeSet};

use uuid::Uuid;

use crate::{
    DynamoRequestValidate, ReadSequenceConsistency, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeOperation, ReadSequenceStringTemplatePart, ReadSequenceStringTemplateParts,
    ReadSequenceValidationCapabilities, ReadSequenceValidationError,
    read_sequence_input_marker_name, read_sequence_string_template_name,
};

type ReadSequenceAdjacency = (Vec<Vec<ReadSequenceNodeId>>, Vec<Vec<ReadSequenceNodeId>>);

pub(super) fn validate_operation(
    node: &ReadSequenceNode,
) -> Result<(), ReadSequenceValidationError> {
    let result = match &node.operation {
        ReadSequenceNodeOperation::Get(request) => request.validate_for_dynamodb(),
        ReadSequenceNodeOperation::BatchGet(request) => request.validate_for_dynamodb(),
        ReadSequenceNodeOperation::Query(request) => request.validate_for_dynamodb(),
    };
    result.map_err(|message| ReadSequenceValidationError::InvalidOperation {
        node: node.name.clone(),
        message,
    })
}

pub(super) fn validate_operation_inputs(
    node: &ReadSequenceNode,
) -> Result<(), ReadSequenceValidationError> {
    let declared = node.inputs().keys().collect::<BTreeSet<_>>();
    let mut markers = Vec::new();
    match &node.operation {
        ReadSequenceNodeOperation::Get(request) => {
            collect_key_markers(node, &request.key, &mut markers)?;
        }
        ReadSequenceNodeOperation::BatchGet(request) => {
            for keys in request.request_items.values() {
                for key in &keys.keys {
                    collect_key_markers(node, key, &mut markers)?;
                }
            }
        }
        ReadSequenceNodeOperation::Query(request) => {
            if let Some(values) = request.expression_attribute_values.as_ref() {
                for value in values.values() {
                    collect_value_markers(node, value, &mut markers)?;
                }
            }
            if let Some(crate::ExclusiveStartKey::Key(key)) = request.exclusive_start_key.as_ref() {
                collect_key_markers(node, key, &mut markers)?;
            }
        }
    }
    for marker in markers {
        if !declared.contains(&marker) {
            return Err(ReadSequenceValidationError::UnknownInput {
                node: node.name.clone(),
                input: marker,
            });
        }
    }
    Ok(())
}

fn collect_key_markers(
    node: &ReadSequenceNode,
    key: &crate::KeyAttributes,
    markers: &mut Vec<String>,
) -> Result<(), ReadSequenceValidationError> {
    for (_, value) in key.iter() {
        collect_value_markers(node, value, markers)?;
    }
    Ok(())
}

fn collect_value_markers(
    node: &ReadSequenceNode,
    value: &crate::AttributeValue,
    markers: &mut Vec<String>,
) -> Result<(), ReadSequenceValidationError> {
    if let Some(name) = read_sequence_input_marker_name(value) {
        markers.push(name.to_string());
        return Ok(());
    }
    if let Some(template) = read_sequence_string_template_name(value) {
        for part in ReadSequenceStringTemplateParts::new(template) {
            match part {
                Ok(ReadSequenceStringTemplatePart::Input(name)) => markers.push(name.to_string()),
                Ok(ReadSequenceStringTemplatePart::Literal(_)) => {}
                Err(_) => {
                    return Err(ReadSequenceValidationError::InvalidStringTemplate {
                        node: node.name.clone(),
                    });
                }
            }
        }
        return Ok(());
    }
    match value {
        crate::AttributeValue::L(values) => {
            for value in values {
                collect_value_markers(node, value, markers)?;
            }
        }
        crate::AttributeValue::M(values) => {
            for value in values.values() {
                collect_value_markers(node, value, markers)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn validate_operation_consistency(
    consistency: ReadSequenceConsistency,
    reads_gsi: bool,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<(), ReadSequenceValidationError> {
    match consistency {
        ReadSequenceConsistency::Eventual if !capabilities.eventual_reads => {
            Err(ReadSequenceValidationError::UnsupportedConsistency { consistency })
        }
        ReadSequenceConsistency::Eventual => Ok(()),
        ReadSequenceConsistency::Strong if !capabilities.strong_reads => {
            Err(ReadSequenceValidationError::UnsupportedConsistency { consistency })
        }
        ReadSequenceConsistency::Strong if reads_gsi => {
            Err(ReadSequenceValidationError::StrongGsiRejected)
        }
        ReadSequenceConsistency::Strong => Ok(()),
        ReadSequenceConsistency::Transactional if !capabilities.transactional_reads => {
            Err(ReadSequenceValidationError::UnsupportedConsistency { consistency })
        }
        ReadSequenceConsistency::Transactional
            if reads_gsi && !capabilities.immediate_gsi_consistency =>
        {
            Err(ReadSequenceValidationError::TransactionalGsiRejected)
        }
        ReadSequenceConsistency::Transactional => Ok(()),
    }
}

pub(super) fn resolve_outputs(
    nodes: &[ReadSequenceNode],
    requested_outputs: Option<&[String]>,
    names: &BTreeMap<String, ReadSequenceNodeId>,
) -> Result<Vec<ReadSequenceNodeId>, ReadSequenceValidationError> {
    let Some(requested_outputs) = requested_outputs else {
        return Ok((0..nodes.len())
            .map(ReadSequenceNodeId::from_index)
            .collect());
    };
    let mut outputs = Vec::with_capacity(requested_outputs.len());
    for output in requested_outputs {
        let Some(node) = names.get(output).copied() else {
            return Err(ReadSequenceValidationError::UnknownNode {
                node: "<outputs>".to_string(),
                referenced: output.clone(),
            });
        };
        if !outputs.contains(&node) {
            outputs.push(node);
        }
    }
    outputs.sort_unstable();
    Ok(outputs)
}

pub(super) fn build_adjacency(
    nodes: &[ReadSequenceNode],
    names: &BTreeMap<String, ReadSequenceNodeId>,
) -> Result<ReadSequenceAdjacency, ReadSequenceValidationError> {
    let mut dependencies = vec![BTreeSet::new(); nodes.len()];
    let mut consumers = vec![BTreeSet::new(); nodes.len()];
    for (ordinal, node) in nodes.iter().enumerate() {
        let current = ReadSequenceNodeId::from_index(ordinal);
        for dependency in node
            .after()
            .iter()
            .chain(node.inputs().values().map(|input| &input.from.node))
        {
            let Some(&dependency_id) = names.get(dependency) else {
                return Err(ReadSequenceValidationError::UnknownNode {
                    node: node.name.clone(),
                    referenced: dependency.clone(),
                });
            };
            if dependency_id == current {
                return Err(ReadSequenceValidationError::SelfDependency {
                    node: node.name.clone(),
                });
            }
            dependencies[ordinal].insert(dependency_id);
            consumers[dependency_id.index()].insert(current);
        }
    }
    Ok((
        dependencies
            .into_iter()
            .map(|set| set.into_iter().collect())
            .collect(),
        consumers
            .into_iter()
            .map(|set| set.into_iter().collect())
            .collect(),
    ))
}

pub(super) fn validate_output_closure(
    dependencies: &[Vec<ReadSequenceNodeId>],
    outputs: &[ReadSequenceNodeId],
    nodes: &[ReadSequenceNode],
) -> Result<(), ReadSequenceValidationError> {
    if outputs.is_empty() {
        return Err(ReadSequenceValidationError::EmptyOutputs);
    }
    let mut required = BTreeSet::new();
    let mut pending = outputs.to_vec();
    while let Some(node) = pending.pop() {
        if !required.insert(node) {
            continue;
        }
        pending.extend(dependencies[node.index()].iter().copied());
    }
    for (ordinal, node) in nodes.iter().enumerate() {
        if !required.contains(&ReadSequenceNodeId::from_index(ordinal)) {
            return Err(ReadSequenceValidationError::UnreachableNode {
                node: node.name.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn topological_schedule(
    dependencies: &[Vec<ReadSequenceNodeId>],
    consumers: &[Vec<ReadSequenceNodeId>],
    nodes: &[ReadSequenceNode],
) -> Result<(Vec<ReadSequenceNodeId>, Vec<Vec<ReadSequenceNodeId>>), ReadSequenceValidationError> {
    let mut remaining = dependencies.iter().map(Vec::len).collect::<Vec<_>>();
    let mut ready = ready_nodes(&remaining);
    let mut order = Vec::with_capacity(nodes.len());
    let mut waves = Vec::<Vec<ReadSequenceNodeId>>::new();
    let mut wave_for = vec![0usize; nodes.len()];
    while !ready.is_empty() {
        let current = ready.iter().copied().collect::<Vec<_>>();
        ready.clear();
        emit_wave(&current, &mut order, &mut waves, &wave_for);
        ready.extend(advance_wave(
            &current,
            consumers,
            &mut remaining,
            &mut wave_for,
        ));
    }
    if order.len() != nodes.len() {
        return Err(ReadSequenceValidationError::DependencyCycle {
            cycle: deterministic_cycle(dependencies, &order, nodes),
        });
    }
    Ok((order, waves))
}

fn ready_nodes(remaining: &[usize]) -> BTreeSet<ReadSequenceNodeId> {
    remaining
        .iter()
        .enumerate()
        .filter_map(|(ordinal, count)| {
            (*count == 0).then_some(ReadSequenceNodeId::from_index(ordinal))
        })
        .collect()
}

fn emit_wave(
    current: &[ReadSequenceNodeId],
    order: &mut Vec<ReadSequenceNodeId>,
    waves: &mut Vec<Vec<ReadSequenceNodeId>>,
    wave_for: &[usize],
) {
    for node in current {
        order.push(*node);
        let wave = wave_for[node.index()];
        if waves.len() <= wave {
            waves.resize_with(wave + 1, Vec::new);
        }
        waves[wave].push(*node);
    }
}

fn advance_wave(
    current: &[ReadSequenceNodeId],
    consumers: &[Vec<ReadSequenceNodeId>],
    remaining: &mut [usize],
    wave_for: &mut [usize],
) -> BTreeSet<ReadSequenceNodeId> {
    let mut ready = BTreeSet::new();
    for node in current {
        for consumer in &consumers[node.index()] {
            remaining[consumer.index()] -= 1;
            wave_for[consumer.index()] = wave_for[consumer.index()].max(wave_for[node.index()] + 1);
            if remaining[consumer.index()] == 0 {
                ready.insert(*consumer);
            }
        }
    }
    ready
}

pub(super) fn deterministic_cycle(
    dependencies: &[Vec<ReadSequenceNodeId>],
    order: &[ReadSequenceNodeId],
    nodes: &[ReadSequenceNode],
) -> Vec<String> {
    let emitted = order.iter().copied().collect::<BTreeSet<_>>();
    let start = (0..nodes.len())
        .map(ReadSequenceNodeId::from_index)
        .find(|node| !emitted.contains(node))
        .unwrap_or(ReadSequenceNodeId::from_index(0));
    let mut path = Vec::new();
    let mut positions = BTreeMap::new();
    let mut current = start;
    loop {
        if let Some(&position) = positions.get(&current) {
            let mut cycle = path[position..]
                .iter()
                .map(|node: &ReadSequenceNodeId| nodes[node.index()].name.clone())
                .collect::<Vec<_>>();
            cycle.push(nodes[current.index()].name.clone());
            return cycle;
        }
        positions.insert(current, path.len());
        path.push(current);
        current = dependencies[current.index()]
            .iter()
            .copied()
            .filter(|node| !emitted.contains(node))
            .min()
            .unwrap_or(start);
    }
}

pub(super) fn graph_digest(nodes: &[ReadSequenceNode], outputs: Option<&[String]>) -> String {
    let payload = crate::canonical_json::to_vec(&(nodes, outputs)).unwrap_or_default();
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string()
}

pub(super) fn validate_node_name(name: &str) -> Result<(), ReadSequenceValidationError> {
    if !name.is_empty()
        && name
            .chars()
            .all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        Ok(())
    } else {
        Err(ReadSequenceValidationError::InvalidNodeName {
            name: name.to_string(),
        })
    }
}
