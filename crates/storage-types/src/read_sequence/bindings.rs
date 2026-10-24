//! Invocation expansion and Get key deduplication for ReadSequence nodes.

use std::collections::BTreeMap;

use crate::{
    AttributeValue, KeyAttributes, ReadSequenceInputReference, ReadSequenceNode,
    ReadSequenceNodeOperation, ReadSequenceValidationError, ResolvedInput, ResolvedInputs,
    bind_operation, canonical_dynamodb_number,
};

/// Every bound value carries the source reference used in the wire response.
/// Root invocations have an empty map, so no optional-reference state is
/// needed.
pub struct InvocationBindings {
    values: BTreeMap<String, ResolvedInput>,
}

impl InvocationBindings {
    pub fn resolved_inputs(&self) -> &BTreeMap<String, ResolvedInput> {
        &self.values
    }

    pub fn input_refs(&self) -> BTreeMap<String, ReadSequenceInputReference> {
        self.values
            .iter()
            .map(|(name, input)| (name.clone(), input.reference.clone()))
            .collect()
    }

    pub fn into_input_refs(self) -> BTreeMap<String, ReadSequenceInputReference> {
        self.values
            .into_iter()
            .map(|(name, input)| (name, input.reference))
            .collect()
    }
}

pub fn invocation_bindings(
    node: &ReadSequenceNode,
    resolved: &ResolvedInputs,
) -> Result<Vec<InvocationBindings>, ReadSequenceValidationError> {
    let bindings = if let Some(iterate) = node.iterate.as_deref() {
        iterated_bindings(resolved, iterate)?
    } else if resolved.values().any(Vec::is_empty) {
        Vec::new()
    } else {
        let values = resolved
            .iter()
            .filter_map(|(name, values)| values.first().map(|value| (name.clone(), value.clone())))
            .collect();
        vec![InvocationBindings { values }]
    };
    deduplicate_get_bindings(node, bindings)
}

fn iterated_bindings(
    resolved: &ResolvedInputs,
    iterate: &str,
) -> Result<Vec<InvocationBindings>, ReadSequenceValidationError> {
    let iterations = resolved.get(iterate).cloned().unwrap_or_default();
    if resolved
        .iter()
        .any(|(name, values)| name != iterate && values.is_empty())
    {
        return Ok(Vec::new());
    }
    let mut invocations = Vec::with_capacity(iterations.len());
    for iteration in iterations {
        let mut values = BTreeMap::new();
        for (name, inputs) in resolved {
            let input = if name == iterate {
                &iteration
            } else {
                inputs
                    .first()
                    .ok_or(ReadSequenceValidationError::GraphResolutionInvariant { remaining: 1 })?
            };
            values.insert(name.clone(), input.clone());
        }
        invocations.push(InvocationBindings { values });
    }
    Ok(invocations)
}

fn deduplicate_get_bindings(
    node: &ReadSequenceNode,
    bindings: Vec<InvocationBindings>,
) -> Result<Vec<InvocationBindings>, ReadSequenceValidationError> {
    if !matches!(node.operation, ReadSequenceNodeOperation::Get(_)) || bindings.len() < 2 {
        return Ok(bindings);
    }
    let mut keys = Vec::with_capacity(bindings.len());
    let mut unique = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let operation = bind_operation(&node.operation, binding.resolved_inputs())?;
        let ReadSequenceNodeOperation::Get(request) = operation else {
            return Err(ReadSequenceValidationError::InvalidOperation {
                node: node.name.clone(),
                message: "Get binding produced another operation".to_string(),
            });
        };
        if keys.iter().all(|key| !same_key(key, &request.key)) {
            keys.push(request.key);
            unique.push(binding);
        }
    }
    Ok(unique)
}

fn same_key(left: &KeyAttributes, right: &KeyAttributes) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, value)| {
            right
                .get(name)
                .is_some_and(|other| same_value(value, other))
        })
}

fn same_value(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => match (
            canonical_dynamodb_number(left),
            canonical_dynamodb_number(right),
        ) {
            (Ok(left), Ok(right)) => left == right,
            _ => left == right,
        },
        _ => left == right,
    }
}
