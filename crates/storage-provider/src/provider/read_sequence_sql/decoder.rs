use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeMap, ReadSequenceNodeId, ReadSequenceNodeOperation, ReadSequencePlan,
};

use crate::provider::{
    ReadSequenceFlatResult, ReadSequenceFlatRow,
    read_sequence_sql::{ReadSequenceSqlCompileError, ReadSequenceSqlIr},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadSequenceSqlRowKind {
    InputRef,
    Item,
    Missing,
    Continuation,
}

/// Dialect-neutral row envelope used by provider decoders.  The SQL emitters
/// only produce `Item` rows today; the other row kinds are represented so a
/// future bounded child lowering can extend the protocol without changing
/// the validation contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlEnvelopeRow {
    pub node_ordinal: u32,
    pub invocation_ordinal: u32,
    pub row_kind: ReadSequenceSqlRowKind,
    pub item_ordinal: u32,
    pub key_values: Vec<storage_types::AttributeValue>,
    pub item_json: Option<Vec<u8>>,
    pub indexer_values: Vec<Option<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadSequenceSqlDecodedRow {
    pub node: ReadSequenceNodeId,
    pub invocation_ordinal: u32,
    pub item_ordinal: u32,
    pub item: AttributeMap,
    pub indexers: Vec<String>,
    pub mapped_indexer_value: Option<String>,
}

/// Validate and decode the stable flat envelope before a provider assembles a
/// public response.  This intentionally rejects missing/continuation/input
/// rows until their exact DynamoDB semantics have a differential fixture.
pub fn decode_read_sequence_sql_rows(
    plan: &ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    rows: Vec<ReadSequenceSqlEnvelopeRow>,
) -> Result<Vec<ReadSequenceSqlDecodedRow>, ReadSequenceSqlCompileError> {
    let mut previous: Option<(ReadSequenceNodeId, u32, u32)> = None;
    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        let decoded_row = decode_sql_row(plan, ir, row, previous)?;
        previous = Some((
            decoded_row.node,
            decoded_row.invocation_ordinal,
            decoded_row.item_ordinal,
        ));
        decoded.push(decoded_row);
    }
    Ok(decoded)
}

pub fn materialize_read_sequence_sql_mapped(
    plan: &ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    mut rows: Vec<ReadSequenceSqlDecodedRow>,
) -> Result<Vec<ReadSequenceFlatRow>, ReadSequenceSqlCompileError> {
    let child = ir
        .nodes
        .iter()
        .find(|node| node.metadata.mapped_source.is_some())
        .ok_or(ReadSequenceSqlCompileError::UnsupportedShape)?;
    let source = child
        .metadata
        .mapped_source
        .as_ref()
        .ok_or(ReadSequenceSqlCompileError::UnsupportedShape)?;
    let mut parent_indices = Vec::new();
    let mut child_indices = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        if row.node == source.parent {
            parent_indices.push(index);
        } else if row.node == child.node {
            child_indices.push(index);
        }
    }
    child_indices.sort_unstable_by_key(|index| rows[*index].invocation_ordinal);
    let child_invocation_limit = if source.iterates {
        parent_indices.len()
    } else {
        usize::from(!parent_indices.is_empty())
    };
    for (index, child_index) in child_indices.iter().enumerate() {
        let child_row = &rows[*child_index];
        let invocation = usize::try_from(child_row.invocation_ordinal)
            .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
        if child_row.item_ordinal != 0
            || invocation >= child_invocation_limit
            || child_indices
                .get(index.wrapping_sub(1))
                .is_some_and(|previous| {
                    rows[*previous].invocation_ordinal == child_row.invocation_ordinal
                })
        {
            return Err(ReadSequenceSqlCompileError::MalformedResult);
        }
    }
    let mapped_parents = if source.iterates {
        parent_indices.as_slice()
    } else {
        parent_indices
            .first()
            .map(std::slice::from_ref)
            .ok_or(ReadSequenceSqlCompileError::MappingMiss)?
    };
    let mapped_attribute = source
        .keys
        .iter()
        .find_map(|key| match &key.source {
            crate::provider::read_sequence_sql::ReadSequenceSqlMappedKeySource::Indexer => {
                Some(key.target_attribute_name.as_str())
            }
            crate::provider::read_sequence_sql::ReadSequenceSqlMappedKeySource::Constant(_) => None,
        })
        .ok_or(ReadSequenceSqlCompileError::MappingMiss)?;
    let mut pending_children = Vec::with_capacity(mapped_parents.len());
    let mut children_by_key = HashMap::with_capacity(child_indices.len());
    for child_index in &child_indices {
        let child = &rows[*child_index];
        let Some(storage_types::AttributeValue::S(mapped_value)) = child.item.get(mapped_attribute)
        else {
            return Err(ReadSequenceSqlCompileError::MappingMiss);
        };
        match children_by_key.entry(mapped_value.as_str()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(*child_index);
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                if rows[*entry.get()].item != child.item {
                    return Err(ReadSequenceSqlCompileError::MappingMiss);
                }
            }
        }
    }
    let mut emitted_keys = HashSet::with_capacity(mapped_parents.len());
    for (parent_ordinal, parent_index) in mapped_parents.iter().enumerate() {
        let parent = &rows[*parent_index];
        if parent.indexers.get(usize::from(source.indexer)) != Some(&source.attribute_name) {
            return Err(ReadSequenceSqlCompileError::MappingMiss);
        }
        if parent.mapped_indexer_value.is_none() {
            if source.iterates {
                continue;
            }
            return Err(ReadSequenceSqlCompileError::MappingMiss);
        }
        let parent_ordinal = u32::try_from(parent_ordinal)
            .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
        let mapped_value = parent
            .mapped_indexer_value
            .as_deref()
            .ok_or(ReadSequenceSqlCompileError::MappingMiss)?;
        let child_index = children_by_key.get(mapped_value).copied();
        let item = child_index.map(|index| &rows[index].item);
        verify_mapped_child_key(parent, item, source)?;
        if !emitted_keys.insert(mapped_value) {
            // Keep the first parent ordinal as the canonical child reference.
            continue;
        }
        let invocation_ordinal = u32::try_from(pending_children.len())
            .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
        let input_refs = [(
            source.input_name.clone(),
            storage_types::ReadSequenceInputReference {
                node: plan.nodes[source.parent.index()].name.clone(),
                invocation_ordinal: 0,
                item_ordinal: Some(parent_ordinal),
            },
        )]
        .into_iter()
        .collect();
        pending_children.push((invocation_ordinal, input_refs, child_index));
    }
    if children_by_key
        .keys()
        .any(|mapped_value| !emitted_keys.contains(mapped_value))
    {
        return Err(ReadSequenceSqlCompileError::MappingMiss);
    }
    drop(emitted_keys);
    drop(children_by_key);
    let count = u32::try_from(parent_indices.len())
        .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
    let mut output = Vec::with_capacity(parent_indices.len() + 1);
    output.push(ReadSequenceFlatRow {
        node: source.parent,
        invocation_ordinal: 0,
        input_refs: Default::default(),
        result: ReadSequenceFlatResult::Query {
            items: parent_indices
                .into_iter()
                .map(|index| std::mem::take(&mut rows[index].item))
                .collect(),
            count,
            scanned_count: count,
            last_evaluated_key: None,
        },
    });
    for (invocation_ordinal, input_refs, child_index) in pending_children {
        let item = child_index.map(|index| std::mem::take(&mut rows[index].item));
        output.push(ReadSequenceFlatRow {
            node: child.node,
            invocation_ordinal,
            input_refs,
            result: ReadSequenceFlatResult::Get { item },
        });
    }
    output.sort_by_key(|row| (row.node, row.invocation_ordinal));
    Ok(output)
}

fn verify_mapped_child_key(
    parent: &ReadSequenceSqlDecodedRow,
    child: Option<&AttributeMap>,
    source: &crate::provider::read_sequence_sql::ReadSequenceSqlMappedSource,
) -> Result<(), ReadSequenceSqlCompileError> {
    for key in &source.keys {
        let actual = child.and_then(|item| item.get(&key.target_attribute_name));
        match &key.source {
            crate::provider::read_sequence_sql::ReadSequenceSqlMappedKeySource::Indexer => {
                match (parent.mapped_indexer_value.as_deref(), actual) {
                    (None, None) => {}
                    (Some(expected), Some(storage_types::AttributeValue::S(actual)))
                        if expected == actual => {}
                    (Some(_), None) if child.is_none() => {}
                    _ => return Err(ReadSequenceSqlCompileError::MappingMiss),
                }
            }
            crate::provider::read_sequence_sql::ReadSequenceSqlMappedKeySource::Constant(
                expected,
            ) => match actual {
                Some(actual) if expected == actual => {}
                None if child.is_none() => {}
                _ => return Err(ReadSequenceSqlCompileError::MappingMiss),
            },
        }
    }
    Ok(())
}

fn decode_sql_row(
    plan: &ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    mut row: ReadSequenceSqlEnvelopeRow,
    previous: Option<(ReadSequenceNodeId, u32, u32)>,
) -> Result<ReadSequenceSqlDecodedRow, ReadSequenceSqlCompileError> {
    let node_id = ReadSequenceNodeId::from_index(row.node_ordinal as usize);
    let Some(node) = plan.nodes.get(node_id.index()) else {
        return Err(ReadSequenceSqlCompileError::MalformedResult);
    };
    let Some(metadata) = ir.nodes.iter().find(|candidate| candidate.node == node_id) else {
        return Err(ReadSequenceSqlCompileError::MissingMetadata);
    };
    validate_sql_row(&row, node_id, metadata, previous)?;
    let (mut item, indexers, mapped_indexer_value) = decode_sql_item(&mut row, metadata, ir)?;
    let key_values = std::mem::take(&mut row.key_values);
    append_sql_key_values(&mut item, key_values, metadata)?;
    if let Some(projected_attributes) = &metadata.metadata.projected_attributes {
        item.retain(|name, _| projected_attributes.iter().any(|wanted| wanted == name));
    }
    if matches!(node.operation, ReadSequenceNodeOperation::Get(_)) && row.item_ordinal != 0 {
        return Err(ReadSequenceSqlCompileError::MalformedResult);
    }
    Ok(ReadSequenceSqlDecodedRow {
        node: node_id,
        invocation_ordinal: row.invocation_ordinal,
        item_ordinal: row.item_ordinal,
        item,
        indexers,
        mapped_indexer_value,
    })
}

fn validate_sql_row(
    row: &ReadSequenceSqlEnvelopeRow,
    node_id: ReadSequenceNodeId,
    metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
    previous: Option<(ReadSequenceNodeId, u32, u32)>,
) -> Result<(), ReadSequenceSqlCompileError> {
    if !matches!(row.row_kind, ReadSequenceSqlRowKind::Item) {
        return Err(ReadSequenceSqlCompileError::UnsupportedResultRow);
    }
    let expected_item = previous
        .filter(|(node, invocation, _)| *node == node_id && *invocation == row.invocation_ordinal)
        .map_or(0, |(_, _, ordinal)| ordinal.saturating_add(1));
    if row.item_ordinal != expected_item {
        return Err(ReadSequenceSqlCompileError::MalformedResult);
    }
    if let Some((previous_node, previous_invocation, _)) = previous
        && (node_id < previous_node
            || (node_id == previous_node && row.invocation_ordinal < previous_invocation))
    {
        return Err(ReadSequenceSqlCompileError::MalformedResult);
    }
    if row.key_values.len() != metadata.metadata.key_columns.len()
        || metadata
            .metadata
            .key_types
            .iter()
            .zip(row.key_values.iter())
            .any(|(expected, value)| !sql_key_value_matches(*expected, value))
    {
        return Err(ReadSequenceSqlCompileError::MalformedResult);
    }
    Ok(())
}

fn decode_sql_item(
    row: &mut ReadSequenceSqlEnvelopeRow,
    metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
    ir: &ReadSequenceSqlIr,
) -> Result<(AttributeMap, Vec<String>, Option<String>), ReadSequenceSqlCompileError> {
    if row.indexer_values.len() != metadata.metadata.max_indexers.as_usize() {
        return Err(ReadSequenceSqlCompileError::MalformedResult);
    }
    // New row shape (§3.2/§5.2): `item_bytes` is the full-item DynamoJSON,
    // moved here without parsing beyond the single map decode the flat-row
    // contract requires. GSI rows carry their own projected bytes, so no
    // GSI→base join or residual/marker reconstruction happens on this path.
    let residual = row
        .item_json
        .take()
        .ok_or(ReadSequenceSqlCompileError::MalformedResult)?;
    let item: AttributeMap = serde_json::from_slice(&residual)
        .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
    let slots = std::mem::take(&mut row.indexer_values);
    // The new schema persists slot values only, not the per-row indexer
    // declaration names the old residual markers carried. The names needed
    // for the mapped Query→Get check come from the IR instead: every mapped
    // source names its parent attribute plus ordinal, which is exactly what
    // the materializer compares. Unknown ordinals keep an empty placeholder
    // (attribute names are never empty, so no false match is possible).
    let mut indexers = vec![String::new(); slots.len()];
    for node in &ir.nodes {
        let Some(source) = node.metadata.mapped_source.as_ref() else {
            continue;
        };
        if source.parent != metadata.node {
            continue;
        }
        if let Some(name) = indexers.get_mut(usize::from(source.indexer)) {
            *name = source.attribute_name.clone();
        }
    }
    let mapped_indexer_value = ir
        .nodes
        .iter()
        .filter_map(|node| node.metadata.mapped_source.as_ref())
        .find(|source| source.parent == metadata.node)
        .and_then(|source| slots.get(usize::from(source.indexer)))
        .cloned()
        .flatten();
    Ok((item, indexers, mapped_indexer_value))
}

pub(super) fn append_sql_key_values(
    item: &mut AttributeMap,
    key_values: Vec<storage_types::AttributeValue>,
    metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
) -> Result<(), ReadSequenceSqlCompileError> {
    for (attribute_name, value) in metadata.metadata.key_attribute_names.iter().zip(key_values) {
        if item
            .get(attribute_name)
            .is_some_and(|stored| stored != &value)
        {
            return Err(ReadSequenceSqlCompileError::MalformedResult);
        }
        item.insert(attribute_name.clone(), value);
    }
    Ok(())
}

fn sql_key_value_matches(
    expected: crate::provider::read_sequence_sql::ReadSequenceSqlKeyType,
    value: &storage_types::AttributeValue,
) -> bool {
    matches!(
        (expected, value),
        (
            crate::provider::read_sequence_sql::ReadSequenceSqlKeyType::String,
            storage_types::AttributeValue::S(_)
        ) | (
            crate::provider::read_sequence_sql::ReadSequenceSqlKeyType::Number,
            storage_types::AttributeValue::N(_)
        ) | (
            crate::provider::read_sequence_sql::ReadSequenceSqlKeyType::Binary,
            storage_types::AttributeValue::B(_)
        )
    )
}
