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
    rows: Vec<ReadSequenceSqlDecodedRow>,
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
    let parent_items = rows
        .iter()
        .filter(|row| row.node == source.parent)
        .collect::<Vec<_>>();
    let mut children = rows
        .iter()
        .filter(|row| row.node == child.node)
        .collect::<Vec<_>>();
    children.sort_by_key(|row| row.invocation_ordinal);
    for (index, child_row) in children.iter().enumerate() {
        let invocation = usize::try_from(child_row.invocation_ordinal)
            .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
        if child_row.item_ordinal != 0
            || invocation >= parent_items.len()
            || children
                .get(index.wrapping_sub(1))
                .is_some_and(|previous| previous.invocation_ordinal == child_row.invocation_ordinal)
        {
            return Err(ReadSequenceSqlCompileError::MalformedResult);
        }
    }
    let mut output = Vec::with_capacity(parent_items.len() + 1);
    let count = u32::try_from(parent_items.len())
        .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
    output.push(ReadSequenceFlatRow {
        node: source.parent,
        invocation_ordinal: 0,
        input_refs: Default::default(),
        result: ReadSequenceFlatResult::Query {
            items: parent_items.iter().map(|row| row.item.clone()).collect(),
            count,
            scanned_count: count,
            last_evaluated_key: None,
        },
    });
    let mapped_parents = if source.iterates {
        parent_items.as_slice()
    } else {
        parent_items
            .first()
            .map(std::slice::from_ref)
            .ok_or(ReadSequenceSqlCompileError::MappingMiss)?
    };
    for (ordinal, parent) in mapped_parents.iter().copied().enumerate() {
        if parent.indexers.get(usize::from(source.indexer)) != Some(&source.attribute_name) {
            return Err(ReadSequenceSqlCompileError::MappingMiss);
        }
        let ordinal =
            u32::try_from(ordinal).map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
        let item = children
            .iter()
            .find(|row| row.invocation_ordinal == ordinal)
            .map(|row| row.item.clone());
        verify_mapped_child_key(parent, item.as_ref(), source)?;
        let input_refs = [(
            source.input_name.clone(),
            storage_types::ReadSequenceInputReference {
                node: plan.nodes[source.parent.index()].name.clone(),
                invocation_ordinal: 0,
                item_ordinal: Some(ordinal),
            },
        )]
        .into_iter()
        .collect();
        output.push(ReadSequenceFlatRow {
            node: child.node,
            invocation_ordinal: ordinal,
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
    append_sql_key_values(&mut item, &row, metadata)?;
    if let Some(projected_attributes) = &metadata.metadata.projected_attributes {
        let mut projected = AttributeMap::with_capacity(projected_attributes.len());
        for name in projected_attributes {
            if let Some(value) = item.get(name) {
                projected.insert(name.clone(), value.clone());
            }
        }
        item = projected;
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
    let residual = row
        .item_json
        .take()
        .ok_or(ReadSequenceSqlCompileError::MalformedResult)?;
    let storage_types::DecodedIndexedWireItem {
        item,
        declaration,
        slots,
    } = storage_types::IndexedWireItem::decode_padded_parts(
        residual,
        std::mem::take(&mut row.indexer_values),
    )
    .map_err(|_| ReadSequenceSqlCompileError::MalformedResult)?;
    let mapped_indexer_value = ir
        .nodes
        .iter()
        .filter_map(|node| node.metadata.mapped_source.as_ref())
        .find(|source| source.parent == metadata.node)
        .and_then(|source| slots.get(usize::from(source.indexer)))
        .cloned()
        .flatten();
    Ok((
        AttributeMap::from(item),
        declaration.into_names(),
        mapped_indexer_value,
    ))
}

fn append_sql_key_values(
    item: &mut AttributeMap,
    row: &ReadSequenceSqlEnvelopeRow,
    metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
) -> Result<(), ReadSequenceSqlCompileError> {
    for (attribute_name, value) in metadata
        .metadata
        .key_attribute_names
        .iter()
        .zip(row.key_values.iter())
    {
        if item
            .get(attribute_name)
            .is_some_and(|stored| stored != value)
        {
            return Err(ReadSequenceSqlCompileError::MalformedResult);
        }
        item.insert(attribute_name.clone(), value.clone());
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
