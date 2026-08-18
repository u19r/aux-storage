use std::collections::{BTreeMap, HashMap};

use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_provider::{ReadSequenceFlatResult, ReadSequenceFlatRow, ReadSequenceMappedRangePage};
use storage_types::{
    AttributeMap, AttributeProjection, AttributeValue, ItemKey, MaxIndexers,
    ReadSequenceInputReference, ReadSequenceNodeId, ReadSequenceStringTemplatePart,
    ReadSequenceStringTemplateParts, StorageError, StorageResult, TableKey,
};

use crate::{
    keyspace::{table_identity::StoredTableMetadata, tuple_keys},
    storage_ops::provider_impl::{
        decode_indexed_wire_item,
        read_sequence_mapped::{
            MappedGetQueryShape, MappedSequenceShape,
            bindings::{MappedInput, MappedInputItem, MappedKeyBinding},
        },
    },
};

type DecodedItem = HashMap<String, AttributeValue>;

struct DecodedParent {
    item: DecodedItem,
    declaration: storage_types::IndexerDeclaration,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IndexedSourceState {
    Unmapped,
    Verified,
    Nil,
    Fallback,
}

struct MappedRowMaterialization<'a> {
    condition: Option<Condition>,
    parent_projection: Option<AttributeProjection<'a>>,
    child_projection: Option<AttributeProjection<'a>>,
    same_item: bool,
}

struct MaterializedMappedItems {
    parents: Vec<AttributeMap>,
    children: Vec<Option<AttributeMap>>,
}

pub(super) fn mapped_edge_rows(
    shape: &MappedSequenceShape<'_>,
    page: ReadSequenceMappedRangePage,
    node_count: usize,
    same_item: bool,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
) -> StorageResult<Option<Vec<Vec<ReadSequenceFlatRow>>>> {
    let scanned_count = u32::try_from(page.entries.len())
        .map_err(|_| StorageError::internal("mapped parent item count overflow"))?;
    let Some((parent_items, child_rows)) = mapped_item_rows(shape, page, same_item, parent, child)?
    else {
        return Ok(None);
    };
    let count = u32::try_from(parent_items.len())
        .map_err(|_| StorageError::internal("mapped filtered item count overflow"))?;
    let mut rows = (0..node_count).map(|_| Vec::new()).collect::<Vec<_>>();
    rows[shape.parent_id.index()] = vec![ReadSequenceFlatRow {
        node: shape.parent_id,
        invocation_ordinal: 0,
        input_refs: Default::default(),
        result: ReadSequenceFlatResult::Query {
            items: parent_items,
            count,
            scanned_count,
            last_evaluated_key: None,
        },
    }];
    rows[shape.child_id.index()] = child_rows;
    Ok(Some(rows))
}

pub(super) fn mapped_get_query_rows(
    shape: &MappedGetQueryShape<'_>,
    page: ReadSequenceMappedRangePage,
    node_count: usize,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
) -> StorageResult<Option<Vec<Vec<ReadSequenceFlatRow>>>> {
    let [entry] = page.entries.as_slice() else {
        return Ok(None);
    };
    let raw_parent = decode_parent(&entry.parent_value, parent.table_info.max_indexers)?;
    let parent_projection = AttributeProjection::new(
        shape.parent_get.projection_expression.as_deref(),
        shape.parent_get.attributes_to_get.as_deref(),
        shape.parent_get.expression_attribute_names.as_ref(),
    );
    let child_projection = AttributeProjection::new(
        shape.child_query.projection_expression.as_deref(),
        shape.child_query.attributes_to_get.as_deref(),
        shape.child_query.expression_attribute_names.as_ref(),
    );
    let child_condition = shape
        .child_query
        .filter_expression
        .as_deref()
        .map(|expression| {
            parse_condition_expression(
                expression,
                shape.child_query.expression_attribute_names.as_ref(),
                shape.child_query.expression_attribute_values.as_ref(),
            )
            .map_err(|error| {
                StorageError::validation(error.replace("ConditionExpression", "FilterExpression"))
            })
        })
        .transpose()?;
    if !mapped_get_query_key_matches(shape, child, &raw_parent.item, &entry.key_values)? {
        return Ok(None);
    }
    let mut child_items = Vec::with_capacity(entry.key_values.len());
    for key_value in &entry.key_values {
        let raw_child = decode_storage_item(&key_value.value, child.table_info.max_indexers)?;
        if child_condition
            .as_ref()
            .is_some_and(|condition| !evaluate_condition(&raw_child, condition))
        {
            continue;
        }
        child_items.push(child_projection.as_ref().map_or_else(
            || raw_child.clone().into(),
            |projection| projection.project(&raw_child),
        ));
    }
    let parent_item = parent_projection.as_ref().map_or_else(
        || raw_parent.item.clone().into(),
        |projection| projection.project(&raw_parent.item),
    );
    let parent_count = u32::from(!entry.parent_value.is_empty());
    let child_count = u32::try_from(child_items.len())
        .map_err(|_| StorageError::internal("mapped child query item count overflow"))?;
    let scanned_count = u32::try_from(entry.key_values.len())
        .map_err(|_| StorageError::internal("mapped child query scanned count overflow"))?;
    let mut rows = (0..node_count).map(|_| Vec::new()).collect::<Vec<_>>();
    rows[shape.parent_id.index()] = vec![ReadSequenceFlatRow {
        node: shape.parent_id,
        invocation_ordinal: 0,
        input_refs: Default::default(),
        result: ReadSequenceFlatResult::Get {
            item: (parent_count == 1).then_some(parent_item),
        },
    }];
    rows[shape.child_id.index()] = vec![ReadSequenceFlatRow {
        node: shape.child_id,
        invocation_ordinal: 0,
        input_refs: mapped_get_query_input_refs(shape),
        result: ReadSequenceFlatResult::Query {
            items: child_items,
            count: child_count,
            scanned_count,
            last_evaluated_key: None,
        },
    }];
    Ok(Some(rows))
}

fn mapped_get_query_key_matches(
    shape: &MappedGetQueryShape<'_>,
    child: &StoredTableMetadata,
    parent: &DecodedItem,
    key_values: &[storage_provider::ReadSequenceMappedKeyValue],
) -> StorageResult<bool> {
    let Some(binding) = shape.keys.first() else {
        return Ok(false);
    };
    let value = if let Some(input_name) = binding.direct_input {
        shape
            .inputs
            .iter()
            .find(|input| input.name == input_name)
            .and_then(|input| parent.get(input.attribute_name))
    } else {
        binding.literal
    };
    let Some(value) = value else {
        return Ok(false);
    };
    let item_key = ItemKey::Table(TableKey::new(
        child.table_info.table_name.clone(),
        value.clone(),
        None,
    ));
    let prefix = tuple_keys::item_key_prefix(&child.identity, &item_key)?;
    Ok(key_values
        .iter()
        .all(|key_value| key_value.key.starts_with(&prefix)))
}

fn mapped_get_query_input_refs(
    shape: &MappedGetQueryShape<'_>,
) -> BTreeMap<String, ReadSequenceInputReference> {
    shape
        .inputs
        .iter()
        .map(|input| {
            (
                input.name.to_string(),
                ReadSequenceInputReference {
                    node: shape.parent_name.to_string(),
                    invocation_ordinal: 0,
                    item_ordinal: None,
                },
            )
        })
        .collect()
}

fn mapped_item_rows(
    shape: &MappedSequenceShape<'_>,
    page: ReadSequenceMappedRangePage,
    same_item: bool,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
) -> StorageResult<Option<(Vec<AttributeMap>, Vec<ReadSequenceFlatRow>)>> {
    let materialization = mapped_row_materialization(shape, same_item)?;
    let Some(materialized) =
        materialize_mapped_items(shape, page.entries, materialization, parent, child)?
    else {
        return Ok(None);
    };
    let rows = mapped_child_rows(shape, &materialized.parents, materialized.children)?;
    Ok(Some((materialized.parents, rows)))
}

fn mapped_row_materialization<'a>(
    shape: &MappedSequenceShape<'a>,
    same_item: bool,
) -> StorageResult<MappedRowMaterialization<'a>> {
    let condition = shape
        .parent_query
        .filter_expression
        .as_deref()
        .map(|expression| {
            parse_condition_expression(
                expression,
                shape.parent_query.expression_attribute_names.as_ref(),
                shape.parent_query.expression_attribute_values.as_ref(),
            )
            .map_err(|error| {
                StorageError::validation(error.replace("ConditionExpression", "FilterExpression"))
            })
        })
        .transpose()?;
    let parent_projection = AttributeProjection::new(
        shape.parent_query.projection_expression.as_deref(),
        shape.parent_query.attributes_to_get.as_deref(),
        shape.parent_query.expression_attribute_names.as_ref(),
    );
    let child_projection = AttributeProjection::new(
        shape.child_get.projection_expression.as_deref(),
        shape.child_get.attributes_to_get.as_deref(),
        shape.child_get.expression_attribute_names.as_ref(),
    );
    Ok(MappedRowMaterialization {
        condition,
        parent_projection,
        child_projection,
        same_item,
    })
}

fn materialize_mapped_items(
    shape: &MappedSequenceShape<'_>,
    entries: Vec<storage_provider::ReadSequenceMappedEntry>,
    materialization: MappedRowMaterialization<'_>,
    parent_metadata: &StoredTableMetadata,
    child_metadata: &StoredTableMetadata,
) -> StorageResult<Option<MaterializedMappedItems>> {
    let mut parents = Vec::with_capacity(entries.len());
    let mut children = Vec::with_capacity(entries.len());
    for entry in entries {
        let raw_parent =
            decode_parent(&entry.parent_value, parent_metadata.table_info.max_indexers)?;
        if materialization
            .condition
            .as_ref()
            .is_some_and(|condition| !evaluate_condition(&raw_parent.item, condition))
        {
            continue;
        }
        let mapping_state = verify_indexed_sources(shape, &raw_parent, &entry.key_values);
        if mapping_state == IndexedSourceState::Fallback {
            return Ok(None);
        }
        let materialize_child = shape.iterates || parents.is_empty();
        let child = materialize_child
            .then(|| {
                child_item(
                    &raw_parent.item,
                    &entry.key_values,
                    materialization.same_item,
                    materialization.child_projection.as_ref(),
                    child_metadata.table_info.max_indexers,
                )
            })
            .transpose()?;
        let item_ordinal = parents.len();
        let projected_parent =
            project_parent(materialization.parent_projection.as_ref(), &raw_parent.item);
        if materialize_child {
            if !mapped_binding_matches(
                shape,
                &parents,
                projected_parent.as_ref(),
                &raw_parent.item,
                item_ordinal,
            ) {
                return Ok(None);
            }
            if mapping_state == IndexedSourceState::Verified
                && !mapped_child_key_matches(
                    shape,
                    child_metadata,
                    &parents,
                    projected_parent.as_ref(),
                    &raw_parent.item,
                    item_ordinal,
                    &entry.key_values,
                )?
            {
                record_indexer("mapped_indexer_key_mismatch");
                record_indexer("mapped_indexer_fallback");
                return Ok(None);
            }
            children.push(child.flatten());
        }
        parents.push(projected_parent.unwrap_or_else(|| raw_parent.item.into()));
    }
    if parents.is_empty() && !shape.iterates {
        return Ok(None);
    }
    Ok(Some(MaterializedMappedItems { parents, children }))
}

fn project_parent(
    projection: Option<&AttributeProjection<'_>>,
    item: &DecodedItem,
) -> Option<AttributeMap> {
    projection.map(|projection| projection.project(item))
}

fn verify_indexed_sources(
    shape: &MappedSequenceShape<'_>,
    parent: &DecodedParent,
    child_values: &[storage_provider::ReadSequenceMappedKeyValue],
) -> IndexedSourceState {
    let mut state = IndexedSourceState::Unmapped;
    for input in shape.inputs.iter().filter(|input| {
        input.indexer.is_some()
            && shape
                .keys
                .iter()
                .any(|key| key.direct_input == Some(input.name))
    }) {
        let Some(indexer) = input.indexer else {
            continue;
        };
        let ordinal = usize::from(indexer);
        let Some(name) = parent.declaration.names().get(ordinal) else {
            record_indexer("mapped_indexer_ordinal_miss");
            record_indexer("mapped_indexer_fallback");
            return IndexedSourceState::Fallback;
        };
        if name != input.attribute_name {
            record_indexer("mapped_indexer_name_mismatch");
            record_indexer("mapped_indexer_fallback");
            return IndexedSourceState::Fallback;
        }
        if parent.item.contains_key(input.attribute_name) {
            state = IndexedSourceState::Verified;
        } else {
            if !child_values.is_empty() {
                record_indexer("mapped_indexer_key_mismatch");
                record_indexer("mapped_indexer_fallback");
                return IndexedSourceState::Fallback;
            }
            state = IndexedSourceState::Nil;
        }
    }
    match state {
        IndexedSourceState::Verified => record_indexer("mapped_indexer_verified"),
        IndexedSourceState::Nil => record_indexer("mapped_indexer_nil"),
        IndexedSourceState::Unmapped | IndexedSourceState::Fallback => {}
    }
    state
}

fn record_indexer(outcome: &'static str) {
    ::metrics::counter!("storage.read_sequence.mapped_indexer.total", "outcome" => outcome)
        .increment(1);
}

fn child_item(
    parent: &DecodedItem,
    key_values: &[storage_provider::ReadSequenceMappedKeyValue],
    same_item: bool,
    projection: Option<&AttributeProjection<'_>>,
    child_capacity: MaxIndexers,
) -> StorageResult<Option<AttributeMap>> {
    if same_item {
        if !key_values.is_empty() {
            return Err(StorageError::internal(
                "same-item mapped range returned a secondary value",
            ));
        }
        return Ok(Some(projection.map_or_else(
            || parent.clone().into(),
            |projection| projection.project(parent),
        )));
    }
    Ok(
        decode_mapped_child(key_values, child_capacity)?.map(|item| match projection {
            Some(projection) => projection.project(&item),
            None => item.into(),
        }),
    )
}

fn mapped_child_rows(
    shape: &MappedSequenceShape<'_>,
    parents: &[AttributeMap],
    children: Vec<Option<AttributeMap>>,
) -> StorageResult<Vec<ReadSequenceFlatRow>> {
    let count = if shape.iterates { children.len() } else { 1 };
    let mut rows = Vec::with_capacity(count);
    for (index, child) in children.into_iter().take(count).enumerate() {
        let ordinal = u32::try_from(index)
            .map_err(|_| StorageError::internal("mapped child invocation ordinal overflow"))?;
        rows.push(mapped_child_row(
            shape.child_id,
            mapped_input_refs(shape, ordinal),
            ordinal,
            child,
        ));
    }
    if rows.is_empty() && !parents.is_empty() {
        return Err(StorageError::internal(
            "mapped child row was not materialized",
        ));
    }
    Ok(rows)
}

fn mapped_binding_matches(
    shape: &MappedSequenceShape<'_>,
    parents: &[AttributeMap],
    current_parent: Option<&AttributeMap>,
    raw_parent: &DecodedItem,
    item_ordinal: usize,
) -> bool {
    shape.keys.iter().all(|key| {
        if let Some(input_name) = key.direct_input {
            let Some(input) = shape.inputs.iter().find(|input| input.name == input_name) else {
                return false;
            };
            let Some(expected) = raw_parent.get(key.source_name) else {
                return input.indexer.is_some();
            };
            return shape
                .inputs
                .iter()
                .find(|input| input.name == input_name)
                .and_then(|input| {
                    mapped_input_value(input, parents, current_parent, raw_parent, item_ordinal)
                })
                == Some(expected);
        }
        if key.literal.is_some() {
            return true;
        }
        let Some(expected) = raw_parent.get(key.source_name) else {
            return false;
        };
        matches!(expected, AttributeValue::S(value)
        if key.template.is_some_and(|template| mapped_template_matches(
            shape, parents, current_parent, raw_parent, item_ordinal, template, value
        )))
    })
}

fn mapped_child_key_matches(
    shape: &MappedSequenceShape<'_>,
    child: &StoredTableMetadata,
    parents: &[AttributeMap],
    current_parent: Option<&AttributeMap>,
    raw_parent: &DecodedItem,
    item_ordinal: usize,
    key_values: &[storage_provider::ReadSequenceMappedKeyValue],
) -> StorageResult<bool> {
    let Some(key_value) = key_values.first() else {
        return Ok(true);
    };
    let mut attributes = HashMap::with_capacity(shape.keys.len());
    for binding in &shape.keys {
        let Some(value) = mapped_binding_value(
            shape,
            binding,
            parents,
            current_parent,
            raw_parent,
            item_ordinal,
        ) else {
            return Ok(false);
        };
        attributes.insert(binding.target_name.to_string(), value);
    }
    let item_key = ItemKey::from_key_schema(
        child.table_info.table_name.clone(),
        &child.table_info.key_schema,
        &attributes,
    )?;
    Ok(tuple_keys::item_key(&child.identity, &item_key)? == key_value.key)
}

fn mapped_binding_value(
    shape: &MappedSequenceShape<'_>,
    binding: &MappedKeyBinding<'_>,
    parents: &[AttributeMap],
    current_parent: Option<&AttributeMap>,
    raw_parent: &DecodedItem,
    item_ordinal: usize,
) -> Option<AttributeValue> {
    if let Some(input_name) = binding.direct_input {
        return shape
            .inputs
            .iter()
            .find(|input| input.name == input_name)
            .and_then(|input| {
                mapped_input_value(input, parents, current_parent, raw_parent, item_ordinal)
            })
            .cloned();
    }
    if let Some(literal) = binding.literal {
        return Some(literal.clone());
    }
    render_template(
        shape,
        binding.template?,
        parents,
        current_parent,
        raw_parent,
        item_ordinal,
    )
    .map(AttributeValue::S)
}

fn render_template(
    shape: &MappedSequenceShape<'_>,
    template: &str,
    parents: &[AttributeMap],
    current_parent: Option<&AttributeMap>,
    raw_parent: &DecodedItem,
    item_ordinal: usize,
) -> Option<String> {
    let mut output = String::with_capacity(template.len());
    for part in ReadSequenceStringTemplateParts::new(template) {
        match part.ok()? {
            ReadSequenceStringTemplatePart::Literal(literal) => output.push_str(literal),
            ReadSequenceStringTemplatePart::Input(name) => {
                let AttributeValue::S(value) = shape
                    .inputs
                    .iter()
                    .find(|input| input.name == name)
                    .and_then(|input| {
                        mapped_input_value(input, parents, current_parent, raw_parent, item_ordinal)
                    })?
                else {
                    return None;
                };
                output.push_str(value);
            }
        }
    }
    Some(output)
}

pub(super) fn mapped_template_matches(
    shape: &MappedSequenceShape<'_>,
    parents: &[AttributeMap],
    current_parent: Option<&AttributeMap>,
    raw_parent: &DecodedItem,
    item_ordinal: usize,
    template: &str,
    expected: &str,
) -> bool {
    let mut remaining = expected;
    for part in ReadSequenceStringTemplateParts::new(template) {
        let value = match part {
            Ok(ReadSequenceStringTemplatePart::Literal(literal)) => literal,
            Ok(ReadSequenceStringTemplatePart::Input(name)) => {
                let Some(AttributeValue::S(value)) = shape
                    .inputs
                    .iter()
                    .find(|input| input.name == name)
                    .and_then(|input| {
                        mapped_input_value(input, parents, current_parent, raw_parent, item_ordinal)
                    })
                else {
                    return false;
                };
                value
            }
            Err(_) => return false,
        };
        let Some(suffix) = remaining.strip_prefix(value) else {
            return false;
        };
        remaining = suffix;
    }
    remaining.is_empty()
}

fn mapped_input_value<'a>(
    input: &MappedInput<'_>,
    parents: &'a [AttributeMap],
    current_parent: Option<&'a AttributeMap>,
    raw_parent: &'a DecodedItem,
    item_ordinal: usize,
) -> Option<&'a AttributeValue> {
    if matches!(input.item, MappedInputItem::Each) || item_ordinal == 0 {
        return current_parent.map_or_else(
            || raw_parent.get(input.attribute_name),
            |item| item.get(input.attribute_name),
        );
    }
    parents.first()?.get(input.attribute_name)
}

fn mapped_input_refs(
    shape: &MappedSequenceShape<'_>,
    item_ordinal: u32,
) -> BTreeMap<String, ReadSequenceInputReference> {
    shape
        .inputs
        .iter()
        .map(|input| {
            let source_item_ordinal = match input.item {
                MappedInputItem::Each => item_ordinal,
                MappedInputItem::First => 0,
            };
            (
                input.name.to_string(),
                ReadSequenceInputReference {
                    node: shape.parent_name.to_string(),
                    invocation_ordinal: 0,
                    item_ordinal: Some(source_item_ordinal),
                },
            )
        })
        .collect()
}

fn decode_mapped_child(
    key_values: &[storage_provider::ReadSequenceMappedKeyValue],
    capacity: MaxIndexers,
) -> StorageResult<Option<DecodedItem>> {
    if key_values.len() > 1 {
        return Err(StorageError::internal(
            "mapped point target returned more than one secondary value",
        ));
    }
    key_values
        .first()
        .map(|key_value| decode_storage_item(&key_value.value, capacity))
        .transpose()
}

fn mapped_child_row(
    child_id: ReadSequenceNodeId,
    input_refs: BTreeMap<String, ReadSequenceInputReference>,
    invocation_ordinal: u32,
    child: Option<AttributeMap>,
) -> ReadSequenceFlatRow {
    ReadSequenceFlatRow {
        node: child_id,
        invocation_ordinal,
        input_refs,
        result: ReadSequenceFlatResult::Get { item: child },
    }
}

pub(super) fn flatten_rows(
    rows_by_node: Vec<Vec<ReadSequenceFlatRow>>,
) -> Vec<ReadSequenceFlatRow> {
    let row_count = rows_by_node.iter().map(Vec::len).sum();
    let mut rows = Vec::with_capacity(row_count);
    for node_rows in rows_by_node {
        rows.extend(node_rows);
    }
    rows
}

fn decode_parent(bytes: &[u8], capacity: MaxIndexers) -> StorageResult<DecodedParent> {
    let indexed = decode_indexed_wire_item(
        crate::sorted_kv_store::ItemValueCodec::FoundationDbTuple,
        bytes,
    )?;
    validate_declaration_capacity(indexed.slots().len(), capacity)?;
    let (item, declaration) = indexed.into_attribute_map_with_declaration()?;
    Ok(DecodedParent { item, declaration })
}

pub(super) fn decode_storage_item(
    bytes: &[u8],
    capacity: MaxIndexers,
) -> StorageResult<DecodedItem> {
    let indexed = decode_indexed_wire_item(
        crate::sorted_kv_store::ItemValueCodec::FoundationDbTuple,
        bytes,
    )?;
    validate_declaration_capacity(indexed.slots().len(), capacity)?;
    indexed
        .into_attribute_map_with_declaration()
        .map(|(item, _)| item)
}

fn validate_declaration_capacity(len: usize, capacity: MaxIndexers) -> StorageResult<()> {
    if len > capacity.as_usize() {
        return Err(StorageError::internal(
            "stored_item_corruption:declaration_exceeds_table_capacity",
        ));
    }
    Ok(())
}
