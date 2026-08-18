use storage_provider::ReadSequenceUnsupportedReason;
use storage_types::{KeySchemaElement, KeyType, ProjectionType, StorageError, StorageResult};

use crate::{
    keyspace::{
        table_identity::StoredTableMetadata,
        tuple_keys::{TupleKeyElement, TupleMapperElement, item_mapper_elements},
    },
    storage_ops::provider_impl::read_sequence_mapped::{
        MappedGetQueryShape, MappedSequenceShape, bindings::MappedKeyBinding,
    },
};

pub(super) struct MappedPhysicalLayout {
    pub(super) mapper: Option<Vec<u8>>,
    pub(super) same_item: bool,
}

#[derive(Clone)]
struct SourceKey<'a> {
    name: &'a str,
    element: TupleMapperElement,
}

pub(super) fn mapped_physical_layout(
    shape: &MappedSequenceShape<'_>,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
) -> StorageResult<Result<MappedPhysicalLayout, ReadSequenceUnsupportedReason>> {
    let source_keys = source_keys(shape, parent)?;
    let target_schema = &child.table_info.key_schema;
    if shape.keys.len() != target_schema.len() {
        return Ok(Err(ReadSequenceUnsupportedReason::OperationShape));
    }
    let Some(target_elements) = target_elements(shape, parent, child, &source_keys)? else {
        return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
    };
    let same_item = is_same_item(shape, parent, child, &source_keys, &target_elements);
    if same_item {
        validate_gsi_child_projection(shape, parent)?;
    }
    let mapper = if same_item {
        None
    } else {
        Some(item_mapper_elements(
            child.identity.table_id,
            &target_elements[0],
            target_elements.get(1),
        )?)
    };
    Ok(Ok(MappedPhysicalLayout { mapper, same_item }))
}

pub(super) fn mapped_get_query_physical_layout(
    shape: &MappedGetQueryShape<'_>,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
) -> StorageResult<Result<MappedPhysicalLayout, ReadSequenceUnsupportedReason>> {
    if shape.child_query.index_name.is_some() {
        return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
    }
    let Some(hash_key) = child
        .table_info
        .key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
    else {
        return Ok(Err(ReadSequenceUnsupportedReason::OperationShape));
    };
    if shape.keys.len() != 1 || shape.keys[0].target_name != hash_key.attribute_name {
        return Ok(Err(ReadSequenceUnsupportedReason::OperationShape));
    }
    let source_keys = primary_source_keys(parent);
    let binding = &shape.keys[0];
    let element = if let Some(input_name) = binding.direct_input
        && let Some(input) = shape.inputs.iter().find(|input| input.name == input_name)
        && let Some(ordinal) = input.indexer
    {
        if input.attribute_name != binding.source_name
            || attribute_type(child, &hash_key.attribute_name) != Some("S")
        {
            return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
        }
        TupleMapperElement::Value(storage_types::indexer_tuple_index(usize::from(ordinal)))
    } else if let Some(literal) = binding.literal {
        if attribute_type(child, &hash_key.attribute_name) != Some(attribute_value_type(literal)) {
            return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
        }
        TupleMapperElement::Literal(literal.clone())
    } else {
        let Some(source) = source_keys
            .iter()
            .find(|key| key.name == binding.source_name)
        else {
            return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
        };
        if !key_types_match(
            parent,
            child,
            binding,
            source.name,
            &hash_key.attribute_name,
        ) {
            return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
        }
        source.element.clone()
    };
    let mapper = crate::keyspace::tuple_keys::item_partition_mapper_elements(
        child.identity.table_id,
        &element,
    )?;
    Ok(Ok(MappedPhysicalLayout {
        mapper: Some(mapper),
        same_item: false,
    }))
}

fn source_keys<'a>(
    shape: &MappedSequenceShape<'_>,
    parent: &'a StoredTableMetadata,
) -> StorageResult<Vec<SourceKey<'a>>> {
    let mut keys = Vec::with_capacity(4);
    if let Some(index_name) = shape.index_name {
        // The mapper sees the configured physical-prefix tuple element before
        // the logical key.  GSI rows therefore have the base hash/range pairs
        // at tags 10/12 and the GSI pairs at tags 6/8.
        append_source_schema(&mut keys, &parent.table_info.key_schema, 10, 12);
        if let Some(schema) = parent
            .table_info
            .global_secondary_indexes
            .as_ref()
            .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
            .map(|index| index.key_schema.as_slice())
        {
            append_source_schema(&mut keys, schema, 6, 8);
        }
    } else {
        keys.extend(primary_source_keys(parent));
    }
    if keys.is_empty() {
        return Err(StorageError::internal(
            "mapped source has no physical key schema",
        ));
    }
    Ok(keys)
}

fn primary_source_keys<'a>(parent: &'a StoredTableMetadata) -> Vec<SourceKey<'a>> {
    let mut keys = Vec::with_capacity(2);
    // The mapper sees the configured physical-prefix tuple element before the
    // logical key, so primary rows have hash/range pairs at tags 5/7.
    append_source_schema(&mut keys, &parent.table_info.key_schema, 5, 7);
    keys
}

fn append_source_schema<'a>(
    output: &mut Vec<SourceKey<'a>>,
    schema: &'a [KeySchemaElement],
    hash_tag: usize,
    range_tag: usize,
) {
    for key in schema {
        let tag = if key.key_type == KeyType::Hash {
            hash_tag
        } else {
            range_tag
        };
        output.push(SourceKey {
            name: &key.attribute_name,
            element: TupleMapperElement::Key(TupleKeyElement {
                tag,
                value: tag + 1,
            }),
        });
    }
}

fn target_elements(
    shape: &MappedSequenceShape<'_>,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
    source_keys: &[SourceKey<'_>],
) -> StorageResult<Option<Vec<TupleMapperElement>>> {
    let mut elements = Vec::with_capacity(child.table_info.key_schema.len());
    for target in ordered_schema(&child.table_info.key_schema) {
        let Some(binding) = shape
            .keys
            .iter()
            .find(|key| key.target_name == target.attribute_name)
        else {
            return Ok(None);
        };
        if let Some(input_name) = binding.direct_input
            && let Some(input) = shape.inputs.iter().find(|input| input.name == input_name)
            && let Some(ordinal) = input.indexer
        {
            if input.attribute_name != binding.source_name
                || attribute_type(child, &target.attribute_name) != Some("S")
            {
                return Ok(None);
            }
            elements.push(TupleMapperElement::Value(
                storage_types::indexer_tuple_index(usize::from(ordinal)),
            ));
            continue;
        }
        if let Some(literal) = binding.literal {
            if attribute_type(child, &target.attribute_name) != Some(attribute_value_type(literal))
            {
                return Ok(None);
            }
            elements.push(TupleMapperElement::Literal(literal.clone()));
            continue;
        }
        let Some(source) = source_keys
            .iter()
            .find(|key| key.name == binding.source_name)
        else {
            return Ok(None);
        };
        if !key_types_match(parent, child, binding, source.name, &target.attribute_name) {
            return Ok(None);
        }
        elements.push(source.element.clone());
    }
    Ok(Some(elements))
}

fn ordered_schema(schema: &[KeySchemaElement]) -> impl Iterator<Item = &KeySchemaElement> {
    [KeyType::Hash, KeyType::Range]
        .into_iter()
        .filter_map(|kind| schema.iter().find(|key| key.key_type == kind))
}

fn key_types_match(
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
    binding: &MappedKeyBinding<'_>,
    source_name: &str,
    target_name: &str,
) -> bool {
    let source_type = attribute_type(parent, source_name);
    source_type.is_some()
        && source_type == attribute_type(child, target_name)
        && (binding.template.is_none() || source_type == Some("S"))
}

fn attribute_type(metadata: &StoredTableMetadata, name: &str) -> Option<&'static str> {
    metadata
        .table_info
        .attribute_definitions
        .iter()
        .find(|attribute| attribute.attribute_name == name)
        .map(|attribute| match attribute.attribute_type {
            storage_types::KeyAttributeType::S => "S",
            storage_types::KeyAttributeType::N => "N",
            storage_types::KeyAttributeType::B => "B",
        })
}

fn attribute_value_type(value: &storage_types::AttributeValue) -> &'static str {
    match value {
        storage_types::AttributeValue::S(_) => "S",
        storage_types::AttributeValue::N(_) => "N",
        storage_types::AttributeValue::B(_) => "B",
        _ => "",
    }
}

fn is_same_item(
    shape: &MappedSequenceShape<'_>,
    parent: &StoredTableMetadata,
    child: &StoredTableMetadata,
    source_keys: &[SourceKey<'_>],
    target_elements: &[TupleMapperElement],
) -> bool {
    if parent.identity.table_id != child.identity.table_id {
        return false;
    }
    ordered_schema(&parent.table_info.key_schema)
        .zip(ordered_schema(&child.table_info.key_schema))
        .zip(target_elements)
        .all(|((source, target), element)| {
            source.attribute_name == target.attribute_name
                && matches!(element, TupleMapperElement::Key(_))
                && source_keys
                    .iter()
                    .any(|key| key.name == source.attribute_name && &key.element == element)
                && shape.keys.iter().any(|binding| {
                    binding.target_name == target.attribute_name
                        && binding.source_name == source.attribute_name
                })
        })
}

fn validate_gsi_child_projection(
    shape: &MappedSequenceShape<'_>,
    parent: &StoredTableMetadata,
) -> StorageResult<()> {
    let Some(index_name) = shape.index_name else {
        return Ok(());
    };
    let index = parent
        .table_info
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
        .ok_or_else(|| StorageError::table_not_found(&shape.parent_query.table_name))?;
    if index
        .projection
        .projection_type
        .as_ref()
        .is_none_or(|projection| *projection == ProjectionType::All)
    {
        return Ok(());
    }
    if shape.child_get.projection_expression.is_none()
        && shape.child_get.attributes_to_get.is_none()
    {
        return Err(StorageError::validation(format!(
            "Global secondary index {index_name} does not contain the full child item"
        )));
    }
    storage_types::validate_gsi_projection(
        &parent.table_info,
        Some(index_name),
        shape.child_get.projection_expression.as_deref(),
        shape.child_get.attributes_to_get.as_deref(),
        shape.child_get.expression_attribute_names.as_ref(),
    )
}
