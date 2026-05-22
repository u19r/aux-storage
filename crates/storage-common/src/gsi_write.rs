use std::collections::HashMap;

use smallvec::SmallVec;
use storage_types::{
    AttributeValue, GlobalSecondaryIndex, KeySchemaElement, Projection, ProjectionType,
    StorageError, StorageResult, StoredTableInfo,
};

use crate::{apply_gsi_projection, ttl::is_ttl_index};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GsiKeyPart<'a> {
    pub name: &'a str,
    pub value: &'a AttributeValue,
}

pub type GsiKeyParts<'a> = SmallVec<[GsiKeyPart<'a>; 2]>;

#[derive(Clone, Debug)]
pub enum GsiWriteAction<'a> {
    Delete {
        index: &'a GlobalSecondaryIndex,
        gsi_key: GsiKeyParts<'a>,
        table_key: GsiKeyParts<'a>,
    },
    Put {
        index: &'a GlobalSecondaryIndex,
        gsi_key: GsiKeyParts<'a>,
        table_key: GsiKeyParts<'a>,
        projected_item: HashMap<String, AttributeValue>,
    },
}

pub fn plan_gsi_write_actions<'a>(
    table_info: &'a StoredTableInfo,
    old_item: Option<&'a HashMap<String, AttributeValue>>,
    new_item: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<Vec<GsiWriteAction<'a>>> {
    let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
        return Ok(Vec::new());
    };

    let mut actions = Vec::with_capacity(gsis.len().saturating_mul(2));
    for gsi in gsis.iter().filter(|gsi| !is_ttl_index(&gsi.index_name)) {
        let old_gsi_key = key_parts(&gsi.key_schema, old_item);
        let old_table_key = key_parts(&table_info.key_schema, old_item);
        let new_gsi_key = key_parts(&gsi.key_schema, new_item);
        let new_table_key = key_parts(&table_info.key_schema, new_item);

        if let (Some(gsi_key), Some(table_key)) = (old_gsi_key.as_ref(), old_table_key.as_ref())
            && new_gsi_key.as_ref() != Some(gsi_key)
        {
            actions.push(GsiWriteAction::Delete {
                index: gsi,
                gsi_key: gsi_key.clone(),
                table_key: table_key.clone(),
            });
        }

        let (Some(item), Some(gsi_key), Some(table_key)) =
            (new_item, new_gsi_key.as_ref(), new_table_key.as_ref())
        else {
            continue;
        };
        if let (Some(old), Some(old_gsi_key)) = (old_item, old_gsi_key.as_ref())
            && old_gsi_key == gsi_key
            && !projected_item_changed(
                old,
                item,
                Some(&gsi.projection),
                &table_info.key_schema,
                &gsi.key_schema,
            )
        {
            continue;
        }

        actions.push(GsiWriteAction::Put {
            index: gsi,
            gsi_key: gsi_key.clone(),
            table_key: table_key.clone(),
            projected_item: apply_gsi_projection(
                item,
                Some(&gsi.projection),
                &table_info.key_schema,
                &gsi.key_schema,
            ),
        });
    }

    Ok(actions)
}

fn projected_item_changed(
    old_item: &HashMap<String, AttributeValue>,
    new_item: &HashMap<String, AttributeValue>,
    projection: Option<&Projection>,
    table_key_schema: &[KeySchemaElement],
    gsi_key_schema: &[KeySchemaElement],
) -> bool {
    let Some(projection) = projection else {
        return old_item != new_item;
    };
    match projection
        .projection_type
        .as_ref()
        .unwrap_or(&ProjectionType::All)
    {
        ProjectionType::All => old_item != new_item,
        ProjectionType::KeysOnly => key_schema_attributes_changed(
            old_item,
            new_item,
            table_key_schema.iter().chain(gsi_key_schema),
        ),
        ProjectionType::Include => {
            if key_schema_attributes_changed(old_item, new_item, table_key_schema.iter()) {
                return true;
            }
            if key_schema_attributes_changed(old_item, new_item, gsi_key_schema.iter()) {
                return true;
            }
            projection
                .non_key_attributes
                .as_deref()
                .is_some_and(|attributes| named_attributes_changed(old_item, new_item, attributes))
        }
    }
}

fn key_schema_attributes_changed<'a>(
    old_item: &HashMap<String, AttributeValue>,
    new_item: &HashMap<String, AttributeValue>,
    mut attributes: impl Iterator<Item = &'a KeySchemaElement>,
) -> bool {
    attributes.any(|attribute| {
        old_item.get(&attribute.attribute_name) != new_item.get(&attribute.attribute_name)
    })
}

fn named_attributes_changed(
    old_item: &HashMap<String, AttributeValue>,
    new_item: &HashMap<String, AttributeValue>,
    attributes: &[String],
) -> bool {
    attributes
        .iter()
        .any(|attribute| old_item.get(attribute) != new_item.get(attribute))
}

pub fn key_parts<'a>(
    key_schema: &'a [KeySchemaElement],
    item: Option<&'a HashMap<String, AttributeValue>>,
) -> Option<GsiKeyParts<'a>> {
    let item = item?;
    let mut parts = GsiKeyParts::new();
    for key in key_schema {
        let value = item.get(&key.attribute_name)?;
        parts.push(GsiKeyPart {
            name: &key.attribute_name,
            value,
        });
    }
    Some(parts)
}

pub fn key_parts_to_map(parts: &[GsiKeyPart<'_>]) -> HashMap<String, AttributeValue> {
    parts
        .iter()
        .map(|part| (part.name.to_string(), part.value.clone()))
        .collect()
}

pub fn require_key_parts<'a>(
    key_schema: &'a [KeySchemaElement],
    item: &'a HashMap<String, AttributeValue>,
) -> StorageResult<GsiKeyParts<'a>> {
    key_parts(key_schema, Some(item)).ok_or_else(StorageError::invalid_or_missing_key)
}
