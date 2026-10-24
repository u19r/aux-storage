use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{AttributeDefinition, KeyAttributeType, KeyType};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct KeySchemaElement {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,

    #[serde(rename = "KeyType")]
    pub key_type: KeyType,
}

impl KeySchemaElement {
    /// Physical key order is HASH then RANGE, independent of request field
    /// order.
    pub fn ordered(schema: &[Self]) -> impl Iterator<Item = &Self> {
        [KeyType::Hash, KeyType::Range]
            .into_iter()
            .flat_map(move |kind| {
                schema
                    .iter()
                    .filter(move |element| element.key_type == kind)
            })
    }
}

/// One deduplicated key column resolved against attribute definitions.
/// Backends map [`PlannedKeyColumn::attribute_type`] to their column type and
/// emit their own dialect; the ordering and deduplication below is shared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedKeyColumn {
    pub attribute_name: String,
    pub attribute_type: KeyAttributeType,
}

impl From<&AttributeDefinition> for PlannedKeyColumn {
    fn from(definition: &AttributeDefinition) -> Self {
        Self {
            attribute_name: definition.attribute_name.clone(),
            attribute_type: definition.attribute_type,
        }
    }
}

/// Ordered, deduplicated key columns across the table key schema followed by
/// the given GSI schemas, resolved against attribute definitions. Keys
/// without definitions are skipped. Callers planning one GSI table pass its
/// schema (plus the table schema for `table_`-prefixed columns) as separate
/// calls to avoid cross-table deduplication.
pub fn planned_key_columns<'a>(
    table_key_schema: &'a [KeySchemaElement],
    gsi_key_schemas: impl IntoIterator<Item = &'a [KeySchemaElement]>,
    attribute_definitions: &'a [AttributeDefinition],
) -> Vec<PlannedKeyColumn> {
    let mut planned = Vec::new();
    let mut seen = HashSet::new();
    for key_element in table_key_schema
        .iter()
        .chain(gsi_key_schemas.into_iter().flatten())
    {
        if !seen.insert(key_element.attribute_name.clone()) {
            continue;
        }
        if let Some(definition) = attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == key_element.attribute_name)
        {
            planned.push(definition.into());
        }
    }
    planned
}
