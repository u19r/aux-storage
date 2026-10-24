//! Projection filtering utilities shared by backends.
//!
//! There are two related helpers:
//! * `apply_projection` – applies a projection in the context of a normal table
//!   item (no secondary index context). For `KeysOnly` it returns only the
//!   table's primary key attributes. For `Include` it returns only the
//!   explicitly listed non-key attributes (does not implicitly add keys –
//!   higher layers decide if keys must always be present).
//! * `apply_gsi_projection` – applies a projection when materializing / reading
//!   a Global Secondary Index (GSI). DynamoDB semantics require that GSI items
//!   for `KeysOnly` and `Include` projections always contain BOTH the base
//!   table's primary key attributes and the index key attributes (plus any
//!   explicitly included non-key attributes for `Include`). This helper
//!   enforces that union.
//!
//! The separation keeps the simpler table projection logic minimal while making
//! the GSI variant explicit and self‑documenting.
use std::collections::HashMap;

use storage_types::{AttributeValue, KeySchemaElement, Projection, ProjectionType};

/// Apply a projection within the context of a GSI, ensuring BOTH the base table
/// primary key attributes and the GSI key attributes are retained for KeysOnly
/// / Include projections, matching DynamoDB semantics tested in kv provider.
pub fn apply_gsi_projection(
    item: &HashMap<String, AttributeValue>,
    projection: Option<&Projection>,
    table_key_schema: &[KeySchemaElement],
    gsi_key_schema: &[KeySchemaElement],
) -> HashMap<String, AttributeValue> {
    let Some(projection) = projection else {
        return item.clone();
    };
    if projection
        .projection_type
        .as_ref()
        .is_none_or(|kind| *kind == ProjectionType::All)
    {
        return item.clone();
    }

    gsi_projection_fields(Some(projection), table_key_schema, gsi_key_schema)
        .into_iter()
        .filter_map(|name| item.get(name).map(|value| (name.to_owned(), value.clone())))
        .collect()
}

/// Select the logical fields retained by a GSI projection. Keys are emitted
/// first, with table keys before index keys; INCLUDE attributes follow them.
/// Duplicate names are kept once, which also handles table/index key overlap.
pub fn gsi_projection_fields<'a>(
    projection: Option<&'a Projection>,
    table_key_schema: &'a [KeySchemaElement],
    gsi_key_schema: &'a [KeySchemaElement],
) -> Vec<&'a str> {
    let mut fields = Vec::with_capacity(
        table_key_schema.len()
            + gsi_key_schema.len()
            + projection
                .and_then(|projection| projection.non_key_attributes.as_ref())
                .map_or(0, Vec::len),
    );
    for key in table_key_schema.iter().chain(gsi_key_schema) {
        push_unique(&mut fields, key.attribute_name.as_str());
    }
    if projection
        .is_some_and(|projection| projection.projection_type == Some(ProjectionType::Include))
    {
        for name in projection
            .and_then(|projection| projection.non_key_attributes.as_deref())
            .unwrap_or_default()
        {
            push_unique(&mut fields, name);
        }
    }
    fields
}

fn push_unique<'a>(fields: &mut Vec<&'a str>, name: &'a str) {
    if !fields.contains(&name) {
        fields.push(name);
    }
}
