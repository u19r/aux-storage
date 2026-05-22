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

/// Apply a projection to an item. For KEYS_ONLY we only return the key
/// attributes. For INCLUDE we return only the listed non-key attributes (does
/// not *auto add* keys per current DynamoDB parity unless specified there). For
/// ALL or None we clone the whole item.
pub fn apply_projection(
    item: &HashMap<String, AttributeValue>,
    projection: Option<&Projection>,
    key_schema: &[KeySchemaElement],
) -> HashMap<String, AttributeValue> {
    let Some(proj) = projection else {
        return item.clone();
    };
    match proj.projection_type.clone().unwrap_or(ProjectionType::All) {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly => {
            let mut out = HashMap::with_capacity(key_schema.len());
            for k in key_schema {
                if let Some(v) = item.get(&k.attribute_name) {
                    out.insert(k.attribute_name.clone(), v.clone());
                }
            }
            out
        }
        ProjectionType::Include => {
            let mut out = HashMap::new();
            if let Some(attrs) = &proj.non_key_attributes {
                for a in attrs {
                    if let Some(v) = item.get(a) {
                        out.insert(a.clone(), v.clone());
                    }
                }
            }
            out
        }
    }
}

/// Apply a projection within the context of a GSI, ensuring BOTH the base table
/// primary key attributes and the GSI key attributes are retained for KeysOnly
/// / Include projections, matching DynamoDB semantics tested in kv provider.
pub fn apply_gsi_projection(
    item: &HashMap<String, AttributeValue>,
    projection: Option<&Projection>,
    table_key_schema: &[KeySchemaElement],
    gsi_key_schema: &[KeySchemaElement],
) -> HashMap<String, AttributeValue> {
    let Some(proj) = projection else {
        return item.clone();
    };
    match proj.projection_type.clone().unwrap_or(ProjectionType::All) {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly => {
            let mut out = HashMap::new();
            for k in table_key_schema.iter().chain(gsi_key_schema.iter()) {
                if let Some(v) = item.get(&k.attribute_name) {
                    out.entry(k.attribute_name.clone())
                        .or_insert_with(|| v.clone());
                }
            }
            out
        }
        ProjectionType::Include => {
            let mut out = HashMap::new();
            if let Some(attrs) = &proj.non_key_attributes {
                for a in attrs {
                    if let Some(v) = item.get(a) {
                        out.insert(a.clone(), v.clone());
                    }
                }
            }
            for k in table_key_schema.iter().chain(gsi_key_schema.iter()) {
                if let Some(v) = item.get(&k.attribute_name) {
                    out.entry(k.attribute_name.clone())
                        .or_insert_with(|| v.clone());
                }
            }
            out
        }
    }
}
