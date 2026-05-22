//! Unified read path utilities for scan & query operations.
//!
//! Centralizes shared logic:
//! - Effective limit calculation (delegates to `helpers::normalize_limit`)
//! - Row -> `WireItem` mapping (handles main table vs GSI physical tables)
//! - Execution of a built SQL query with pagination (fetch limit+1)
//! - Construction of `LastEvaluatedKey` token
//!
//! This allows `scan_table` and `query_table` implementations in
//! `storage_provider.rs` to delegate the bulk of their work here, reducing
//! duplication and making future additions (`FilterExpression`,
//! `ProjectionExpression`) simpler.

use storage_types::{StoredTableInfo, WireItem};

use crate::key_attribute_handler::wire_item_key_attributes_from_row;

/// Mode for mapping rows: either the logical base table or a GSI physical table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOrigin {
    Main,
    Gsi,
}

/// Result of executing a unified read
pub struct UnifiedReadResult {
    pub items: Vec<WireItem>,
    pub last_evaluated_key: Option<String>,
}

/// Execute a prepared SQL query returning up to `limit` items (internally
/// fetches limit+1).
///
/// `key_schema_for_origin` must be the key schema of the physical table queried
/// (main or GSI).
#[expect(
    clippy::too_many_arguments,
    clippy::ref_option,
    reason = "Central read path signature maintained for consistency"
)]
pub fn execute_unified_read(
    conn: &rusqlite::Connection,
    sql: &str,
    values: &[String],
    table_info: &StoredTableInfo,
    origin: RowOrigin,
    key_schema_for_origin: &[storage_types::KeySchemaElement],
    effective_limit: u32,
    index_name: &Option<storage_types::IndexName>,
) -> Result<UnifiedReadResult, storage_types::StorageError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(crate::error_handler::map_sqlite_error)?;

    let rows = stmt
        .query_map(rusqlite::params_from_iter(values.iter()), |row| {
            let primary_key = match origin {
                RowOrigin::Main => wire_item_key_attributes_from_row(
                    row,
                    &table_info.key_schema,
                    &table_info.attribute_definitions,
                    None,
                )
                .map_err(|err| storage_error_to_rusqlite(&err))?,
                RowOrigin::Gsi => wire_item_key_attributes_from_row(
                    row,
                    key_schema_for_origin,
                    &table_info.attribute_definitions,
                    None,
                )
                .map_err(|err| storage_error_to_rusqlite(&err))?,
            };
            let secondary_key = match origin {
                RowOrigin::Main => None,
                RowOrigin::Gsi => Some(
                    wire_item_key_attributes_from_row(
                        row,
                        &table_info.key_schema,
                        &table_info.attribute_definitions,
                        Some("table_"),
                    )
                    .map_err(|err| storage_error_to_rusqlite(&err))?,
                ),
            };
            let non_key_attributes_blob = row
                .get::<_, Option<String>>("attributes_blob")?
                .map(String::into_bytes);
            Ok(WireItem::local_split(
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            ))
        })
        .map_err(crate::error_handler::map_sqlite_error)?;

    let mut items: Vec<WireItem> = Vec::with_capacity(effective_limit.saturating_add(1) as usize);
    for row in rows {
        items.push(row.map_err(crate::error_handler::map_sqlite_error)?);
    }

    let has_more = items.len() > effective_limit as usize;
    if has_more {
        items.pop();
    }

    let last_evaluated_key = if has_more {
        if let Some(last_item) = items.last() {
            last_item.last_evaluated_key(table_info, index_name)?
        } else {
            None
        }
    } else {
        None
    };

    Ok(UnifiedReadResult {
        items,
        last_evaluated_key,
    })
}

fn storage_error_to_rusqlite(err: &storage_types::StorageError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            err.to_string(),
        )),
    )
}
