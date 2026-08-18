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

use storage_types::{StorageError, StoredTableInfo};

use crate::indexed_item::SqlDecodedItem;

/// Mode for mapping rows: either the logical base table or a GSI physical table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOrigin {
    Main,
    Gsi,
}

/// Result of executing a unified read
pub struct UnifiedReadResult {
    pub items: Vec<SqlDecodedItem>,
    pub last_evaluated_key: Option<String>,
}

/// Execute a prepared SQL query returning up to `limit` items (internally
/// fetches limit+1).
#[expect(
    clippy::ref_option,
    reason = "Central read path signature maintained for consistency"
)]
pub fn execute_unified_read(
    conn: &rusqlite::Connection,
    sql: &str,
    values: &[String],
    table_info: &StoredTableInfo,
    origin: RowOrigin,
    effective_limit: u32,
    index_name: &Option<storage_types::IndexName>,
) -> Result<UnifiedReadResult, storage_types::StorageError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(crate::error_handler::map_sqlite_error)?;

    let rows = stmt
        .query_map(rusqlite::params_from_iter(values.iter()), |row| {
            let prefix = matches!(origin, RowOrigin::Gsi).then_some("table_");
            crate::indexed_item::sqlite_row_to_decoded_item(row, table_info, prefix)
        })
        .map_err(crate::error_handler::map_sqlite_error)?;

    let mut items = Vec::with_capacity(effective_limit.saturating_add(1) as usize);
    for row in rows {
        items.push(row.map_err(crate::error_handler::map_sqlite_error)?);
    }

    if matches!(origin, RowOrigin::Gsi) {
        let index_name = index_name
            .as_ref()
            .ok_or_else(|| StorageError::internal("GSI read target is missing its index name"))?;
        crate::indexed_item::project_gsi_decoded_items(&mut items, table_info, Some(index_name))?;
    }

    let has_more = items.len() > effective_limit as usize;
    if has_more {
        items.pop();
    }

    let last_evaluated_key = if has_more {
        if let Some(last_item) = items.last() {
            last_item.item.last_evaluated_key(table_info, index_name)?
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
