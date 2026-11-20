//! Immutable table-incarnation catalog facts within the caller's SQL snapshot.

use storage_types::{StorageError, StorageResult, TableHistoryId, TableHistoryState};

use crate::{ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlRow, SqlValue};

pub(crate) async fn load_history_table_states<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    histories: &[TableHistoryId],
) -> StorageResult<Vec<Option<TableHistoryState>>> {
    if histories.is_empty() {
        return Ok(Vec::new());
    }
    let pages = attempt
        .query_batch(
            Vec::from(super::sql_statements::read_history_table_states(
                engine, histories,
            )),
            protection,
            storage_provider::CATALOG_READ_BYTES,
        )
        .await?;
    if pages.len() != 2
        || pages.iter().any(|page| page.more)
        || pages.iter().map(|page| page.rows.len()).sum::<usize>() > histories.len()
    {
        return Err(invalid_catalog_read());
    }
    let mut states = histories
        .iter()
        .map(|history| (*history, None))
        .collect::<std::collections::HashMap<_, _>>();
    for row in pages.into_iter().flat_map(|page| page.rows) {
        let (history, state) = decode_history_table_state(row)?;
        let slot = states.get_mut(&history).ok_or_else(invalid_catalog_read)?;
        if slot.replace(state).is_some() {
            return Err(invalid_catalog_read());
        }
    }
    Ok(histories
        .iter()
        .map(|history| states.get(history).cloned().flatten())
        .collect())
}

fn decode_history_table_state(row: SqlRow) -> StorageResult<(TableHistoryId, TableHistoryState)> {
    let SqlValue::Text(id) = row.value(ColumnIndex::new(0))? else {
        return Err(invalid_catalog_read());
    };
    let history = uuid::Uuid::parse_str(id)
        .map_err(|_| invalid_catalog_read())?
        .into();
    let state = match row.value(ColumnIndex::new(1))? {
        SqlValue::Integer(0) => {
            let (SqlValue::Text(name), SqlValue::Text(schema)) = (
                row.value(ColumnIndex::new(2))?,
                row.value(ColumnIndex::new(3))?,
            ) else {
                return Err(invalid_catalog_read());
            };
            let schema = storage_types::TableHistorySchema::try_from((
                storage_types::TableName::new(name),
                serde_json::from_str(schema)?,
            ))?;
            TableHistoryState::Active(std::sync::Arc::new(schema))
        }
        SqlValue::Integer(1) => TableHistoryState::Deleting,
        _ => return Err(invalid_catalog_read()),
    };
    Ok((history, state))
}

fn invalid_catalog_read() -> StorageError {
    StorageError::internal("Invalid SQL table history catalog read")
}
