//! Key-only namespace discovery never transfers history payloads.

use storage_provider::maintenance::TableHistoryNamespace;
use storage_types::{ReadProtection, StorageError, StorageResult, StreamName, TableHistoryId};

use crate::{SqlAttempt, SqlLayout, SqlValue, constants::STREAM_READ_BYTES};

pub(super) async fn load_namespace(
    attempt: &mut impl SqlAttempt,
    engine: impl SqlLayout,
    history: TableHistoryId,
    after: Option<&[u8]>,
) -> StorageResult<Option<TableHistoryNamespace>> {
    let page = attempt
        .query(
            super::sql_statements::read_history_namespace(engine, history, after)?,
            ReadProtection::Conflict,
            STREAM_READ_BYTES,
        )
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal("Invalid history namespace page"));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    let [SqlValue::Blob(name)]: [SqlValue; 1] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("Invalid history namespace row"))?
    else {
        return Err(StorageError::internal("Invalid history namespace type"));
    };
    Ok(Some(TableHistoryNamespace {
        stream_name: StreamName::new(&name),
        cursor: name,
    }))
}
