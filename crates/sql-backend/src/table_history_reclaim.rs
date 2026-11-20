//! Bounded deletion of one immutable incarnation and its native feed index.

use storage_provider::TableReclaimStep;
use storage_types::{StorageError, StorageResult, TableHistoryId};

use crate::{SqlAttempt, SqlLayout, table_history_reclaim_sql_statements as sql_statements};

pub(crate) async fn delete_table_history_rows<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    history: TableHistoryId,
) -> StorageResult<TableReclaimStep> {
    match attempt
        .execute(sql_statements::delete_table_history_rows(engine, history))
        .await?
    {
        0..64 => Ok(TableReclaimStep::Complete),
        64 => Ok(TableReclaimStep::Advanced),
        _ => Err(StorageError::internal(
            "Table history deletion exceeded its physical row limit",
        )),
    }
}
