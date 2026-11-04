//! Table deletion preserves source positions while removing old-ID references.

use std::num::NonZeroU32;

use storage_provider::{CATALOG_READ_BYTES, TableReclaimStep, TableSystemHistoryPage};
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, SystemHistoryRecord, TableHistoryId,
};

use crate::{SqlAttempt, SqlLayout, table_history_reclaim_sql_statements as sql_statements};

pub(crate) async fn load_table_system_rows<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    history: TableHistoryId,
    limit: NonZeroU32,
) -> StorageResult<TableSystemHistoryPage> {
    let page = attempt
        .query(
            sql_statements::read_table_system_rows(engine, history, limit),
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    let more = page.more || page.rows.len() > limit.get() as usize;
    let records = page
        .rows
        .into_iter()
        .take(limit.get() as usize)
        .map(crate::stream_read::row::decode_history_row)
        .map(|result| result.map(|(record, _)| record))
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(TableSystemHistoryPage { records, more })
}

pub(crate) async fn store_table_system_removals<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    history: TableHistoryId,
    rows: &[SystemHistoryRecord],
) -> StorageResult<()> {
    let statements = rows
        .iter()
        .map(|row| sql_statements::store_table_system_removal(engine, history, row))
        .collect::<StorageResult<Vec<_>>>()?;
    let counts = attempt.execute_batch(statements).await?;
    if counts.len() != rows.len() || counts.iter().any(|count| *count != 1) {
        return Err(StorageEnum::TransactionConflict {
            message: "Source history changed before table cleanup".to_owned(),
        }
        .into());
    }
    Ok(())
}

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
