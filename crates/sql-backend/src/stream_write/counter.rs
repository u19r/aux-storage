//! SQL stream sequence authority for Dynamo writes and individual user streams.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};

use super::sql_statements;
use crate::{ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlValue};

pub(crate) async fn allocate_stream_item_id<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    stream_name: &StreamName,
) -> StorageResult<StreamItemId> {
    let name = sql_statements::user_sequence_name(stream_name);
    // The returned ID is allocated by this write attempt. Rollback restores
    // the counter, and concurrent appends to one stream commit in ID order.
    let page = attempt
        .query(
            sql_statements::allocate_sequence(engine, &name),
            ReadProtection::Snapshot,
            storage_provider::CATALOG_READ_BYTES,
        )
        .await?;
    if page.rows.is_empty() {
        return Err(StorageError::internal(
            "SQL stream sequence is missing or exhausted",
        ));
    }
    let first = decode_counter(page)?;
    Ok(stream_provider::sequenced_stream_item_id(first))
}

pub(crate) fn decode_counter(page: SqlQueryPage) -> StorageResult<u64> {
    if page.more || page.rows.len() != 1 {
        return Err(StorageError::internal("SQL stream sequence is missing"));
    }
    let first = match page.rows[0].value(ColumnIndex::new(0))? {
        SqlValue::Integer(value) => u64::try_from(*value)
            .map_err(|_| StorageError::internal("SQL stream sequence is invalid"))?,
        _ => return Err(StorageError::internal("SQL stream sequence is invalid")),
    };
    if first == 0 {
        return Err(StorageError::internal("SQL stream sequence starts at zero"));
    }
    Ok(first)
}
