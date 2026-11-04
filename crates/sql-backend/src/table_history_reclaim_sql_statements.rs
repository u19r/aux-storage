//! ID-indexed source redaction and bounded history-row deletion.

use std::num::NonZeroU32;

use storage_types::{
    StorageError, StorageResult, SystemHistoryRecord, TableHistoryId, storage_serde,
};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn read_table_system_rows(
    engine: impl SqlLayout,
    history: TableHistoryId,
    limit: NonZeroU32,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT shard_id, lineage_id, offset_order, offset_text, table_id, record_blob, \
             created_at FROM {} WHERE table_id = {} ORDER BY shard_id, lineage_id, offset_order, \
             offset_text LIMIT {}",
            engine.relation_name("sys_dynamo_stream_records"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2)
        ),
        vec![
            SqlValue::Blob(history.as_ref().to_vec()),
            SqlValue::Integer(i64::from(limit.get()) + 1),
        ],
    )
}

pub(crate) fn store_table_system_removal(
    engine: impl SqlLayout,
    history: TableHistoryId,
    record: &SystemHistoryRecord,
) -> StorageResult<SqlStatement> {
    if record.pointer().is_some() {
        return Err(StorageError::internal(
            "Source removal still contains a table reference",
        ));
    }
    let position = record.position();
    Ok(SqlStatement::new(
        format!(
            "UPDATE {} SET table_id = NULL, record_blob = {} WHERE table_id = {} AND shard_id = \
             {} AND lineage_id = {} AND offset_order = {} AND offset_text = {} AND created_at = {}",
            engine.relation_name("sys_dynamo_stream_records"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5),
            engine.engine().placeholder(6),
            engine.engine().placeholder(7)
        ),
        vec![
            SqlValue::Blob(storage_serde::to_bytes(record)?),
            SqlValue::Blob(history.as_ref().to_vec()),
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(
                position
                    .offset()
                    .order()
                    .map_err(|_| StorageError::internal("Invalid source removal position"))?
                    .to_be_bytes()
                    .to_vec(),
            ),
            SqlValue::Text(position.offset().as_str().to_owned()),
            SqlValue::Integer(record.created_at().timestamp_millis()),
        ],
    ))
}

pub(crate) fn delete_table_history_rows(
    engine: impl SqlLayout,
    history: TableHistoryId,
) -> SqlStatement {
    let begin = history.stream_prefix();
    let mut end = begin.clone();
    // Canonical history namespaces end in '/'; '0' is the exclusive upper
    // bound.
    let _ = end.pop();
    end.push(b'0');
    SqlStatement::new(
        format!(
            "DELETE FROM {relation} WHERE (stream_name, item_id) IN (SELECT stream_name, item_id \
             FROM {relation} WHERE stream_name >= {} AND stream_name < {} ORDER BY stream_name, \
             item_id LIMIT 64)",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation = engine.relation_name("sys_dynamo_stream_items")
        ),
        vec![SqlValue::Blob(begin), SqlValue::Blob(end)],
    )
}
