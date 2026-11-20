//! Canonical entry deletion is selected only by the immutable table ID.

use storage_types::TableHistoryId;

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn delete_table_history_rows(
    engine: impl SqlLayout,
    history: TableHistoryId,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {relation} WHERE table_id = {} AND (shard_id, lineage_id, offset_order, \
             offset_text) IN (SELECT shard_id, lineage_id, offset_order, offset_text FROM \
             {relation} WHERE table_id = {} ORDER BY shard_id, lineage_id, offset_order, \
             offset_text LIMIT 64)",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation = engine.relation_name("sys_stream_entries")
        ),
        vec![
            SqlValue::Blob(history.as_ref().to_vec()),
            SqlValue::Blob(history.as_ref().to_vec()),
        ],
    )
}
