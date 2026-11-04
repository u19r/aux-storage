//! Table-ID cleanup inventory and final descriptor deletion.

use storage_provider::TableReclaimTarget;
use storage_types::{StorageResult, TableHistoryId, storage_serde};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn schema(engine: impl SqlLayout) -> SqlStatement {
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS {} (history_id {} PRIMARY KEY, payload {} NOT NULL){}",
            engine.relation_name("sys_table_reclaim"),
            engine.engine().text_type(),
            engine.engine().blob_type(),
            engine.engine().table_suffix()
        ),
        vec![],
    )
}

pub(crate) fn load_target(engine: impl SqlLayout) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT history_id, payload FROM {} ORDER BY history_id LIMIT 1",
            engine.relation_name("sys_table_reclaim")
        ),
        vec![],
    )
}

pub(crate) fn store_target(
    engine: impl SqlLayout,
    target: &TableReclaimTarget,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "UPDATE {} SET payload = {} WHERE history_id = {}",
            engine.relation_name("sys_table_reclaim"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2)
        ),
        vec![
            SqlValue::Blob(storage_serde::to_bytes(target)?),
            SqlValue::Text(uuid::Uuid::from_bytes(*target.history_id.as_ref()).to_string()),
        ],
    ))
}

pub(crate) fn finish_target(engine: impl SqlLayout, history: TableHistoryId) -> [SqlStatement; 2] {
    let id = uuid::Uuid::from_bytes(*history.as_ref()).to_string();
    [
        SqlStatement::new(
            format!(
                "DELETE FROM {} WHERE id = {} AND table_status = 'DELETING'",
                engine.relation_name("tables"),
                engine.engine().placeholder(1)
            ),
            vec![SqlValue::Text(id.clone())],
        ),
        SqlStatement::new(
            format!(
                "DELETE FROM {} WHERE history_id = {}",
                engine.relation_name("sys_table_reclaim"),
                engine.engine().placeholder(1)
            ),
            vec![SqlValue::Text(id)],
        ),
    ]
}

pub(crate) fn load_external_cleanup_requirement(
    engine: impl SqlLayout,
    history: TableHistoryId,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT external_cleanup_required FROM {} WHERE id = {} AND table_status <> 'DELETING'",
            engine.relation_name("tables"),
            engine.engine().placeholder(1)
        ),
        vec![SqlValue::Text(
            uuid::Uuid::from_bytes(*history.as_ref()).to_string(),
        )],
    )
}
