//! Physical retention schema and statements for all SQL drivers.

use storage_provider::{StreamRetentionFinishRecord, StreamTrimDueRecord, StreamTrimStateRecord};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(super) fn schema(engine: impl SqlLayout) -> [SqlStatement; 3] {
    let int = engine.engine().integer_type();
    let blob = engine.engine().blob_type();
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_stream_trim_state} (scope_id TEXT PRIMARY \
             KEY, state_blob {blob} NOT NULL, updated_at {int} NOT NULL)",
            relation_sys_stream_trim_state = engine.relation_name("sys_stream_trim_state"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_stream_trim_due_markers} (due_at {int} NOT \
             NULL, scope_id TEXT NOT NULL, policy_version {int} NOT NULL, marker_blob {blob} NOT \
             NULL, PRIMARY KEY (due_at, scope_id, policy_version))",
            relation_sys_stream_trim_due_markers =
                engine.relation_name("sys_stream_trim_due_markers"),
        ),
        crate::sql_template::sql_template!(
            engine,
            "CREATE INDEX IF NOT EXISTS "["idx_stream_trim_due_markers_scope"],
            " ON "["sys_stream_trim_due_markers"],
            "(scope_id)"
        )
        .into_owned(),
    ]
    .map(|sql| SqlStatement::new(sql, vec![]))
}

pub(super) fn save_state(engine: impl SqlLayout, record: StreamTrimStateRecord) -> SqlStatement {
    let sql = format!(
        "INSERT INTO {relation_sys_stream_trim_state} (scope_id, state_blob, updated_at) VALUES \
         ({}) ON CONFLICT(scope_id) DO UPDATE SET state_blob = excluded.state_blob, updated_at = \
         excluded.updated_at",
        engine.engine().placeholders(3),
        relation_sys_stream_trim_state = engine.relation_name("sys_stream_trim_state"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(record.scope_id),
            SqlValue::Blob(record.state_blob),
            SqlValue::Integer(record.updated_at),
        ],
    )
}

pub(super) fn save_marker(engine: impl SqlLayout, record: StreamTrimDueRecord) -> SqlStatement {
    let sql = format!(
        "INSERT INTO {relation_sys_stream_trim_due_markers} (due_at, scope_id, policy_version, \
         marker_blob) VALUES ({}) ON CONFLICT(due_at, scope_id, policy_version) DO UPDATE SET \
         marker_blob = excluded.marker_blob",
        engine.engine().placeholders(4),
        relation_sys_stream_trim_due_markers = engine.relation_name("sys_stream_trim_due_markers"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Integer(record.due_at),
            SqlValue::Text(record.scope_id),
            SqlValue::Integer(record.policy_version),
            SqlValue::Blob(record.marker_blob),
        ],
    )
}

pub(super) fn remove_marker(engine: impl SqlLayout, record: StreamTrimDueRecord) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {} WHERE due_at = {} AND scope_id = {} AND policy_version = {}",
            engine.relation_name("sys_stream_trim_due_markers"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
        ),
        vec![
            SqlValue::Integer(record.due_at),
            SqlValue::Text(record.scope_id),
            SqlValue::Integer(record.policy_version),
        ],
    )
}

pub(super) fn due_markers(engine: impl SqlLayout, due_before: i64, limit: i64) -> SqlStatement {
    let sql = format!(
        "SELECT due_at, scope_id, policy_version, marker_blob FROM \
         {relation_sys_stream_trim_due_markers} WHERE due_at <= {} ORDER BY due_at, scope_id, \
         policy_version LIMIT {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_stream_trim_due_markers = engine.relation_name("sys_stream_trim_due_markers"),
    );
    SqlStatement::new(
        sql,
        vec![SqlValue::Integer(due_before), SqlValue::Integer(limit)],
    )
}

pub(crate) fn state(engine: impl SqlLayout, scope_id: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT scope_id, state_blob FROM {relation_sys_stream_trim_state} WHERE scope_id = {}",
            engine.engine().placeholder(1),
            relation_sys_stream_trim_state = engine.relation_name("sys_stream_trim_state"),
        ),
        vec![SqlValue::Text(scope_id.to_owned())],
    )
}

pub(super) fn marker(engine: impl SqlLayout, record: StreamTrimDueRecord) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT marker_blob FROM {relation_sys_stream_trim_due_markers} WHERE due_at = {} AND \
             scope_id = {} AND policy_version = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            relation_sys_stream_trim_due_markers =
                engine.relation_name("sys_stream_trim_due_markers"),
        ),
        vec![
            SqlValue::Integer(record.due_at),
            SqlValue::Text(record.scope_id),
            SqlValue::Integer(record.policy_version),
        ],
    )
}

pub(super) fn delete_marker(
    engine: impl SqlLayout,
    record: &StreamRetentionFinishRecord,
    marker_blob: Vec<u8>,
) -> SqlStatement {
    let mut sql = format!(
        "DELETE FROM {relation_sys_stream_trim_due_markers} WHERE due_at = {} AND scope_id = {} \
         AND policy_version = {} AND marker_blob = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        relation_sys_stream_trim_due_markers = engine.relation_name("sys_stream_trim_due_markers"),
    );
    let mut binds = vec![
        SqlValue::Integer(record.due_at),
        SqlValue::Text(record.scope_id.clone()),
        SqlValue::Integer(record.policy_version),
        SqlValue::Blob(marker_blob),
    ];
    if let Some(expected) = &record.expected_state_blob {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM {relation_sys_stream_trim_state} WHERE scope_id = {} AND \
             state_blob = {})",
            engine.engine().placeholder(5),
            engine.engine().placeholder(6),
            relation_sys_stream_trim_state = engine.relation_name("sys_stream_trim_state"),
        ));
        binds.push(SqlValue::Text(record.scope_id.clone()));
        binds.push(SqlValue::Blob(expected.clone()));
    }
    SqlStatement::new(sql, binds)
}
