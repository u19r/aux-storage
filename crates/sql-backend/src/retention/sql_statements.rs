//! Physical retention schema and statements for all SQL drivers.

use storage_provider::{StreamRetentionFinishRecord, StreamTrimDueRecord, StreamTrimStateRecord};
use storage_types::{StreamItemId, StreamName};
use stream_provider::PointerOwner;

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
            "CREATE TABLE IF NOT EXISTS {relation_sys_stream_trim_due_markers} (due_bucket {int} \
             NOT NULL, scope_id TEXT NOT NULL, policy_version {int} NOT NULL, marker_blob {blob} \
             NOT NULL, PRIMARY KEY (due_bucket, scope_id, policy_version))",
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
        "INSERT INTO {relation_sys_stream_trim_due_markers} (due_bucket, scope_id, \
         policy_version, marker_blob) VALUES ({}) ON CONFLICT(due_bucket, scope_id, \
         policy_version) DO UPDATE SET marker_blob = excluded.marker_blob",
        engine.engine().placeholders(4),
        relation_sys_stream_trim_due_markers = engine.relation_name("sys_stream_trim_due_markers"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Integer(record.due_bucket),
            SqlValue::Text(record.scope_id),
            SqlValue::Integer(record.policy_version),
            SqlValue::Blob(record.marker_blob),
        ],
    )
}

pub(super) fn due_markers(engine: impl SqlLayout, due_before: i64, limit: i64) -> SqlStatement {
    let sql = format!(
        "SELECT due_bucket, scope_id, policy_version, marker_blob FROM \
         {relation_sys_stream_trim_due_markers} WHERE due_bucket <= {} ORDER BY due_bucket, \
         scope_id, policy_version LIMIT {}",
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
            "SELECT marker_blob FROM {relation_sys_stream_trim_due_markers} WHERE due_bucket = {} \
             AND scope_id = {} AND policy_version = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            relation_sys_stream_trim_due_markers =
                engine.relation_name("sys_stream_trim_due_markers"),
        ),
        vec![
            SqlValue::Integer(record.due_bucket),
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
        "DELETE FROM {relation_sys_stream_trim_due_markers} WHERE due_bucket = {} AND scope_id = \
         {} AND policy_version = {} AND marker_blob = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        relation_sys_stream_trim_due_markers = engine.relation_name("sys_stream_trim_due_markers"),
    );
    let mut binds = vec![
        SqlValue::Integer(record.due_bucket),
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

pub(super) fn first_pointer_reference(
    engine: impl SqlLayout,
    item_stream: &StreamName,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT item_item_id FROM {relation_sys_dynamo_stream_pointer_owners} WHERE \
             item_stream = {} AND item_item_id > {} ORDER BY item_item_id LIMIT 1",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_dynamo_stream_pointer_owners =
                engine.relation_name("sys_dynamo_stream_pointer_owners"),
        ),
        vec![
            SqlValue::Blob(item_stream.as_ref().to_vec()),
            SqlValue::Blob(StreamItemId::default().as_bytes().to_vec()),
        ],
    )
}

pub(super) fn latest_stream_item(engine: impl SqlLayout, stream_name: &StreamName) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT item_id FROM {relation_sys_dynamo_stream_items} WHERE stream_name = {} ORDER \
             BY item_id DESC LIMIT 1",
            engine.engine().placeholder(1),
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        vec![SqlValue::Blob(stream_name.as_ref().to_vec())],
    )
}

pub(super) fn trim_candidates(
    engine: impl SqlLayout,
    stream_name: &StreamName,
    cutoff_timestamp: i64,
    max_deleted_item_id: Option<StreamItemId>,
    limit: i64,
) -> SqlStatement {
    let mut binds = vec![
        SqlValue::Blob(stream_name.as_ref().to_vec()),
        SqlValue::Integer(cutoff_timestamp),
    ];
    let mut sql = format!(
        "SELECT item_id, created_at, row_blob FROM {relation_sys_dynamo_stream_items} WHERE \
         stream_name = {} AND created_at <= {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
    );
    if let Some(maximum) = max_deleted_item_id {
        sql.push_str(&format!(
            " AND item_id <= {}",
            engine.engine().placeholder(3)
        ));
        binds.push(SqlValue::Blob(maximum.as_bytes().to_vec()));
    }
    sql.push_str(&format!(
        " ORDER BY item_id LIMIT {}",
        engine.engine().placeholder(binds.len() + 1),
    ));
    binds.push(SqlValue::Integer(limit));
    SqlStatement::new(sql, binds)
}

pub(super) fn first_remaining(engine: impl SqlLayout, stream_name: &StreamName) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT item_id, created_at FROM {relation_sys_dynamo_stream_items} WHERE stream_name \
             = {} ORDER BY item_id LIMIT 1",
            engine.engine().placeholder(1),
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        vec![SqlValue::Blob(stream_name.as_ref().to_vec())],
    )
}

pub(super) fn delete_stream_rows(
    engine: impl SqlLayout,
    stream_name: &StreamName,
    ids: &[StreamItemId],
) -> SqlStatement {
    delete_stream_ids(
        engine,
        "sys_dynamo_stream_items",
        "stream_name",
        "item_id",
        stream_name,
        ids,
    )
}

pub(super) fn release_pointer_owners(
    engine: impl SqlLayout,
    owner: PointerOwner,
    stream_name: &StreamName,
    ids: &[StreamItemId],
) -> SqlStatement {
    let (mut sql, mut binds) = delete_stream_ids(
        engine,
        "sys_dynamo_stream_pointer_owners",
        "owner_stream",
        "owner_item_id",
        stream_name,
        ids,
    )
    .into_parts();
    sql.to_mut().push_str(&format!(
        " AND owner_kind = {}",
        engine.engine().placeholder(ids.len() + 2)
    ));
    binds.push(SqlValue::Integer(owner as i64));
    SqlStatement::new(sql, binds)
}

fn delete_stream_ids(
    engine: impl SqlLayout,
    table: &str,
    stream_column: &str,
    id_column: &str,
    stream_name: &StreamName,
    ids: &[StreamItemId],
) -> SqlStatement {
    let placeholders = (2..ids.len() + 2)
        .map(|index| engine.engine().placeholder(index))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "DELETE FROM {table} WHERE {stream_column} = {} AND {id_column} IN ({placeholders})",
        engine.engine().placeholder(1),
        table = engine.relation_name(table),
    );
    let binds = std::iter::once(SqlValue::Blob(stream_name.as_ref().to_vec()))
        .chain(ids.iter().map(|id| SqlValue::Blob(id.as_bytes().to_vec())))
        .collect();
    SqlStatement::new(sql, binds)
}

pub(super) fn referenced_history(
    engine: impl SqlLayout,
    stream: &StreamName,
    reference: StreamItemId,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT item_id FROM {relation_sys_dynamo_stream_items} WHERE stream_name = {} AND \
             item_id <= {} ORDER BY item_id DESC LIMIT 2",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        vec![
            SqlValue::Blob(stream.as_ref().to_vec()),
            SqlValue::Blob(reference.as_bytes().to_vec()),
        ],
    )
}
