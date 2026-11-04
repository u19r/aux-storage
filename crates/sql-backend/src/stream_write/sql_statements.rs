//! SQL text and bindings for stream rows and per-stream sequence authority.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use storage_types::{StorageError, StorageResult, StreamName, storage_serde};
use stream_provider::{StreamItem, StreamRowValue};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn user_sequence_name(stream_name: &StreamName) -> String {
    format!("user:{}", URL_SAFE_NO_PAD.encode(stream_name.as_ref()))
}

pub(super) fn delete_table_appends(
    engine: impl SqlLayout,
    history: storage_types::TableHistoryId,
) -> Vec<SqlStatement> {
    let stream = history.append_stream_name();
    let scopes = [
        (
            "sys_dynamo_stream_items",
            "stream_name",
            "item_id",
            SqlValue::Blob(stream.as_ref().to_vec()),
        ),
        (
            "sys_user_stream_cursors",
            "stream_name",
            "cursor_name",
            SqlValue::Blob(stream.as_ref().to_vec()),
        ),
        (
            "sys_stream_append_receipts",
            "table_id",
            "operation_id",
            SqlValue::Text(uuid::Uuid::from_bytes(*history.as_ref()).to_string()),
        ),
    ];
    let mut statements = scopes
        .into_iter()
        .map(|(relation, scope, row, value)| {
            delete_scope_page(engine, relation, (scope, row), value)
        })
        .collect::<Vec<_>>();
    statements.push(delete_sequence(engine, &user_sequence_name(&stream)));
    statements
}

fn delete_scope_page(
    engine: impl SqlLayout,
    relation: &str,
    columns: (&str, &str),
    value: SqlValue,
) -> SqlStatement {
    let (scope, row) = columns;
    // The subquery selects the same scope. Binding once also avoids copying
    // native values solely to express the bounded deletion.
    SqlStatement::new(
        format!(
            "DELETE FROM {relation} WHERE ({scope}, {row}) IN (SELECT {scope}, {row} FROM \
             {relation} WHERE {scope} = {parameter} ORDER BY {row} LIMIT 64)",
            relation = engine.relation_name(relation),
            parameter = engine.engine().placeholder(1),
        ),
        vec![value],
    )
}

pub(super) fn read_append_receipt(
    engine: impl SqlLayout,
    key: stream_provider::TableAppendReceiptKey,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {} WHERE table_id = {} AND operation_id = {}",
            engine.relation_name("sys_stream_append_receipts"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
        ),
        vec![
            SqlValue::Text(uuid::Uuid::from_bytes(*key.history_id.as_ref()).to_string()),
            SqlValue::Blob(key.operation.bytes().to_vec()),
        ],
    )
}

pub(super) fn store_append_receipt(
    engine: impl SqlLayout,
    receipt: &stream_provider::StreamAppendReceipt,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {} (table_id, operation_id, receipt_blob) VALUES ({})",
            engine.relation_name("sys_stream_append_receipts"),
            engine.engine().placeholders(3),
        ),
        vec![
            SqlValue::Text(uuid::Uuid::from_bytes(*receipt.history_id.as_ref()).to_string()),
            SqlValue::Blob(receipt.operation.bytes().to_vec()),
            SqlValue::Blob(storage_serde::to_bytes(receipt)?),
        ],
    ))
}

pub(crate) fn initialize_sequence(engine: impl SqlLayout, name: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_sequence} (name, next_id) VALUES ({}, 1) ON \
             CONFLICT(name) DO NOTHING",
            engine.engine().placeholder(1),
            relation_sys_dynamo_stream_sequence =
                engine.relation_name("sys_dynamo_stream_sequence"),
        ),
        vec![SqlValue::Text(name.to_owned())],
    )
}

pub(crate) fn delete_sequence(engine: impl SqlLayout, name: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {relation_sys_dynamo_stream_sequence} WHERE name = {}",
            engine.engine().placeholder(1),
            relation_sys_dynamo_stream_sequence =
                engine.relation_name("sys_dynamo_stream_sequence"),
        ),
        vec![SqlValue::Text(name.to_owned())],
    )
}

pub(super) fn allocate_sequence(engine: impl SqlLayout, name: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "UPDATE {relation_sys_dynamo_stream_sequence} SET next_id = next_id + 1 WHERE name = \
             {} AND next_id < {} RETURNING next_id - 1",
            engine.engine().placeholder(1),
            i64::MAX,
            relation_sys_dynamo_stream_sequence =
                engine.relation_name("sys_dynamo_stream_sequence"),
        ),
        vec![SqlValue::Text(name.to_owned())],
    )
}

pub(super) fn postgres_append_user_item(
    engine: impl SqlLayout,
    row: &StreamRowValue,
) -> StorageResult<SqlStatement> {
    let stream = row
        .stream_name()
        .ok_or_else(|| StorageError::internal("Postgres user stream row has no stream name"))?;
    Ok(SqlStatement::new(
        crate::sql_template::sql_template!(
            engine,
            "INSERT INTO "["sys_dynamo_stream_items"],
            " (stream_name, item_id, row_blob, created_at) VALUES ($1, decode('58494431', 'hex') \
             || int8send(pg_current_xact_id()::text::bigint), $2, $3) RETURNING item_id"
        )
        .to_owned(),
        vec![
            SqlValue::Blob(stream.as_ref().to_vec()),
            SqlValue::Blob(row.to_bytes()?),
            SqlValue::Integer(row.created_at().timestamp_millis()),
        ],
    ))
}

pub(crate) fn item(
    engine: impl SqlLayout,
    row: stream_provider::StreamWriteKind<StreamItem>,
) -> StorageResult<SqlStatement> {
    let name =
        row.value().stream_name.as_ref().ok_or_else(|| {
            StorageError::internal("planned Dynamo stream row has no stream name")
        })?;
    let name = name.as_ref().to_vec();
    let id = row.value().id.as_bytes().to_vec();
    let created_at = row.value().created_at.timestamp_millis();
    let payload = StreamRowValue::from_planned(row)?.to_bytes()?;
    let binds = vec![
        SqlValue::Blob(name),
        SqlValue::Blob(id),
        SqlValue::Blob(payload),
        SqlValue::Integer(created_at),
    ];
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_items} (stream_name, item_id, row_blob, \
             created_at) VALUES ({})",
            engine.engine().placeholders(4),
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        binds,
    ))
}

pub(crate) fn delete_history_row(
    engine: impl SqlLayout,
    stream: storage_types::StreamName,
    version: storage_types::StreamItemId,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {} WHERE stream_name = {} AND item_id = {}",
            engine.relation_name("sys_dynamo_stream_items"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2)
        ),
        vec![
            SqlValue::Blob(stream.as_ref().to_vec()),
            SqlValue::Blob(version.as_bytes().to_vec()),
        ],
    )
}

pub(super) fn record(
    engine: impl SqlLayout,
    record: storage_types::SystemHistoryRecord,
) -> StorageResult<SqlStatement> {
    let position = record.position();
    let order = position
        .offset()
        .order()
        .map_err(|_| StorageError::internal("Invalid system history position"))?;
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {} (shard_id, lineage_id, offset_order, offset_text, table_id, \
             record_blob, created_at) VALUES ({})",
            engine.relation_name("sys_dynamo_stream_records"),
            engine.engine().placeholders(7)
        ),
        vec![
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(order.to_be_bytes().to_vec()),
            SqlValue::Text(position.offset().as_str().to_owned()),
            record
                .table_history_id()
                .map_or(SqlValue::Null, |id| SqlValue::Blob(id.as_ref().to_vec())),
            SqlValue::Blob(storage_serde::to_bytes(&record)?),
            SqlValue::Integer(record.created_at().timestamp_millis()),
        ],
    ))
}

pub(super) fn schema(engine: impl SqlLayout) -> [SqlStatement; 5] {
    let text = engine.engine().text_type();
    let blob = engine.engine().blob_type();
    let int = engine.engine().integer_type();
    let suffix = engine.engine().table_suffix();
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {} (table_id {text} NOT NULL, operation_id {blob} NOT \
             NULL, receipt_blob {blob} NOT NULL, PRIMARY KEY (table_id, operation_id)){suffix}",
            engine.relation_name("sys_stream_append_receipts")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_items} (stream_name {blob} \
             NOT NULL, item_id {blob} NOT NULL, row_blob {blob} NOT NULL, created_at {int} NOT \
             NULL, PRIMARY KEY (stream_name, item_id)){suffix}",
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (shard_id {text} NOT NULL, lineage_id {text} NOT NULL, \
             offset_order {blob} NOT NULL, offset_text {text} NOT NULL, table_id {blob}, \
             record_blob {blob} NOT NULL, created_at {int} NOT NULL, PRIMARY KEY (shard_id, \
             lineage_id, offset_order, offset_text)){suffix}",
            engine.relation_name("sys_dynamo_stream_records")
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (table_id, shard_id, lineage_id, offset_order, \
             offset_text)",
            engine.relation_name("idx_dynamo_stream_records_table"),
            engine.relation_name("sys_dynamo_stream_records")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_sequence} (name {text} \
             PRIMARY KEY, next_id {int} NOT NULL CHECK (next_id > 0)){suffix}",
            relation_sys_dynamo_stream_sequence =
                engine.relation_name("sys_dynamo_stream_sequence"),
        ),
    ]
    .map(|sql| SqlStatement::new(sql, vec![]))
}
