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
             || int8send(pg_current_xact_id()::text::bigint + (SELECT transaction_offset FROM \
             sys_system_stream_scope WHERE scope_key = 'system')), $2, $3) RETURNING item_id"
        )
        .to_owned(),
        vec![
            SqlValue::Blob(stream.as_ref().to_vec()),
            SqlValue::Blob(row.to_bytes()?),
            SqlValue::Integer(row.created_at().timestamp_millis()),
        ],
    ))
}

pub(crate) fn item(engine: impl SqlLayout, row: StreamItem) -> StorageResult<SqlStatement> {
    let name = row
        .stream_name
        .as_ref()
        .ok_or_else(|| StorageError::internal("user stream row has no stream name"))?;
    let name = name.as_ref().to_vec();
    let id = row.id.as_bytes().to_vec();
    let created_at = row.created_at.timestamp_millis();
    let payload = StreamRowValue::from(row).to_bytes()?;
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

pub(crate) fn native_entry(
    engine: impl SqlLayout,
    table: &storage_types::BoundTableInfo,
    lineage: &stream_provider::SystemStreamLineage,
    (entry, scope): (
        &stream_provider::StreamEntryInput<'_>,
        &storage_types::SystemStreamScope,
    ),
    (event_number, transaction_offset): (u16, i64),
) -> StorageResult<SqlStatement> {
    let (sql, binds) = native_entry_parts(
        engine,
        table,
        lineage,
        (entry, scope),
        (event_number, transaction_offset),
    )?;
    Ok(SqlStatement::new(sql, binds))
}

pub(crate) fn counted_native_entry(
    engine: impl SqlLayout,
    table: &storage_types::BoundTableInfo,
    lineage: &stream_provider::SystemStreamLineage,
    (entry, scope): (
        &stream_provider::StreamEntryInput<'_>,
        &storage_types::SystemStreamScope,
    ),
    (event_number, transaction_offset): (u16, i64),
) -> StorageResult<crate::SqlCountedWrite> {
    let (body, binds) = native_entry_parts(
        engine,
        table,
        lineage,
        (entry, scope),
        (event_number, transaction_offset),
    )?;
    let mut sql = crate::statement::SqlText::start_counted_write()?;
    sql.append(format_args!("{body}"))?;
    sql.finish_counted_write(
        binds,
        crate::catalog::SqlDynamoCount::ExactlyOne,
        crate::catalog::SqlDynamoWriteKind::Stream.error_factory(),
    )
}

fn native_entry_parts(
    engine: impl SqlLayout,
    table: &storage_types::BoundTableInfo,
    lineage: &stream_provider::SystemStreamLineage,
    (entry, scope): (
        &stream_provider::StreamEntryInput<'_>,
        &storage_types::SystemStreamScope,
    ),
    (event_number, transaction_offset): (u16, i64),
) -> StorageResult<(String, Vec<SqlValue>)> {
    Ok((
        format!(
            "WITH stream_position AS MATERIALIZED (SELECT ((pg_current_xact_id()::text::bigint + \
             $7::bigint) * 65536 + $4::bigint) AS stream_offset) INSERT INTO {} (table_id, \
             shard_id, lineage_id, offset_order, offset_text, entry_blob, created_at) SELECT $1, \
             $2, $3, int8send(stream_offset), lpad(stream_offset::text, 20, '0') || '-ns', $5, $6 \
             FROM stream_position",
            engine.relation_name("sys_stream_entries"),
        ),
        vec![
            SqlValue::Blob(table.history_id.as_ref().to_vec()),
            SqlValue::Text(lineage.shard_id().as_str().to_owned()),
            SqlValue::Text(lineage.lineage_id().as_str().to_owned()),
            SqlValue::Integer(i64::from(event_number)),
            SqlValue::Blob(stream_provider::PositionedStreamEntry::encode_input(
                &table.table_name,
                scope,
                entry,
            )?),
            SqlValue::Integer(entry.created_at().timestamp_millis()),
            SqlValue::Integer(transaction_offset),
        ],
    ))
}

pub(crate) fn entry(
    engine: impl SqlLayout,
    table: &storage_types::BoundTableInfo,
    position: &storage_types::SystemStreamPosition,
    (entry, scope): (
        &stream_provider::StreamEntryInput<'_>,
        &storage_types::SystemStreamScope,
    ),
) -> StorageResult<SqlStatement> {
    let (sql, binds) = entry_parts(engine, table, position, (entry, scope))?;
    Ok(SqlStatement::new(sql, binds))
}

pub(crate) fn counted_entry(
    engine: impl SqlLayout,
    table: &storage_types::BoundTableInfo,
    position: &storage_types::SystemStreamPosition,
    (entry, scope): (
        &stream_provider::StreamEntryInput<'_>,
        &storage_types::SystemStreamScope,
    ),
) -> StorageResult<crate::SqlCountedWrite> {
    let (body, binds) = entry_parts(engine, table, position, (entry, scope))?;
    let mut sql = crate::statement::SqlText::start_counted_write()?;
    sql.append(format_args!("{body}"))?;
    sql.finish_counted_write(
        binds,
        crate::catalog::SqlDynamoCount::ExactlyOne,
        crate::catalog::SqlDynamoWriteKind::Stream.error_factory(),
    )
}

fn entry_parts(
    engine: impl SqlLayout,
    table: &storage_types::BoundTableInfo,
    position: &storage_types::SystemStreamPosition,
    (entry, scope): (
        &stream_provider::StreamEntryInput<'_>,
        &storage_types::SystemStreamScope,
    ),
) -> StorageResult<(String, Vec<SqlValue>)> {
    let order = position
        .offset()
        .order()
        .map_err(|_| StorageError::internal("Invalid stream entry position"))?;
    Ok((
        format!(
            "INSERT INTO {} (table_id, shard_id, lineage_id, offset_order, offset_text, \
             entry_blob, created_at) VALUES ({})",
            engine.relation_name("sys_stream_entries"),
            engine.engine().placeholders(7)
        ),
        vec![
            SqlValue::Blob(table.history_id.as_ref().to_vec()),
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(order.to_be_bytes().to_vec()),
            SqlValue::Text(position.offset().as_str().to_owned()),
            SqlValue::Blob(stream_provider::PositionedStreamEntry::encode_input(
                &table.table_name,
                scope,
                entry,
            )?),
            SqlValue::Integer(entry.created_at().timestamp_millis()),
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
            "CREATE TABLE IF NOT EXISTS {} (table_id {blob} NOT NULL, shard_id {text} NOT NULL, \
             lineage_id {text} NOT NULL, offset_order {blob} NOT NULL, offset_text {text} NOT \
             NULL, entry_blob {blob} NOT NULL, created_at {int} NOT NULL, PRIMARY KEY (table_id, \
             shard_id, lineage_id, offset_order, offset_text)){suffix}",
            engine.relation_name("sys_stream_entries")
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (shard_id, lineage_id, offset_order, \
             offset_text)",
            engine.relation_name("idx_stream_entries_feed"),
            engine.relation_name("sys_stream_entries")
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

/// Exact canonical primary key; the native feed index is removed with the row.
pub(crate) fn delete_entry(
    engine: impl SqlLayout,
    history_id: storage_types::TableHistoryId,
    position: &storage_types::SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    let order = position
        .offset()
        .order()
        .map_err(|_| StorageError::internal("Invalid stream entry position"))?;
    Ok(SqlStatement::new(
        format!(
            "DELETE FROM {} WHERE table_id = {} AND shard_id = {} AND lineage_id = {} AND \
             offset_order = {} AND offset_text = {}",
            engine.relation_name("sys_stream_entries"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5)
        ),
        vec![
            SqlValue::Blob(history_id.as_ref().to_vec()),
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(order.to_be_bytes().to_vec()),
            SqlValue::Text(position.offset().as_str().to_owned()),
        ],
    ))
}
