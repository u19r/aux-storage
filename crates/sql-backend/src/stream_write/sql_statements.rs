//! SQL text and bindings for stream rows and per-stream sequence authority.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use storage_types::{StorageError, StorageResult, StreamName, storage_serde};
use stream_provider::{
    DynamoStreamEffectRow, StreamItem, StreamPointerReference, StreamRowValue,
    retained_image::system_stream::RetainedSystemStreamRecord,
};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn user_sequence_name(stream_name: &StreamName) -> String {
    format!("user:{}", URL_SAFE_NO_PAD.encode(stream_name.as_ref()))
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

pub(super) fn item(
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

pub(super) fn pointer_reference(
    engine: impl SqlLayout,
    reference: StreamPointerReference,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_pointer_owners} (owner_stream, \
             owner_item_id, item_stream, item_item_id, owner_kind) VALUES ({})",
            engine.engine().placeholders(5),
            relation_sys_dynamo_stream_pointer_owners =
                engine.relation_name("sys_dynamo_stream_pointer_owners"),
        ),
        vec![
            SqlValue::Blob(reference.owner_stream.as_ref().to_vec()),
            SqlValue::Blob(reference.owner_item_id.as_bytes().to_vec()),
            SqlValue::Blob(reference.item_stream.as_ref().to_vec()),
            SqlValue::Blob(
                reference
                    .item_item_id
                    .unwrap_or_default()
                    .as_bytes()
                    .to_vec(),
            ),
            SqlValue::Integer(reference.owner as i64),
        ],
    )
}

pub(super) fn record(
    engine: impl SqlLayout,
    record: RetainedSystemStreamRecord,
    receipt: &DynamoStreamEffectRow,
) -> StorageResult<SqlStatement> {
    let position = &receipt.position;
    let order = position.offset().order().map_err(|error| {
        StorageError::internal(&format!("invalid planned stream offset: {error}"))
    })?;
    let binds = vec![
        SqlValue::Text(position.shard_id().as_str().to_owned()),
        SqlValue::Text(position.lineage_id().as_str().to_owned()),
        SqlValue::Blob(order.to_be_bytes().to_vec()),
        SqlValue::Text(position.offset().as_str().to_owned()),
        SqlValue::Text(receipt.effect_id.as_str().to_owned()),
        SqlValue::Blob(receipt.pointer_item_id.as_bytes().to_vec()),
        SqlValue::Blob(record.encode(crate::constants::STREAM_READ_BYTES.get())?),
        SqlValue::Integer(i64::from(
            stream_provider::retained_image::system_stream::RETAINED_SYSTEM_STREAM_RECORD_FORMAT,
        )),
        SqlValue::Integer(receipt.created_at.timestamp_millis()),
    ];
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_records} (shard_id, lineage_id, \
             offset_order, offset_text, effect_id, pointer_item_id, record_blob, \
             read_work_format, created_at) VALUES ({})",
            engine.engine().placeholders(9),
            relation_sys_dynamo_stream_records = engine.relation_name("sys_dynamo_stream_records"),
        ),
        binds,
    ))
}

pub(super) fn effect(
    engine: impl SqlLayout,
    receipt: &DynamoStreamEffectRow,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_effects} (effect_id, receipt_blob, \
             created_at) VALUES ({})",
            engine.engine().placeholders(3),
            relation_sys_dynamo_stream_effects = engine.relation_name("sys_dynamo_stream_effects"),
        ),
        vec![
            SqlValue::Text(receipt.effect_id.as_str().to_owned()),
            SqlValue::Blob(storage_serde::to_bytes(receipt)?),
            SqlValue::Integer(receipt.created_at.timestamp_millis()),
        ],
    ))
}

pub(super) fn schema(engine: impl SqlLayout) -> [SqlStatement; 8] {
    let text = engine.engine().text_type();
    let blob = engine.engine().blob_type();
    let int = engine.engine().integer_type();
    let suffix = engine.engine().table_suffix();
    [
        format!("CREATE TABLE IF NOT EXISTS {} (operation_id {blob} PRIMARY KEY, receipt_blob {blob} NOT NULL){suffix}", engine.relation_name("sys_stream_append_receipts")),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_items} (stream_name {blob} \
             NOT NULL, item_id {blob} NOT NULL, row_blob {blob} NOT NULL, created_at {int} NOT \
             NULL, PRIMARY KEY (stream_name, item_id)){suffix}",
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_pointer_owners} (owner_stream \
             {blob} NOT NULL, owner_item_id {blob} NOT NULL, item_stream {blob} NOT NULL, \
             item_item_id {blob} NOT NULL, owner_kind {int} NOT NULL, PRIMARY KEY (owner_stream, \
             owner_item_id)){suffix}",
            relation_sys_dynamo_stream_pointer_owners =
                engine.relation_name("sys_dynamo_stream_pointer_owners"),
        ),
        crate::sql_template::sql_template!(
            engine,
            "CREATE INDEX IF NOT EXISTS "["idx_dynamo_stream_pointer_owners_item"],
            " ON "["sys_dynamo_stream_pointer_owners"],
            "(item_stream, item_item_id)"
        )
        .into_owned(),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_records} (shard_id {text} NOT \
             NULL, lineage_id {text} NOT NULL, offset_order {blob} NOT NULL, offset_text {text} \
             NOT NULL, effect_id {text} NOT NULL, pointer_item_id {blob} NOT NULL, record_blob \
             {blob} NOT NULL, read_work_format {int} NOT NULL DEFAULT {read_work_format} CHECK (read_work_format = {read_work_format}), created_at {int} NOT NULL, PRIMARY KEY (shard_id, lineage_id, \
             offset_order, offset_text)){suffix}",
            relation_sys_dynamo_stream_records = engine.relation_name("sys_dynamo_stream_records"),
            read_work_format = stream_provider::retained_image::system_stream::RETAINED_SYSTEM_STREAM_RECORD_FORMAT,
        ),
        crate::sql_template::sql_template!(
            engine,
            "CREATE INDEX IF NOT EXISTS "["idx_dynamo_stream_records_effect"],
            " ON "["sys_dynamo_stream_records"],
            "(effect_id)"
        )
        .into_owned(),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_effects} (effect_id {text} \
             PRIMARY KEY, receipt_blob {blob} NOT NULL, created_at {int} NOT NULL){suffix}",
            relation_sys_dynamo_stream_effects = engine.relation_name("sys_dynamo_stream_effects"),
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
