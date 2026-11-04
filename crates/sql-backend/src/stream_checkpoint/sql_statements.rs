//! Durable checkpoint receipts for SQLite, Turso, and Postgres.

use storage_types::StorageResult;
use stream_provider::{SystemStreamCheckpointIssue, SystemStreamCheckpointReceipt};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(super) fn schema(engine: impl SqlLayout) -> [SqlStatement; 1] {
    let text = engine.engine().text_type();
    let blob = engine.engine().blob_type();
    let suffix = engine.engine().table_suffix();
    [format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_checkpoints} (token {text} \
         PRIMARY KEY, operation_key {text} NOT NULL UNIQUE, receipt_blob {blob} NOT NULL){suffix}",
        relation_sys_dynamo_stream_checkpoints =
            engine.relation_name("sys_dynamo_stream_checkpoints"),
    )]
    .map(|sql| SqlStatement::new(sql, vec![]))
}

pub(super) fn by_token(engine: impl SqlLayout, token: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {relation_sys_dynamo_stream_checkpoints} WHERE token = {}",
            engine.engine().placeholder(1),
            relation_sys_dynamo_stream_checkpoints =
                engine.relation_name("sys_dynamo_stream_checkpoints"),
        ),
        vec![SqlValue::Text(token.to_owned())],
    )
}

pub(super) fn by_operation(engine: impl SqlLayout, operation_key: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {relation_sys_dynamo_stream_checkpoints} WHERE \
             operation_key = {}",
            engine.engine().placeholder(1),
            relation_sys_dynamo_stream_checkpoints =
                engine.relation_name("sys_dynamo_stream_checkpoints"),
        ),
        vec![SqlValue::Text(operation_key.to_owned())],
    )
}

pub(super) fn insert(
    engine: impl SqlLayout,
    issue: &SystemStreamCheckpointIssue,
    receipt: &SystemStreamCheckpointReceipt,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_checkpoints} (token, operation_key, \
             receipt_blob) VALUES ({}) ON CONFLICT DO NOTHING",
            engine.engine().placeholders(3),
            relation_sys_dynamo_stream_checkpoints =
                engine.relation_name("sys_dynamo_stream_checkpoints"),
        ),
        vec![
            SqlValue::Text(issue.issued_token.as_str().to_owned()),
            SqlValue::Text(issue.operation_key.clone()),
            SqlValue::Blob(storage_types::storage_serde::to_bytes(receipt)?),
        ],
    ))
}
