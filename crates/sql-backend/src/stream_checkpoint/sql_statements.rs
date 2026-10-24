//! One checkpoint and frontier schema for SQLite, Turso, and Postgres.

use storage_types::{StorageError, StorageResult, SystemStreamPosition};
use stream_provider::{SystemStreamCheckpointIssue, SystemStreamCheckpointReceipt};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(super) fn schema(engine: impl SqlLayout) -> [SqlStatement; 2] {
    let text = engine.engine().text_type();
    let blob = engine.engine().blob_type();
    let integer = engine.engine().integer_type();
    let suffix = engine.engine().table_suffix();
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_checkpoints} (token {text} \
             PRIMARY KEY, operation_key {text} NOT NULL UNIQUE, receipt_blob {blob} NOT \
             NULL){suffix}",
            relation_sys_dynamo_stream_checkpoints =
                engine.relation_name("sys_dynamo_stream_checkpoints"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_dynamo_stream_protected_frontier} (scope_id \
             {text} NOT NULL, generation {integer} NOT NULL, shard_id {text} NOT NULL, lineage_id \
             {text} NOT NULL, offset_order {blob} NOT NULL, offset_text {text} NOT NULL, PRIMARY \
             KEY (scope_id, generation, shard_id, lineage_id)){suffix}",
            relation_sys_dynamo_stream_protected_frontier =
                engine.relation_name("sys_dynamo_stream_protected_frontier"),
        ),
    ]
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

pub(crate) fn protected_frontiers(
    engine: impl SqlLayout,
    scope: &storage_types::SystemStreamScope,
) -> StorageResult<SqlStatement> {
    let generation = i64::try_from(scope.generation())
        .map_err(|_| StorageError::internal("SystemStream generation exceeds SQL range"))?;
    Ok(SqlStatement::new(
        format!(
            "SELECT shard_id, lineage_id, offset_order, offset_text FROM \
             {relation_sys_dynamo_stream_protected_frontier} WHERE scope_id = {} AND generation = \
             {} ORDER BY shard_id, lineage_id LIMIT {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES + 1,
            relation_sys_dynamo_stream_protected_frontier =
                engine.relation_name("sys_dynamo_stream_protected_frontier"),
        ),
        vec![
            SqlValue::Text(scope.scope_id().as_str().to_owned()),
            SqlValue::Integer(generation),
        ],
    ))
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

pub(crate) fn protect_frontier(
    engine: impl SqlLayout,
    scope: &storage_types::SystemStreamScope,
    position: &SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    let generation = i64::try_from(scope.generation())
        .map_err(|_| StorageError::internal("SystemStream generation exceeds SQL range"))?;
    let order = position
        .offset()
        .order()
        .map_err(|_| StorageError::internal("SystemStream protected offset is invalid"))?;
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_dynamo_stream_protected_frontier} (scope_id, generation, \
             shard_id, lineage_id, offset_order, offset_text) VALUES ({}) ON CONFLICT (scope_id, \
             generation, shard_id, lineage_id) DO UPDATE SET offset_order = \
             excluded.offset_order, offset_text = excluded.offset_text WHERE \
             excluded.offset_order < {relation_sys_dynamo_stream_protected_frontier}.offset_order",
            engine.engine().placeholders(6),
            relation_sys_dynamo_stream_protected_frontier =
                engine.relation_name("sys_dynamo_stream_protected_frontier"),
        ),
        vec![
            SqlValue::Text(scope.scope_id().as_str().to_owned()),
            SqlValue::Integer(generation),
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(order.to_be_bytes().to_vec()),
            SqlValue::Text(position.offset().as_str().to_owned()),
        ],
    ))
}
