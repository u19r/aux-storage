//! SQL text and bindings for the scheduled-job physical store.
use scheduled_bg_jobs::store::{CommitOperationId, RangeDirection, RangeRequest, StoreKey};
use storage_provider::job_store::EstablishedCommitOutcome;
use storage_types::{InfrastructureResultKind, StorageResult};

use crate::{SqlEngine, SqlStatement, SqlValue, statement::SqlText};

const DATA_TABLE: &str = "sys_scheduled_jobs_v2";
const RECEIPT_TABLE: &str = "sys_scheduled_job_receipts_v2";

pub(super) fn schema(engine: SqlEngine) -> StorageResult<[SqlStatement; 2]> {
    let blob = engine.blob_type();
    let suffix = engine.table_suffix();
    Ok([
        SqlStatement::new(
            SqlText::render(format_args!(
                "CREATE TABLE IF NOT EXISTS {DATA_TABLE} (key {blob} PRIMARY KEY, value {blob} \
                 NOT NULL){suffix}"
            ))?,
            Vec::new(),
        ),
        SqlStatement::new(
            SqlText::render(format_args!(
                "CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE} (operation_id {blob} PRIMARY KEY, \
                 outcome BIGINT NOT NULL, content {blob} NOT NULL){suffix}"
            ))?,
            Vec::new(),
        ),
    ])
}

pub(crate) fn points(engine: SqlEngine, keys: &[StoreKey]) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "SELECT key, value FROM {DATA_TABLE} WHERE key IN ("
    ))?;
    let mut binds = bind_slots(keys.len())?;
    for (ordinal, key) in keys.iter().enumerate() {
        if ordinal != 0 {
            sql.append(format_args!(", "))?;
        }
        sql.append_parameter(engine, ordinal + 1, false)?;
        binds.push(SqlValue::Blob(copy_blob(key.as_bytes())?));
    }
    sql.append(format_args!(")"))?;
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(super) fn range(engine: SqlEngine, request: &RangeRequest) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!("SELECT key, value FROM {DATA_TABLE}"))?;
    let mut binds = bind_slots(4)?;
    for (key, operator) in [
        (request.start(), ">="),
        (request.end(), "<"),
        (
            request.continuation(),
            if request.direction() == RangeDirection::Forward {
                ">"
            } else {
                "<"
            },
        ),
    ] {
        let Some(key) = key else {
            continue;
        };
        sql.append(format_args!(
            "{} key {operator} ",
            if binds.is_empty() { " WHERE" } else { " AND" }
        ))?;
        binds.push(SqlValue::Blob(copy_blob(key.as_bytes())?));
        sql.append_parameter(engine, binds.len(), false)?;
    }
    let order = if request.direction() == RangeDirection::Forward {
        "ASC"
    } else {
        "DESC"
    };
    sql.append(format_args!(" ORDER BY key {order} LIMIT "))?;
    binds.push(SqlValue::Integer(i64::from(request.item_limit().get()) + 1));
    sql.append_parameter(engine, binds.len(), false)?;
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(super) fn put(engine: SqlEngine, key: &[u8], value: &[u8]) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "INSERT INTO {DATA_TABLE} (key, value) VALUES ("
    ))?;
    sql.append_parameter(engine, 1, false)?;
    sql.append(format_args!(", "))?;
    sql.append_parameter(engine, 2, false)?;
    sql.append(format_args!(
        ") ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))?;
    let mut binds = bind_slots(2)?;
    binds.push(SqlValue::Blob(copy_blob(key)?));
    binds.push(SqlValue::Blob(copy_blob(value)?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(super) fn delete(engine: SqlEngine, key: &[u8]) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!("DELETE FROM {DATA_TABLE} WHERE key = "))?;
    sql.append_parameter(engine, 1, false)?;
    let mut binds = bind_slots(1)?;
    binds.push(SqlValue::Blob(copy_blob(key)?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(super) fn read_receipt(
    engine: SqlEngine,
    operation_id: CommitOperationId,
) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "SELECT outcome, content FROM {RECEIPT_TABLE} WHERE operation_id = "
    ))?;
    sql.append_parameter(engine, 1, false)?;
    let mut binds = bind_slots(1)?;
    binds.push(SqlValue::Blob(copy_blob(operation_id.as_bytes())?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(super) fn save_receipt(
    engine: SqlEngine,
    operation_id: CommitOperationId,
    content: &[u8],
    outcome: EstablishedCommitOutcome,
) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "INSERT INTO {RECEIPT_TABLE} (operation_id, outcome, content) VALUES ("
    ))?;
    for position in 1..=3 {
        if position != 1 {
            sql.append(format_args!(", "))?;
        }
        sql.append_parameter(engine, position, false)?;
    }
    sql.append(format_args!(")"))?;
    let outcome = match outcome {
        EstablishedCommitOutcome::Committed => 0,
        EstablishedCommitOutcome::Conflict => 1,
    };
    let mut binds = bind_slots(3)?;
    binds.push(SqlValue::Blob(copy_blob(operation_id.as_bytes())?));
    binds.push(SqlValue::Integer(outcome));
    binds.push(SqlValue::Blob(copy_blob(content)?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(crate) fn copy_blob(value: &[u8]) -> StorageResult<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    owned.extend_from_slice(value);
    Ok(owned)
}

fn bind_slots(count: usize) -> StorageResult<Vec<SqlValue>> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(count)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    Ok(binds)
}
