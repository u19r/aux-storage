//! SQL text and bindings for the scheduled-job physical store.
use scheduled_bg_jobs::store::{RangeDirection, RangeRequest, StoreKey};
use storage_types::StorageResult;

use crate::{
    SqlEngine, SqlStatement, SqlValue, constants::SCHEDULED_JOB_DATA_TABLE, statement::SqlText,
};

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(super) fn schema(engine: SqlEngine) -> StorageResult<[SqlStatement; 1]> {
    let blob = engine.blob_type();
    let suffix = engine.table_suffix();
    Ok([SqlStatement::new(
        SqlText::render(format_args!(
            "CREATE TABLE IF NOT EXISTS {SCHEDULED_JOB_DATA_TABLE} (key {blob} PRIMARY KEY, value \
             {blob} NOT NULL){suffix}"
        ))?,
        Vec::new(),
    )])
}

pub(crate) fn points(engine: SqlEngine, keys: &[StoreKey]) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "SELECT key, value FROM {SCHEDULED_JOB_DATA_TABLE} WHERE key IN ("
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
    sql.append(format_args!(
        "SELECT key, value FROM {SCHEDULED_JOB_DATA_TABLE}"
    ))?;
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

pub(crate) fn put(engine: SqlEngine, key: &[u8], value: &[u8]) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "INSERT INTO {SCHEDULED_JOB_DATA_TABLE} (key, value) VALUES ("
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
    sql.append(format_args!(
        "DELETE FROM {SCHEDULED_JOB_DATA_TABLE} WHERE key = "
    ))?;
    sql.append_parameter(engine, 1, false)?;
    let mut binds = bind_slots(1)?;
    binds.push(SqlValue::Blob(copy_blob(key)?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

/// Removes one effect receipt group: every key in `[begin, end)`.
pub(super) fn delete_range(
    engine: SqlEngine,
    begin: &[u8],
    end: &[u8],
) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "DELETE FROM {SCHEDULED_JOB_DATA_TABLE} WHERE key >= "
    ))?;
    sql.append_parameter(engine, 1, false)?;
    sql.append(format_args!(" AND key < "))?;
    sql.append_parameter(engine, 2, false)?;
    let mut binds = bind_slots(2)?;
    binds.push(SqlValue::Blob(copy_blob(begin)?));
    binds.push(SqlValue::Blob(copy_blob(end)?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

/// Postgres: lock one job-store row with a no-op write and return it. A
/// SERIALIZABLE transaction that later writes the row fails and retries
/// instead of committing on a snapshot that predates this transaction.
pub(crate) fn lock_point(engine: SqlEngine, key: &StoreKey) -> StorageResult<SqlStatement> {
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "UPDATE {SCHEDULED_JOB_DATA_TABLE} SET value = value WHERE key = "
    ))?;
    sql.append_parameter(engine, 1, false)?;
    sql.append(format_args!(" RETURNING key, value"))?;
    let mut binds = bind_slots(1)?;
    binds.push(SqlValue::Blob(copy_blob(key.as_bytes())?));
    Ok(SqlStatement::new(sql.finish()?, binds))
}

/// The job-effect receipt write of a counted Dynamo commit (Postgres).
pub(crate) fn counted_put(
    engine: SqlEngine,
    key: &[u8],
    value: Vec<u8>,
) -> StorageResult<crate::SqlCountedWrite> {
    let mut sql = SqlText::start_counted_write()?;
    sql.append(format_args!(
        "INSERT INTO {SCHEDULED_JOB_DATA_TABLE} (key, value) VALUES ("
    ))?;
    sql.append_parameter(engine, 1, false)?;
    sql.append(format_args!(", "))?;
    sql.append_parameter(engine, 2, false)?;
    sql.append(format_args!(
        ") ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))?;
    sql.finish_counted_write(
        vec![SqlValue::Blob(copy_blob(key)?), SqlValue::Blob(value)],
        crate::catalog::SqlDynamoCount::ExactlyOne,
        crate::transact_write::receipt_count_error,
    )
}

pub(crate) fn copy_blob(value: &[u8]) -> StorageResult<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    owned.extend_from_slice(value);
    Ok(owned)
}

fn bind_slots(count: usize) -> StorageResult<Vec<SqlValue>> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    Ok(binds)
}
