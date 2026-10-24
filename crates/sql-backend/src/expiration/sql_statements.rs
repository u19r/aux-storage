//! Ordered TTL index page for SQLite, Turso, and Postgres.

use storage_provider::{TtlIndexRead, maintenance::ExpirationCandidateCursor};
use storage_types::{StorageResult, TableHistoryId};

use crate::{SqlLayout, SqlStatement, SqlValue, statement::SqlText};

pub(super) fn expiration_index_page(
    engine: impl SqlLayout,
    read: &TtlIndexRead,
    history_id: TableHistoryId,
) -> StorageResult<SqlStatement> {
    let physical = engine.ttl_table_name(&read.table_name, history_id);
    let cursor = read
        .cursor
        .as_ref()
        .map(ExpirationCandidateCursor::try_from)
        .transpose()?;
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(if cursor.is_some() { 5 } else { 2 })
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    binds.push(SqlValue::Integer(read.cutoff_seconds));
    let mut sql = SqlText::default();
    sql.append(format_args!(
        "SELECT ttl_value, key_token FROM \"{physical}\" WHERE ttl_value <= "
    ))?;
    sql.append_parameter(engine.engine(), 1, false)?;
    if let Some(cursor) = cursor {
        binds.push(SqlValue::Integer(cursor.expiration_seconds));
        binds.push(SqlValue::Integer(cursor.expiration_seconds));
        binds.push(SqlValue::Text(cursor.ttl_item_key));
        sql.append(format_args!(" AND (ttl_value > "))?;
        sql.append_parameter(engine.engine(), 2, false)?;
        sql.append(format_args!(" OR (ttl_value = "))?;
        sql.append_parameter(engine.engine(), 3, false)?;
        sql.append(format_args!(" AND key_token > "))?;
        sql.append_parameter(engine.engine(), 4, false)?;
        sql.append(format_args!("))"))?;
    }
    let limit = read
        .limit
        .get()
        .checked_add(1)
        .and_then(|limit| i64::try_from(limit).ok())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    binds.push(SqlValue::Integer(limit));
    sql.append(format_args!(" ORDER BY ttl_value, key_token LIMIT "))?;
    sql.append_parameter(engine.engine(), binds.len(), false)?;
    Ok(SqlStatement::new(sql.finish()?, binds))
}
