//! Ordered TTL index page for SQLite, Turso, and Postgres.

use storage_provider::{TtlIndexRead, maintenance::ExpirationCandidateCursor};
use storage_types::{StorageResult, TableHistoryId};

use crate::{SqlLayout, SqlStatement, SqlValue, statement::SqlText};

pub(super) fn expiration_index_page(
    engine: impl SqlLayout,
    read: &TtlIndexRead,
    history_id: TableHistoryId,
) -> StorageResult<SqlStatement> {
    let ttl_key = crate::item_schema::ttl_key_identifier();
    let physical = engine.item_table_name(history_id);
    let cursor = read
        .cursor
        .as_ref()
        .map(ExpirationCandidateCursor::try_from)
        .transpose()?;
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(if cursor.is_some() { 3 } else { 2 })
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    let mut sql = SqlText::default();
    // Turso requires the partial-index predicate explicitly.
    sql.append(format_args!(
        "SELECT {ttl_key} FROM \"{physical}\" WHERE {ttl_key} IS NOT NULL"
    ))?;
    if let Some(upper) = read.cutoff_seconds.checked_add(1) {
        binds.push(SqlValue::Blob(
            storage_common::TtlIndexKey::encode_ordered_seconds(upper).to_vec(),
        ));
        sql.append(format_args!(" AND {ttl_key} < "))?;
        sql.append_parameter(engine.engine(), binds.len(), false)?;
    }
    if let Some(cursor) = cursor {
        let key =
            storage_common::TtlIndexKey::new(cursor.expiration_seconds, cursor.ttl_item_key.into());
        binds.push(SqlValue::Blob(key.encode_ordered_bytes()?));
        sql.append(format_args!(" AND {ttl_key} > "))?;
        sql.append_parameter(engine.engine(), binds.len(), false)?;
    }
    let limit = read
        .limit
        .get()
        .checked_add(1)
        .and_then(|limit| i64::try_from(limit).ok())
        .ok_or(storage_types::AttemptFailure::CAPACITY)?;
    binds.push(SqlValue::Integer(limit));
    sql.append(format_args!(" ORDER BY {ttl_key} LIMIT "))?;
    sql.append_parameter(engine.engine(), binds.len(), false)?;
    Ok(SqlStatement::new(sql.finish()?, binds))
}
