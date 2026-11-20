//! Checked per-attempt event numbers and durable sparse stream offsets.

use storage_types::{StorageError, StorageResult};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlStatement, SqlValue,
    constants::ORDINALS_PER_TRANSACTION,
};

#[derive(Default)]
pub(crate) struct StreamEventNumbers {
    next_event_number: u32,
}

impl StreamEventNumbers {
    pub(crate) fn reserve(&mut self) -> StorageResult<u16> {
        let event_number =
            u16::try_from(self.next_event_number).map_err(|_| invalid_allocation())?;
        self.next_event_number += 1;
        Ok(event_number)
    }
}

pub(crate) fn mapped_offset(version: u64) -> StorageResult<u64> {
    version
        .checked_mul(ORDINALS_PER_TRANSACTION)
        .filter(|offset| *offset > 0 && *offset <= i64::MAX as u64)
        .ok_or_else(invalid_allocation)
}

pub(crate) fn integer(row: &crate::SqlRow, column: usize) -> StorageResult<u64> {
    match row.value(ColumnIndex::new(column))? {
        SqlValue::Integer(value) => u64::try_from(*value).map_err(|_| invalid_allocation()),
        _ => Err(invalid_allocation()),
    }
}

fn invalid_allocation() -> StorageError {
    StorageError::internal("Postgres native stream allocation is invalid or exhausted")
}

pub(crate) async fn store_retired_offsets<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl crate::SqlLayout,
    frontiers: &[storage_types::SystemStreamPosition],
) -> StorageResult<()> {
    let statements = frontiers
        .iter()
        .map(|position| {
            let offset = position
                .offset()
                .order()
                .map_err(|_| invalid_allocation())?;
            let offset = i64::try_from(offset).map_err(|_| invalid_allocation())?;
            Ok(SqlStatement::new(
                crate::sql_template::sql_template!(
                    engine,
                    "UPDATE "["sys_system_stream_lineage"],
                    " SET retired_offset = CASE WHEN retired_offset < $1 THEN $1 ELSE \
                     retired_offset END WHERE scope_key = 'system' AND shard_id = $2 AND \
                     lineage_id = $3"
                ),
                vec![
                    SqlValue::Integer(offset),
                    SqlValue::Text(position.shard_id().as_str().to_owned()),
                    SqlValue::Text(position.lineage_id().as_str().to_owned()),
                ],
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    if attempt
        .execute_batch(statements)
        .await?
        .into_iter()
        .any(|count| count != 1)
    {
        return Err(invalid_allocation());
    }
    Ok(())
}

pub(crate) async fn load_retired_offsets<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl crate::SqlLayout,
    origins: &[storage_types::SystemStreamPosition],
) -> StorageResult<storage_types::Exact<Option<storage_types::SystemStreamPosition>>> {
    let statements = origins
        .iter()
        .map(|origin| {
            SqlStatement::new(
                crate::sql_template::sql_template!(
                    engine,
                    "SELECT retired_offset FROM "["sys_system_stream_lineage"],
                    " WHERE scope_key = 'system' AND shard_id = $1 AND lineage_id = $2"
                ),
                vec![
                    SqlValue::Text(origin.shard_id().as_str().to_owned()),
                    SqlValue::Text(origin.lineage_id().as_str().to_owned()),
                ],
            )
        })
        .collect();
    let pages = attempt
        .query_batch_exact(
            statements,
            ReadProtection::Snapshot,
            storage_provider::SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    pages.try_zip_map(origins, |origin, page| {
        if page.more || page.rows.len() != 1 {
            return Err(invalid_allocation());
        }
        let offset = integer(&page.rows[0], 0)?;
        if offset == 0 {
            return Ok(None);
        }
        storage_types::SystemStreamPosition::try_new(
            origin.shard_id().as_str(),
            origin.lineage_id().as_str(),
            if engine.engine() == SqlEngine::Postgres {
                format!("{offset:020}-ns")
            } else {
                format!("{offset:020}")
            },
        )
        .map(Some)
        .map_err(|_| invalid_allocation())
    })
}

/// The stream offset, the snapshot's next transaction ID minus one, and the
/// system and user highs.
/// No transaction ID is assigned. Each high is read by index range: the tail
/// of each lineage, and the last `XID1` ID of each user stream (a skip scan
/// over the stream names of the primary key). The user range holds
/// `XID1 || int8send(id)`, so the last 12-byte ID is the numeric maximum.
pub(crate) const POSTGRES_STREAM_POSITIONS: &str =
    "WITH RECURSIVE streams(name) AS ( (SELECT stream_name FROM sys_dynamo_stream_items ORDER BY \
     stream_name LIMIT 1) UNION ALL SELECT (SELECT i.stream_name FROM sys_dynamo_stream_items AS \
     i WHERE i.stream_name > streams.name ORDER BY i.stream_name LIMIT 1) FROM streams WHERE \
     streams.name IS NOT NULL) SELECT s.transaction_offset, \
     pg_snapshot_xmax(pg_current_snapshot())::text::bigint - 1, COALESCE((SELECT \
     MAX(GREATEST(l.retired_offset, COALESCE(l.sealed_after_offset, 0), COALESCE(tail.position, \
     0))) FROM sys_system_stream_lineage AS l LEFT JOIN LATERAL (SELECT split_part(r.offset_text, \
     '-', 1)::bigint AS position FROM sys_stream_entries AS r WHERE r.shard_id = l.shard_id AND \
     r.lineage_id = l.lineage_id ORDER BY r.offset_order DESC, r.offset_text DESC LIMIT 1) AS \
     tail ON TRUE WHERE l.scope_key = 'system'), 0), COALESCE((SELECT MAX(tail.id) FROM streams \
     CROSS JOIN LATERAL (SELECT ('x' || encode(substring(i.item_id FROM 5 FOR 8), \
     'hex'))::bit(64)::bigint AS id FROM sys_dynamo_stream_items AS i WHERE i.stream_name = \
     streams.name AND i.item_id >= decode('58494431', 'hex') AND i.item_id < decode('58494432', \
     'hex') AND octet_length(i.item_id) = 12 ORDER BY i.item_id DESC LIMIT 1) AS tail), 0) FROM \
     sys_system_stream_scope AS s WHERE s.scope_key = 'system'";

/// Keep restored history before every subsequently assigned transaction. This
/// runs after schema setup, before publication, and never rewrites history.
pub(crate) async fn initialize_postgres_positions<A: SqlAttempt>(
    attempt: &mut A,
) -> StorageResult<()> {
    let (old_offset, offset) = observe_postgres_positions(attempt).await?;
    if offset == old_offset {
        return Ok(());
    }
    // Raising the offset: a writer still in flight with the old offset could
    // publish an ID below a stream frontier computed with the new one. Writers
    // hold ROW EXCLUSIVE from BEGIN; drain them, then observe again.
    attempt
        .execute(SqlStatement::new(
            "LOCK TABLE sys_system_stream_scope IN SHARE ROW EXCLUSIVE MODE",
            vec![],
        ))
        .await?;
    let (_, offset) = observe_postgres_positions(attempt).await?;
    let changed = attempt
        .execute(SqlStatement::new(
            "UPDATE sys_system_stream_scope SET transaction_offset = $1 WHERE scope_key = 'system'",
            vec![SqlValue::Integer(offset)],
        ))
        .await?;
    if changed != 1 {
        return Err(invalid_allocation());
    }
    Ok(())
}

/// The stored offset and the offset that covers every restored ID.
async fn observe_postgres_positions<A: SqlAttempt>(attempt: &mut A) -> StorageResult<(i64, i64)> {
    let page = attempt
        .query(
            SqlStatement::new(POSTGRES_STREAM_POSITIONS, vec![]),
            ReadProtection::Snapshot,
            storage_provider::SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    if page.more || page.rows.len() != 1 {
        return Err(invalid_allocation());
    }
    let row = &page.rows[0];
    let old_offset = integer(row, 0)?;
    // Every transaction ID assigned after the snapshot is above this value.
    let last_transaction = integer(row, 1)?;
    let system_high = integer(row, 2)?;
    // XID1 plus exactly eight bytes is the existing Postgres user-ID format.
    // Other explicit IDs are opaque and are not native clock authorities.
    let user_high = integer(row, 3)?;
    let offset = restored_transaction_offset(old_offset, last_transaction, system_high, user_high)?;
    Ok((
        i64::try_from(old_offset).map_err(|_| invalid_allocation())?,
        offset,
    ))
}

pub(super) fn restored_transaction_offset(
    old_offset: u64,
    transaction: u64,
    system_high: u64,
    user_high: u64,
) -> StorageResult<i64> {
    let high = system_high.max(
        user_high
            .checked_mul(ORDINALS_PER_TRANSACTION)
            .filter(|value| *value <= i64::MAX as u64)
            .ok_or_else(invalid_allocation)?,
    );
    let offset = old_offset.max((high / ORDINALS_PER_TRANSACTION).saturating_sub(transaction));
    mapped_offset(
        transaction
            .checked_add(offset)
            .ok_or_else(invalid_allocation)?,
    )?;
    i64::try_from(offset).map_err(|_| invalid_allocation())
}
