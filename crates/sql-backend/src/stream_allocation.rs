//! Postgres transaction allocation floor and safe, sparse stream offsets.

use storage_types::{StorageError, StorageResult};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement, SqlValue,
    constants::ORDINALS_PER_TRANSACTION,
};

pub(crate) struct NativeStreamAllocation {
    next: u64,
    remaining: u32,
}

impl NativeStreamAllocation {
    pub(crate) fn from_history(page: &SqlQueryPage) -> StorageResult<Self> {
        let row = page.rows.first().ok_or_else(invalid_allocation)?;
        let next = mapped_offset(integer(row, 12)?)?;
        Ok(Self {
            next,
            remaining: ORDINALS_PER_TRANSACTION as u32,
        })
    }

    pub(crate) fn reserve(&mut self) -> StorageResult<u64> {
        if self.remaining == 0 {
            return Err(invalid_allocation());
        }
        let offset = self.next;
        self.next = self.next.checked_add(1).ok_or_else(invalid_allocation)?;
        self.remaining -= 1;
        Ok(offset)
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
    selected: &[stream_provider::SystemStreamTrimRecord],
) -> StorageResult<()> {
    let mut offsets = std::collections::BTreeMap::new();
    for record in selected {
        let offset = record
            .position
            .offset()
            .order()
            .map_err(|_| invalid_allocation())?;
        let offset = i64::try_from(offset).map_err(|_| invalid_allocation())?;
        offsets
            .entry(record.position.shard_id().as_str())
            .and_modify(|stored: &mut i64| *stored = (*stored).max(offset))
            .or_insert(offset);
    }
    let statements = offsets
        .into_iter()
        .map(|(shard, offset)| {
            SqlStatement::new(
                crate::sql_template::sql_template!(
                    engine,
                    "UPDATE "["sys_system_stream_sequences"],
                    " SET retired_offset = GREATEST(retired_offset, $1) WHERE scope_key = \
                     'system' AND shard_id = $2"
                ),
                vec![SqlValue::Integer(offset), SqlValue::Text(shard.to_owned())],
            )
        })
        .collect();
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
    reads: &[stream_provider::SystemStreamHistoryRead],
) -> StorageResult<Vec<Option<storage_types::SystemStreamPosition>>> {
    let statements = reads
        .iter()
        .map(|read| {
            SqlStatement::new(
                crate::sql_template::sql_template!(
                    engine,
                    "SELECT retired_offset FROM "["sys_system_stream_sequences"],
                    " WHERE scope_key = 'system' AND shard_id = $1"
                ),
                vec![SqlValue::Text(read.after.shard_id().as_str().to_owned())],
            )
        })
        .collect();
    let pages = attempt
        .query_batch(
            statements,
            ReadProtection::Snapshot,
            storage_provider::SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    if pages.len() != reads.len() {
        return Err(invalid_allocation());
    }
    pages
        .into_iter()
        .zip(reads)
        .map(|(page, read)| {
            if page.more || page.rows.len() != 1 {
                return Err(invalid_allocation());
            }
            let offset = integer(&page.rows[0], 0)?;
            if offset == 0 {
                return Ok(None);
            }
            storage_types::SystemStreamPosition::try_new(
                read.after.shard_id().as_str(),
                read.after.lineage_id().as_str(),
                format!("{offset:020}"),
            )
            .map(Some)
            .map_err(|_| invalid_allocation())
        })
        .collect()
}
