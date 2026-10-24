//! Bounded destination-state observations on the caller's SQL attempt.

use storage_backfill::{
    GsiProjection, GsiProjectionKey, GsiProjectionStateRead, GsiProjectionStateReadBatch,
    GsiSourceProjection,
};
use storage_provider::CATALOG_READ_BYTES;
use storage_types::{ItemRevision, StorageError, StorageResult, TableHistoryId};

use super::sql_statements;
use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlValue,
    constants::GSI_PROJECTION_STATE_READ_ROWS as STATE_READ_ROWS,
};

pub(crate) async fn read_projection_states<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    batch: GsiProjectionStateReadBatch<'_>,
    history_id: TableHistoryId,
) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
    if batch.table.table_name != batch.target.table_name {
        return Err(StorageError::internal(
            "SQL GSI destination table has changed",
        ));
    }
    let index = batch
        .table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|index| index.index_name == batch.target.index_name);
    if index.is_none() && !batch.target.index_name.is_ttl_index() {
        return Err(StorageError::internal(
            "SQL GSI destination index has changed",
        ));
    }
    let mut current = (0..batch.sources.len()).map(|_| None).collect::<Vec<_>>();
    let chunks = batch.reads.chunks(STATE_READ_ROWS).collect::<Vec<_>>();
    let mut statements = Vec::with_capacity(chunks.len());
    for reads in &chunks {
        let keys = reads.iter().map(|read| read.key).collect::<Vec<_>>();
        if keys
            .iter()
            .any(|key| key.index_name != batch.target.index_name)
        {
            return Err(StorageError::internal(
                "SQL GSI state key has another index",
            ));
        }
        let statement = if batch.target.index_name.is_ttl_index() {
            sql_statements::ttl_projection_state_statement(engine, batch.table, &keys, history_id)?
        } else {
            sql_statements::projection_state_statement(
                engine,
                batch.table,
                index.ok_or_else(|| {
                    StorageError::internal("SQL GSI destination index has changed")
                })?,
                &keys,
                &batch.target.incarnation,
            )?
        };
        // The write attempt is serializable; this grouped left join
        // participates in its predicate conflicts without Postgres FOR UPDATE
        // on a nullable side.
        statements.push(statement);
    }
    let pages = attempt
        .query_batch(statements, ReadProtection::Snapshot, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != chunks.len() {
        return Err(StorageError::internal(
            "SQL GSI destination query batch count differs",
        ));
    }
    for (reads, page) in chunks.into_iter().zip(pages) {
        decode_projection_chunk(&batch, reads, page, &mut current)?;
    }
    Ok(current)
}

fn decode_projection_chunk(
    batch: &GsiProjectionStateReadBatch<'_>,
    reads: &[GsiProjectionStateRead<'_>],
    page: SqlQueryPage,
    current: &mut [Option<GsiProjection<GsiProjectionKey>>],
) -> StorageResult<()> {
    if page.more || page.rows.len() != reads.len() {
        return Err(StorageError::internal(
            "SQL GSI destination batch count is invalid",
        ));
    }
    let mut seen = vec![false; reads.len()];
    for row in page.rows {
        let [ordinal, tombstone, revision]: [SqlValue; 3] = row
            .into_values()
            .try_into()
            .map_err(|_| StorageError::internal("SQL GSI destination row width is invalid"))?;
        let SqlValue::Integer(ordinal) = ordinal else {
            return Err(StorageError::internal(
                "SQL GSI destination ordinal is invalid",
            ));
        };
        let ordinal = usize::try_from(ordinal)
            .map_err(|_| StorageError::internal("SQL GSI destination ordinal is negative"))?;
        let slot = seen
            .get_mut(ordinal)
            .ok_or_else(|| StorageError::internal("SQL GSI destination ordinal is out of range"))?;
        if std::mem::replace(slot, true) {
            return Err(StorageError::internal(
                "SQL GSI destination ordinal is duplicated",
            ));
        }
        let source_ordinal = reads[ordinal].source_ordinal;
        let source = batch
            .sources
            .get(source_ordinal)
            .ok_or_else(|| StorageError::internal("SQL GSI source ordinal is invalid"))?;
        if let Some((version, live)) = decode_stored_state(
            source,
            tombstone,
            revision,
            batch.target.index_name.is_ttl_index(),
        )? {
            current[source_ordinal] =
                Some(GsiProjection::from_stored_state(source, version, live)?);
        }
    }
    Ok(())
}

fn decode_stored_state(
    source: &GsiSourceProjection,
    tombstone: SqlValue,
    revision: SqlValue,
    is_ttl: bool,
) -> StorageResult<Option<(ItemRevision, bool)>> {
    let live = match tombstone {
        SqlValue::Null => return Ok(None),
        SqlValue::Integer(0) => true,
        SqlValue::Integer(1) => false,
        _ => return Err(StorageError::internal("SQL GSI tombstone value is invalid")),
    };
    let version = match revision {
        SqlValue::Integer(version) => ItemRevision::try_from(version)?,
        SqlValue::Null if is_ttl => source.item_version(),
        _ => return Err(StorageError::internal("SQL GSI revision is invalid")),
    };
    Ok(Some((version, live)))
}
