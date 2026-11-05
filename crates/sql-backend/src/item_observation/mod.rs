//! Protected physical observation plans for SQL item writes.

mod decode;
#[cfg(test)]
mod decode_tests;
mod sql_statements;

pub(crate) use decode::{decode_write_observations, match_observation_rows};
use sql_statements::{deleted_observation_batch_statement, live_observation_statement};
use storage_provider::{ItemWriteObservationRead, lifecycle_tables_for_writes};
use storage_types::{BoundTableInfo, StorageResult};

use crate::{ReadProtection, SqlEngine, SqlLayout, SqlStatement, gsi::gsi_lifecycle_statement};

pub(crate) struct WriteObservationReads<'a> {
    pub(crate) statements: Vec<(SqlStatement, ReadProtection)>,
    pub(crate) live_groups: Vec<Vec<usize>>,
    pub(crate) lifecycle_tables: Vec<&'a BoundTableInfo>,
}

pub(crate) fn prepare_observation_reads<'a>(
    engine: impl SqlLayout,
    reads: &[ItemWriteObservationRead<'a>],
) -> StorageResult<WriteObservationReads<'a>> {
    let lifecycle_tables = lifecycle_tables_for_writes(reads)?;
    let tables = group_observation_tables(reads)?;
    let count = tables
        .len()
        .checked_add(lifecycle_tables.len())
        .and_then(|count| count.checked_add(2))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    if engine.engine() == SqlEngine::Postgres && !reads.is_empty() {
        statements.push((
            crate::item_lock::item_lock_statement(
                reads.iter().map(ItemWriteObservationRead::target),
            )?,
            ReadProtection::Snapshot,
        ));
    }
    let live_groups = prepare_live_observations(engine, reads, tables, &mut statements)?;
    if !reads.is_empty() {
        statements.push((
            deleted_observation_batch_statement(engine, reads)?,
            ReadProtection::Snapshot,
        ));
    }
    statements.extend(
        lifecycle_tables
            .iter()
            .filter_map(|table| gsi_lifecycle_statement(engine, table))
            .map(|statement| (statement, ReadProtection::Snapshot)),
    );
    Ok(WriteObservationReads {
        statements,
        live_groups,
        lifecycle_tables,
    })
}

fn group_observation_tables<'a>(
    reads: &[ItemWriteObservationRead<'a>],
) -> StorageResult<Vec<&'a BoundTableInfo>> {
    let mut tables: Vec<&BoundTableInfo> = Vec::new();
    for read in reads {
        let table = read.target().0;
        if !tables
            .iter()
            .any(|known| known.history_id == table.history_id)
        {
            tables
                .try_reserve(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            tables.push(table);
        }
    }
    Ok(tables)
}

fn prepare_live_observations(
    engine: impl SqlLayout,
    reads: &[ItemWriteObservationRead<'_>],
    tables: Vec<&BoundTableInfo>,
    statements: &mut Vec<(SqlStatement, ReadProtection)>,
) -> StorageResult<Vec<Vec<usize>>> {
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(tables.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for table in tables {
        let indices = reads
            .iter()
            .enumerate()
            .filter_map(|(index, read)| {
                (read.target().0.history_id == table.history_id).then_some(index)
            })
            .collect::<Vec<_>>();
        // The preceding advisory lock owns exact-item exclusion on Postgres;
        // observations must execute after it in the native statement pipeline.
        statements.push((
            live_observation_statement(engine, table, reads, &indices)?,
            ReadProtection::Snapshot,
        ));
        groups.push(indices);
    }
    Ok(groups)
}
