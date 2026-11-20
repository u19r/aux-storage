use storage_provider::ItemWriteObservationRead;
use storage_types::{BoundTableInfo, StorageResult};

use super::sql_statements::live_observation_statement;
use crate::{ReadProtection, SqlLayout, SqlStatement};

pub(crate) struct WriteObservationReads {
    pub(crate) statements: Vec<(SqlStatement, ReadProtection)>,
    pub(crate) live_groups: Vec<Vec<usize>>,
    pub(crate) marker_groups: Vec<Vec<usize>>,
}

pub(crate) fn prepare_observation_reads(
    engine: impl SqlLayout,
    reads: &[ItemWriteObservationRead<'_>],
) -> StorageResult<WriteObservationReads> {
    let tables = group_observation_tables(reads)?;
    let count = tables.len();
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    let paired = tables.iter().any(|table| table.binding.regional.is_some());
    let live_groups = prepare_live_observations(engine, reads, tables, &mut statements)?;
    let mut marker_groups = Vec::new();
    for table in if paired {
        group_observation_tables(reads)?
    } else {
        Vec::new()
    } {
        let indices = reads
            .iter()
            .enumerate()
            .filter_map(|(index, read)| {
                (read.reads_marker() && read.target().0.history_id == table.history_id)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        if indices.is_empty() {
            continue;
        }
        statements.push((
            crate::regional::marker_observation_statement(
                engine,
                table,
                indices.iter().map(|index| reads[*index].target().1),
            )?,
            ReadProtection::Snapshot,
        ));
        marker_groups.push(indices);
    }
    Ok(WriteObservationReads {
        statements,
        live_groups,
        marker_groups,
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
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
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
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for table in tables {
        let indices = reads
            .iter()
            .enumerate()
            .filter_map(|(index, read)| {
                (read.target().0.history_id == table.history_id).then_some(index)
            })
            .collect::<Vec<_>>();
        // The item lock that the caller puts at the head of the window owns
        // exact-item exclusion on Postgres; observations execute after it.
        statements.push((
            live_observation_statement(engine, table, reads, &indices)?,
            ReadProtection::Snapshot,
        ));
        groups.push(indices);
    }
    Ok(groups)
}
