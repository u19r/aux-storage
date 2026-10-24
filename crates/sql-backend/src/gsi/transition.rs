//! Conditional SQL GSI effects rendered into one native write batch.

use std::ops::RangeInclusive;

use storage_backfill::GsiBuild;
use storage_provider::{GsiWriteSource, maintenance::GsiTransition};
use storage_types::{
    BoundTableInfo, StorageEnum, StorageError, StorageResult, maintenance::MaintenanceKind,
};

use super::{GsiWriteInput, prepare_gsi_writes, sql_statements};
use crate::{SqlAttempt, SqlLayout, SqlStatement, item_write::ttl_index_statements, maintenance};

pub(crate) async fn apply_gsi_transition<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut crate::catalog::SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    transition: GsiTransition<'_, Vec<u8>>,
) -> StorageResult<()> {
    let writes = transition_statements(catalog, table, transition).await?;
    let mut statements = Vec::new();
    let mut expected = Vec::new();
    statements
        .try_reserve_exact(writes.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    expected
        .try_reserve_exact(writes.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for (statement, count) in writes {
        statements.push(statement);
        expected.push(count);
    }
    let changed = catalog.attempt.execute_batch(statements).await?;
    if changed.len() != expected.len() {
        return Err(StorageError::internal(
            "SQL GSI transition write count differs",
        ));
    }
    for (count, expected) in changed.into_iter().zip(expected) {
        if count < *expected.start() {
            return Err(StorageEnum::TransactionConflict {
                message: "GSI transition lost a conditional write fence".to_owned(),
            }
            .into());
        }
        if count > *expected.end() {
            return Err(StorageError::internal(
                "SQL GSI transition changed the wrong row count",
            ));
        }
    }
    Ok(())
}

async fn transition_statements<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut crate::catalog::SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    transition: GsiTransition<'_, Vec<u8>>,
) -> StorageResult<Vec<(SqlStatement, RangeInclusive<u64>)>> {
    let engine = catalog.engine;
    let GsiTransition {
        expected_revision,
        next_progress,
        expected,
        projections,
        next_state,
        ttl_activation,
    } = transition;
    let statement = sql_statements::transition_state_statement(engine, &expected, &next_state)?;
    let (projections, ttl_writes) = projections.separate_ttl_writes()?;
    let projection_writes = prepare_gsi_writes(
        catalog,
        GsiWriteInput {
            table,
            source: GsiWriteSource::Backfill,
            writes: projections,
            targets: std::slice::from_ref(&expected.target),
            prepared: &mut Vec::new(),
        },
    )
    .await?;
    let ttl_writes = ttl_index_statements(engine, table, ttl_writes)?;
    let next = GsiBuild {
        target: expected.target.clone(),
        state: next_state,
    };
    let hold_writes = history_hold_statements(engine, table, &expected, &next)?;
    let count = projection_writes
        .len()
        .checked_add(ttl_writes.len())
        .and_then(|count| count.checked_add(hold_writes.len()))
        .and_then(|count| count.checked_add(2 + usize::from(ttl_activation.is_some())))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut writes = Vec::new();
    writes
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    writes.push((statement, 1..=1));
    writes.extend(
        projection_writes
            .into_iter()
            .map(|statement| (statement, 1..=1)),
    );
    writes.extend(ttl_writes.into_iter().map(|statement| (statement, 0..=1)));
    writes.extend(hold_writes);
    if let Some(activation) = ttl_activation {
        writes.push((
            sql_statements::activate_ttl_statement(engine, table.history_id, activation)?,
            1..=1,
        ));
    }
    writes.push((
        maintenance::progress_statement(
            engine,
            MaintenanceKind::GsiBackfill,
            expected_revision,
            &next_progress,
        )?,
        1..=1,
    ));
    Ok(writes)
}

fn history_hold_statements(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    expected: &GsiBuild<Vec<u8>>,
    next: &GsiBuild<Vec<u8>>,
) -> StorageResult<Vec<(SqlStatement, RangeInclusive<u64>)>> {
    let Some((old, new)) = expected.history_hold_change(&next.state) else {
        return Ok(Vec::new());
    };
    let mut writes = Vec::new();
    let count = old
        .frontier()
        .len()
        .checked_add(new.frontier().len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    writes
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    // Delete only departed SQL primary keys; retained lineage holds update in
    // place. No delete depends on an insert or update elsewhere in this batch.
    for position in old.frontier() {
        if !new
            .frontier()
            .iter()
            .any(|next| next.lineage_id() == position.lineage_id())
        {
            writes.push((
                sql_statements::delete_history_hold_statement(
                    engine,
                    table.history_id,
                    &expected.target,
                    position,
                ),
                1..=1,
            ));
        }
    }
    for position in new.frontier() {
        writes.push((
            sql_statements::store_history_hold_statement(engine, table.history_id, next, position)?,
            1..=1,
        ));
    }
    Ok(writes)
}
