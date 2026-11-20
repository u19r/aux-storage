//! Conditional SQL GSI effects rendered into one native write batch.

use std::ops::RangeInclusive;

use storage_provider::maintenance::{GsiTransition, ProgressWrite};
use storage_types::{BoundTableInfo, MissExpectation, StorageError, StorageResult};

use super::sql_statements;
use crate::{SqlAttempt, SqlLayout, SqlStatement, maintenance};

pub(crate) async fn apply_gsi_transition<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut crate::catalog::SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    transition: GsiTransition<'_, Vec<u8>>,
    progress: ProgressWrite,
) -> StorageResult<()> {
    let binding = transition.next_binding(&table.binding)?;
    let ttl_activation = transition.ttl_activation.is_some();
    let mut writes = transition_statements(catalog, table, transition, progress).await?;
    if let Some(binding) = &binding {
        writes.push((binding_statement(catalog.engine, table, binding)?, 1..=1));
    }
    let mut statements = Vec::new();
    let mut expected = Vec::new();
    statements
        .try_reserve_exact(writes.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    expected
        .try_reserve_exact(writes.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for (statement, count) in writes {
        statements.push(statement);
        expected.push(count);
    }
    // Only a writer-visible phase change or TTL activation changes the
    // binding and its stamp; other pages keep the shared catalog lock.
    let changed = if binding.is_some() || ttl_activation {
        catalog
            .execute_catalog_change(&table.info.table_name, statements)
            .await?
    } else {
        catalog.attempt.execute_batch_exact(statements).await?
    };
    for (count, expected) in changed.into_iter().zip(expected) {
        if count < *expected.start() {
            return Err(StorageError::write_miss(
                "gsi_transition",
                MissExpectation::Rows {
                    expected: *expected.start(),
                    found: count,
                },
            ));
        }
        if count > *expected.end() {
            return Err(StorageError::internal(
                "SQL GSI transition changed the wrong row count",
            ));
        }
    }
    Ok(())
}

/// Store the next GSI binding if the stored one is still the observed one.
pub(super) fn binding_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    next: &storage_types::TableBinding,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "UPDATE {relation_tables} SET gsi_blob = {}, catalog_revision = catalog_revision + 1 \
             WHERE id = {} AND gsi_blob = {} AND catalog_revision < {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            i64::MAX,
            relation_tables = engine.relation_name("tables"),
        ),
        vec![
            crate::catalog::encode_gsi_binding(next)?,
            crate::SqlValue::Text(uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string()),
            crate::catalog::encode_gsi_binding(&table.binding)?,
        ],
    ))
}

async fn transition_statements<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut crate::catalog::SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    transition: GsiTransition<'_, Vec<u8>>,
    progress: ProgressWrite,
) -> StorageResult<Vec<(SqlStatement, RangeInclusive<u64>)>> {
    let engine = catalog.engine;
    let GsiTransition {
        expected_revision: _,
        next_progress: _,
        expected,
        projections,
        primary_ttl_metadata,
        next_state,
        ttl_activation,
    } = transition;
    let statement = sql_statements::transition_state_statement(engine, &expected, &next_state)?;
    // User GSI fills already changed primary columns. TTL metadata below joins
    // this transition batch; no SQL projection relation remains to mutate.
    if projections
        .into_iter()
        .any(|write| !write.key.index_name.is_ttl_index())
    {
        return Err(StorageError::internal(
            "SQL projection rows are only valid for TTL",
        ));
    }
    let count = primary_ttl_metadata
        .len()
        .checked_add(2 + usize::from(ttl_activation.is_some()))
        .ok_or(storage_types::AttemptFailure::CAPACITY)?;
    let mut writes = Vec::new();
    writes
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    writes.push((statement, 1..=1));
    for row in primary_ttl_metadata {
        writes.push((
            sql_statements::ttl_metadata_statement(engine, table, row)?,
            1..=1,
        ));
    }
    if let Some(activation) = ttl_activation {
        writes.push((
            sql_statements::activate_ttl_statement(engine, table.history_id, activation)?,
            1..=1,
        ));
    }
    writes.push((maintenance::progress_statement(engine, progress)?, 1..=1));
    Ok(writes)
}
