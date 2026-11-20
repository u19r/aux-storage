//! Physical table and GSI layout changes in the update's native SQL attempt.

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_common::{GsiLifecycleAction, GsiTableUpdatePlan};
use storage_types::{
    BoundTableInfo, GlobalSecondaryIndex, IndexName, StorageError, StorageResult, TableBinding,
    TimestampMillis, maintenance::TargetIncarnation,
};

use super::sql_statements;
use crate::{
    SqlAttempt, SqlLayout, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
    gsi::register_gsi_builds,
    item_schema::{projecting_key_columns, render_add_indexer_column, render_add_key_column},
    stream_history::capture_high,
};

pub(super) struct GsiLayoutChange<'a> {
    pub current: &'a BoundTableInfo,
    pub plan: &'a GsiTableUpdatePlan,
    pub targets: &'a [GsiBuildTarget],
    pub name_locked_until: TimestampMillis,
}

impl GsiLayoutChange<'_> {
    fn target(&self, index: &IndexName) -> StorageResult<&GsiBuildTarget> {
        self.targets
            .iter()
            .find(|target| {
                target.table_name == self.current.info.table_name && target.index_name == *index
            })
            .ok_or_else(|| {
                StorageError::internal("SQL GSI layout has no current build incarnation")
            })
    }
}

/// The layout effects of one table update: the GSI part of the next binding
/// when the GSI set changes, and the `ALTER TABLE` statements that the
/// attempt runs as its last batch (`SqlCatalogAttempt::flush_layout_ddl`).
pub(super) struct LayoutEffects {
    pub binding: Option<TableBinding>,
    pub alters: Vec<SqlStatement>,
}

pub(super) async fn write_layout<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    change: GsiLayoutChange<'_>,
) -> StorageResult<LayoutEffects> {
    let current = change.current;
    let plan = change.plan;
    let mut alters = indexer_slot_columns(engine, &change);
    let binding = match plan.lifecycle.as_ref() {
        Some(GsiLifecycleAction::Create(create)) => {
            let index: GlobalSecondaryIndex = create.clone().into();
            let incarnation = TargetIncarnation::from(*uuid::Uuid::now_v7().as_bytes());
            let frontier = capture_high(attempt, engine).await?;
            let (binding, build) = GsiBuild::prepare_native(
                current,
                &plan.next_table,
                GsiBuildTarget {
                    history_id: current.history_id,
                    table_name: current.info.table_name.clone(),
                    index_name: index.index_name.clone(),
                    incarnation,
                },
                frontier,
            )?;
            if !index.index_name.is_ttl_index() {
                alters.extend(key_columns(engine, current, &binding, &index.index_name)?);
            }
            register_gsi_builds(attempt, engine, vec![build]).await?;
            Some(binding)
        }
        Some(GsiLifecycleAction::Delete(delete)) => {
            delete_index(attempt, engine, &change, change.target(&delete.index_name)?).await?;
            Some(current.binding.without_gsi(&delete.index_name))
        }
        None => None,
    };
    Ok(LayoutEffects { binding, alters })
}

fn indexer_slot_columns(engine: impl SqlLayout, change: &GsiLayoutChange<'_>) -> Vec<SqlStatement> {
    let current = change.current;
    let old = current.max_indexers.as_usize();
    let next = change.plan.next_table.max_indexers.as_usize();
    let physical = engine.item_table_name(current.history_id);
    (old..next)
        .map(|slot| SqlStatement::new(render_add_indexer_column(&physical, slot), Vec::new()))
        .collect()
}

/// A new GSI gets a column for each key column that no other user GSI of the
/// table has. The partial index and the fill of existing rows
/// are W6F N3b.
fn key_columns(
    engine: impl SqlLayout,
    current: &BoundTableInfo,
    next: &TableBinding,
    index: &IndexName,
) -> StorageResult<Vec<SqlStatement>> {
    let gsi = next
        .gsi(index)
        .ok_or_else(|| StorageError::internal("SQL added GSI is absent from its binding"))?;
    Ok(gsi
        .columns
        .iter()
        .filter(|column| !projecting_key_columns(&current.binding).any(|known| known == *column))
        .map(|column| {
            SqlStatement::new(
                render_add_key_column(engine, column, current.history_id),
                Vec::new(),
            )
        })
        .collect())
}

async fn delete_index<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    change: &GsiLayoutChange<'_>,
    target: &GsiBuildTarget,
) -> StorageResult<()> {
    let table = change.current;
    let index = &target.index_name;
    let name = index.as_ref();
    let changed = attempt
        .execute(sql_statements::delete_build(engine, table.history_id, name))
        .await?;
    if changed != 1 {
        return Err(StorageError::internal(
            "SQL GSI deletion lost its build record",
        ));
    }
    attempt
        .execute(SqlStatement::new(
            format!(
                "INSERT INTO {relation_sys_gsi_name_locks} (table_id, index_name, locked_until) \
                 VALUES ({}) ON CONFLICT (table_id, index_name) DO UPDATE SET locked_until = \
                 excluded.locked_until",
                engine.engine().placeholders(3),
                relation_sys_gsi_name_locks = engine.relation_name("sys_gsi_name_locks"),
            ),
            vec![
                SqlValue::Text(uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string()),
                SqlValue::Text(index.to_string()),
                SqlValue::Integer(change.name_locked_until.timestamp_millis()),
            ],
        ))
        .await?;
    Ok(())
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    pub(super) fn stage_layout_ddl(&mut self, alters: Vec<SqlStatement>) -> StorageResult<()> {
        self.layout_ddl
            .try_reserve(alters.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        self.layout_ddl.extend(alters);
        Ok(())
    }

    /// Run the staged `ALTER TABLE` statements in one batch, the last before
    /// COMMIT: their ACCESS EXCLUSIVE lock is then held for one round trip.
    /// A waiting ACCESS EXCLUSIVE request queues every later reader and
    /// writer of the table; on Postgres the bounded wait fails the attempt as
    /// a conflict, which the operation retries.
    pub(crate) async fn flush_layout_ddl(&mut self) -> StorageResult<()> {
        if self.layout_ddl.is_empty() {
            return Ok(());
        }
        let mut statements = Vec::new();
        statements
            .try_reserve_exact(self.layout_ddl.len() + 1)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        #[cfg(feature = "postgres")]
        if self.engine.engine() == crate::SqlEngine::Postgres {
            statements.push(SqlStatement::new(
                format!(
                    "SET LOCAL lock_timeout = '{}ms'",
                    crate::constants::POSTGRES_DDL_LOCK_TIMEOUT.as_millis()
                ),
                Vec::new(),
            ));
        }
        statements.append(&mut self.layout_ddl);
        self.attempt.execute_batch(statements).await?;
        Ok(())
    }
}
