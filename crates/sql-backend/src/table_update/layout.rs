//! Physical table and GSI layout changes in the update's native SQL attempt.

use std::collections::HashSet;

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_common::{GsiLifecycleAction, GsiTableUpdatePlan};
use storage_types::{
    BoundTableInfo, GlobalSecondaryIndex, IndexName, PlannedKeyColumn, StorageError, StorageResult,
    StoredTableInfo, TimestampMillis, maintenance::TargetIncarnation,
};

use super::sql_statements;
use crate::{
    SqlAttempt, SqlLayout, SqlStatement, SqlValue,
    gsi::register_gsi_builds,
    item_schema::{
        render_add_indexer_column, render_add_union_key_column, render_gsi_read_index,
        render_gsi_table,
    },
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

pub(super) async fn write_layout<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    change: GsiLayoutChange<'_>,
) -> StorageResult<()> {
    let current = change.current;
    let plan = change.plan;
    grow_indexer_slots(attempt, engine, &change).await?;
    match plan.lifecycle.as_ref() {
        Some(GsiLifecycleAction::Create(create)) => {
            let index: GlobalSecondaryIndex = create.clone().into();
            let incarnation = TargetIncarnation::from(*uuid::Uuid::now_v7().as_bytes());
            let frontier = capture_high(attempt, engine).await?;
            if !index.index_name.is_ttl_index() {
                grow_item_union(attempt, engine, current, &plan.next_table).await?;
                for sql in [
                    render_gsi_table(engine, &plan.next_table, &index, &incarnation),
                    render_gsi_read_index(engine, &plan.next_table, &index, &incarnation),
                ] {
                    attempt.execute(SqlStatement::new(sql, Vec::new())).await?;
                }
            }
            let build = GsiBuild::<()>::new(
                GsiBuildTarget {
                    table_name: current.info.table_name.clone(),
                    index_name: index.index_name,
                    incarnation,
                },
                frontier,
            );
            register_gsi_builds(attempt, engine, current.history_id, vec![build]).await?;
        }
        Some(GsiLifecycleAction::Delete(delete)) => {
            delete_index(attempt, engine, &change, change.target(&delete.index_name)?).await?;
        }
        None => {}
    }
    Ok(())
}

async fn grow_indexer_slots<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    change: &GsiLayoutChange<'_>,
) -> StorageResult<()> {
    let current = change.current;
    let plan = change.plan;
    let old = current.max_indexers.as_usize();
    let next = plan.next_table.max_indexers.as_usize();
    if next <= old {
        return Ok(());
    }
    let mut relations = vec![engine.item_table_name(&current.table_name, current.history_id)];
    let gsi_relations = current
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|index| {
            !index.index_name.is_ttl_index()
                && !matches!(plan.lifecycle.as_ref(),
            Some(GsiLifecycleAction::Delete(delete)) if delete.index_name == index.index_name)
        })
        .map(|index| {
            let target = change.target(&index.index_name)?;
            Ok(engine.gsi_table_name(&current.table_name, &index.index_name, &target.incarnation))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    relations.extend(gsi_relations);
    for physical in relations {
        for slot in old..next {
            attempt
                .execute(SqlStatement::new(
                    render_add_indexer_column(&physical, slot),
                    Vec::new(),
                ))
                .await?;
        }
    }
    Ok(())
}

async fn grow_item_union<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    current: &BoundTableInfo,
    next: &StoredTableInfo,
) -> StorageResult<()> {
    let old = current
        .attribute_definitions
        .iter()
        .map(|definition| definition.attribute_name.as_str())
        .collect::<HashSet<_>>();
    let statements = next
        .attribute_definitions
        .iter()
        .filter(|definition| !old.contains(definition.attribute_name.as_str()))
        .map(PlannedKeyColumn::from)
        .map(|column| {
            SqlStatement::new(
                render_add_union_key_column(engine, next, &column, current.history_id),
                Vec::new(),
            )
        })
        .collect();
    attempt.execute_batch(statements).await?;
    Ok(())
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
    attempt
        .execute(sql_statements::delete_history_holds(
            engine,
            &table.table_name,
            name,
        ))
        .await?;
    let changed = attempt
        .execute(sql_statements::delete_build(
            engine,
            &table.table_name,
            name,
        ))
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
