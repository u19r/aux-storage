//! Physical SQL reclamation after a deleted incarnation leaves the public
//! catalog.

use storage_backfill::GsiBuildTarget;
use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, ReclaimAttempt, TableReclaimPhase, TableReclaimStep,
    TableReclaimTarget,
};
use storage_types::{
    StorageError, StorageResult, TableHistoryId, maintenance::TargetIncarnation, storage_serde,
};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt, gsi_reclaim_sql_statements,
};

pub(crate) async fn initialize_table_reclaim<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(crate::table_reclaim_sql_statements::schema(engine))
        .await?;
    Ok(())
}

impl<A: SqlAttempt, L: SqlLayout> ReclaimAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_reclaim_target(&mut self) -> StorageResult<Option<TableReclaimTarget>> {
        let page = self
            .attempt
            .query(
                crate::table_reclaim_sql_statements::load_target(self.engine),
                ReadProtection::Conflict,
                CATALOG_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL reclaim discovery returned multiple targets",
            ));
        }
        let Some(row) = page.rows.into_iter().next() else {
            return Ok(None);
        };
        let target = decode_reclaim_target(row)?;
        if self
            .read_catalog_stamp(&target.table_name)
            .await?
            .is_some_and(|stamp| stamp.history_id == target.history_id)
        {
            return Err(StorageError::internal(
                "SQL reclaim marker targets an active incarnation",
            ));
        }
        Ok(Some(target))
    }

    async fn delete_reclaim_page(
        &mut self,
        target: &TableReclaimTarget,
    ) -> StorageResult<TableReclaimStep> {
        let history = target.history_id;
        match target.phase {
            TableReclaimPhase::ExternalData => {
                return Err(StorageError::internal(
                    "Table reclaim phase requires its domain owner",
                ));
            }
            TableReclaimPhase::StreamEntries => {
                return crate::table_history_reclaim::delete_table_history_rows(
                    self.attempt,
                    self.engine,
                    history,
                )
                .await;
            }
            TableReclaimPhase::MetadataCleanup => {
                let mut statements = Vec::new();
                statements.push(SqlStatement::new(
                    format!(
                        "DELETE FROM {relation_sys_stream_trim_due_markers} WHERE (due_at, \
                         scope_id, policy_version) IN (SELECT due_at, scope_id, policy_version \
                         FROM {relation_sys_stream_trim_due_markers} WHERE scope_id = {} LIMIT 64)",
                        self.engine.engine().placeholder(1),
                        relation_sys_stream_trim_due_markers =
                            self.engine.relation_name("sys_stream_trim_due_markers"),
                    ),
                    vec![SqlValue::Text(storage_provider::table_retention_scope_id(
                        history,
                    ))],
                ));
                let counts = self.attempt.execute_batch_exact(statements).await?;
                if counts.into_iter().any(|count| count == 64) {
                    return Ok(TableReclaimStep::Advanced);
                }
            }
            TableReclaimPhase::ExplicitAppends => {
                return crate::stream_write::delete_table_appends(
                    self.attempt,
                    self.engine,
                    history,
                )
                .await;
            }
            TableReclaimPhase::PrimaryItems => {
                drop_relation(self.attempt, self.engine.item_table_name(history)).await?;
            }
            TableReclaimPhase::TtlIndex => {}
            // GSI partial indexes go with the item relation.
            TableReclaimPhase::GsiProjections | TableReclaimPhase::GsiNameLocks => {}
        }
        Ok(TableReclaimStep::Complete)
    }

    async fn store_reclaim_phase(
        &mut self,
        target: &TableReclaimTarget,
        next: Option<TableReclaimPhase>,
    ) -> StorageResult<()> {
        let statements = if let Some(phase) = next {
            let mut next_target = target.clone();
            next_target.phase = phase;
            vec![crate::table_reclaim_sql_statements::store_target(
                self.engine,
                &next_target,
            )?]
        } else {
            crate::table_reclaim_sql_statements::finish_target(self.engine, target.history_id)
                .into()
        };
        let expected = statements.len();
        let counts = self.attempt.execute_batch(statements).await?;
        if counts.len() != expected || counts.iter().any(|count| *count != 1) {
            return Err(StorageError::internal(
                "SQL reclaim target changed during phase update",
            ));
        }
        Ok(())
    }

    async fn load_index_incarnation(
        &mut self,
        _history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> StorageResult<Option<TargetIncarnation>> {
        let page = self
            .attempt
            .query(
                gsi_reclaim_sql_statements::load_incarnation(self.engine, target),
                ReadProtection::Conflict,
                CATALOG_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal("SQL index binding page is invalid"));
        }
        page.rows
            .first()
            .map(|row| {
                let SqlValue::Blob(bytes) = row.value(ColumnIndex::new(0))? else {
                    return Err(StorageError::internal("SQL index binding type is invalid"));
                };
                TargetIncarnation::try_from(bytes.clone())
                    .map_err(|_| StorageError::internal("SQL index binding incarnation is invalid"))
            })
            .transpose()
    }

    async fn delete_index_page(
        &mut self,
        history: TableHistoryId,
        _target: &GsiBuildTarget,
        native: Option<storage_provider::NativeGsiRetirement>,
    ) -> StorageResult<TableReclaimStep> {
        let Some(native) = native else {
            return Ok(TableReclaimStep::Complete);
        };
        let page = self
            .attempt
            .query(
                crate::gsi::index_state_statement(self.engine, history, &native.index),
                ReadProtection::Snapshot,
                CATALOG_READ_BYTES,
            )
            .await?;
        if !page.rows.is_empty() {
            self.deferred_ddl.push(crate::ddl::SqlDdl::drop_index(
                self.engine,
                history,
                &native.index,
            ));
            return Ok(TableReclaimStep::Advanced);
        }
        // SQLite and Turso retire unused columns in place; dropping them
        // would rewrite every item. A future GSI always gets a new generation.
        if self.engine.engine() != crate::SqlEngine::Postgres || native.unused_columns.is_empty() {
            return Ok(TableReclaimStep::Complete);
        }
        let page = self
            .attempt
            .query(
                gsi_reclaim_sql_statements::load_columns(
                    self.engine,
                    history,
                    &native.unused_columns,
                ),
                ReadProtection::Snapshot,
                CATALOG_READ_BYTES,
            )
            .await?;
        if !page.rows.is_empty() {
            for column in native.unused_columns.iter().filter(|column| page.rows.iter().any(|row| {
                matches!(row.value(ColumnIndex::new(0)), Ok(SqlValue::Text(name)) if *name == column.physical_name())
            })) {
                self.deferred_ddl.push(crate::ddl::SqlDdl::drop_column(self.engine, history, column));
            }
            return Ok(TableReclaimStep::Advanced);
        }
        Ok(TableReclaimStep::Complete)
    }
}

async fn drop_relation<A: SqlAttempt>(attempt: &mut A, relation: String) -> StorageResult<()> {
    attempt
        .execute(SqlStatement::new(
            format!("DROP TABLE IF EXISTS \"{relation}\""),
            vec![],
        ))
        .await?;
    Ok(())
}

fn decode_reclaim_target(row: crate::SqlRow) -> StorageResult<TableReclaimTarget> {
    let values: [SqlValue; 2] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("Invalid SQL reclaim target width"))?;
    let [SqlValue::Text(id), SqlValue::Blob(bytes)] = values else {
        return Err(StorageError::internal("Invalid SQL reclaim target types"));
    };
    let target: TableReclaimTarget = storage_serde::from_bytes(&bytes)?;
    if id != uuid::Uuid::from_bytes(*target.history_id.as_ref()).to_string() {
        return Err(StorageError::internal(
            "SQL reclaim target differs from its marker",
        ));
    }
    Ok(target)
}
