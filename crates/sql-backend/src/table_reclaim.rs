//! Physical SQL reclamation after a deleted incarnation leaves the public
//! catalog.

use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, TableReclaimAttempt, TableReclaimPhase, TableReclaimStep,
    TableReclaimTarget,
};
use storage_types::{StorageError, StorageResult, storage_serde};

use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlStatement, SqlValue, catalog::SqlCatalogAttempt,
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

impl<A: SqlAttempt, L: SqlLayout> TableReclaimAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_table_system_rows(
        &mut self,
        history: storage_types::TableHistoryId,
        limit: std::num::NonZeroU32,
    ) -> StorageResult<storage_provider::TableSystemHistoryPage> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = crate::table_history_reclaim::load_table_system_rows(
            catalog.attempt,
            catalog.engine,
            history,
            limit,
        )
        .await;
        operation.complete(result)
    }

    async fn store_table_system_removals(
        &mut self,
        history: storage_types::TableHistoryId,
        rows: &[storage_types::SystemHistoryRecord],
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = crate::table_history_reclaim::store_table_system_removals(
            catalog.attempt,
            catalog.engine,
            history,
            rows,
        )
        .await;
        operation.complete(result)
    }

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
            TableReclaimPhase::ExternalData | TableReclaimPhase::SystemHistory => {
                return Err(StorageError::internal(
                    "Table reclaim phase requires its domain owner",
                ));
            }
            TableReclaimPhase::TableHistory => {
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
                let expected = statements.len();
                let counts = self.attempt.execute_batch(statements).await?;
                if counts.len() != expected {
                    return Err(StorageError::internal("SQL metadata reclaim count differs"));
                }
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
            TableReclaimPhase::PrimaryTombstones => {
                let count = self
                    .attempt
                    .execute(SqlStatement::new(
                        format!(
                            "DELETE FROM {relation} WHERE table_id = {} AND key_json IN (SELECT \
                             key_json FROM {relation} WHERE table_id = {} ORDER BY key_json LIMIT \
                             64)",
                            self.engine.engine().placeholder(1),
                            self.engine.engine().placeholder(2),
                            relation = self.engine.relation_name("item_tombstones"),
                        ),
                        vec![
                            SqlValue::Blob(history.as_ref().to_vec()),
                            SqlValue::Blob(history.as_ref().to_vec()),
                        ],
                    ))
                    .await?;
                if count == 64 {
                    return Ok(TableReclaimStep::Advanced);
                }
            }
            TableReclaimPhase::GsiProjections => {
                let statements = target
                    .gsi_targets
                    .iter()
                    .map(|gsi| {
                        let relation = self.engine.gsi_table_name(&gsi.incarnation);
                        SqlStatement::new(format!("DROP TABLE IF EXISTS \"{relation}\""), vec![])
                    })
                    .collect();
                if !target.gsi_targets.is_empty() {
                    self.attempt.execute_batch(statements).await?;
                }
            }
            TableReclaimPhase::TtlIndex => {
                drop_relation(self.attempt, self.engine.ttl_table_name(history)).await?;
            }
            TableReclaimPhase::GsiTombstones | TableReclaimPhase::GsiNameLocks => {}
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
