//! SQL table deletion under the catalog operation's native attempt.

use storage_provider::{
    DeleteTableAttempt, TableReclaimPhase, TableReclaimTarget, name_locked_until,
    table_retention_scope_id,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult, TimestampMillis, storage_serde};

use crate::{
    SqlAttempt, SqlLayout, SqlStatement, SqlValue, catalog::SqlCatalogAttempt,
    gsi::read_gsi_readiness,
};

impl<A: SqlAttempt, L: SqlLayout> DeleteTableAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn delete_table_layout(
        &mut self,
        table: &BoundTableInfo,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let name = &table.info.table_name;
            let engine = catalog.engine;
            let (_, targets) =
                read_gsi_readiness(catalog.attempt, engine, table, catalog.protection, None)
                    .await?;
            if targets.len()
                != table
                    .info
                    .global_secondary_indexes
                    .as_deref()
                    .unwrap_or_default()
                    .len()
            {
                return Err(StorageError::internal(
                    "SQL GSI deletion has no build incarnation",
                ));
            }
            let mut statements = Vec::new();
            let name_value = name.to_string();
            statements.push(SqlStatement::new(
                format!(
                    "DELETE FROM {relation_sys_gsi_builds} WHERE table_name = {}",
                    engine.engine().placeholder(1),
                    relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
                ),
                vec![SqlValue::Text(name_value.clone())],
            ));
            statements.push(SqlStatement::new(
                format!(
                    "DELETE FROM {relation_sys_gsi_name_locks} WHERE table_id = {}",
                    engine.engine().placeholder(1),
                    relation_sys_gsi_name_locks = engine.relation_name("sys_gsi_name_locks"),
                ),
                vec![SqlValue::Text(
                    uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string(),
                )],
            ));
            let scope = table_retention_scope_id(table.history_id);
            statements.push(SqlStatement::new(
                format!(
                    "DELETE FROM {relation_sys_stream_trim_state} WHERE scope_id = {}",
                    engine.engine().placeholder(1),
                    relation_sys_stream_trim_state = engine.relation_name("sys_stream_trim_state"),
                ),
                vec![SqlValue::Text(scope)],
            ));
            let history = uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string();
            let locked_until = name_locked_until(captured_at);
            statements.push(SqlStatement::new(
                format!(
                    "UPDATE {relation_tables} SET locked_until = {}, table_status = 'DELETING', \
                     stream_enabled = FALSE WHERE table_name = {} AND id = {} AND locked_until IS \
                     NULL",
                    engine.engine().placeholder(1),
                    engine.engine().placeholder(2),
                    engine.engine().placeholder(3),
                    relation_tables = engine.relation_name("tables"),
                ),
                vec![
                    SqlValue::Integer(locked_until.timestamp_millis()),
                    SqlValue::Text(name_value.clone()),
                    SqlValue::Text(history.clone()),
                ],
            ));
            let target = TableReclaimTarget {
                history_id: table.history_id,
                table_name: name.clone(),
                name_locked_until: locked_until,
                phase: TableReclaimPhase::MetadataCleanup,
                gsi_targets: targets,
            };
            statements.push(SqlStatement::new(
                format!(
                    "INSERT INTO {relation_sys_table_reclaim} (history_id, ready_at, payload) \
                     VALUES ({}, {}, {})",
                    engine.engine().placeholder(1),
                    engine.engine().placeholder(2),
                    engine.engine().placeholder(3),
                    relation_sys_table_reclaim = engine.relation_name("sys_table_reclaim"),
                ),
                vec![
                    SqlValue::Text(history),
                    SqlValue::Integer(i64::MIN),
                    SqlValue::Blob(storage_serde::to_bytes(&target)?),
                ],
            ));
            let expected_count = statements.len();
            let counts = catalog.attempt.execute_batch(statements).await?;
            if counts.len() != expected_count
                || counts[expected_count - 2] != 1
                || counts.last() != Some(&1)
            {
                return Err(StorageError::internal(
                    "SQL table incarnation changed during deletion",
                ));
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
