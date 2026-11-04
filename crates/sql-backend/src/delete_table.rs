//! SQL table deletion under the catalog operation's native attempt.

use storage_provider::{
    DeleteTableAttempt, TableReclaimPhase, TableReclaimTarget, table_retention_scope_id,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult, storage_serde};

use crate::{
    SqlAttempt, SqlLayout, SqlStatement, SqlValue, catalog::SqlCatalogAttempt,
    gsi::read_gsi_readiness,
};

impl<A: SqlAttempt, L: SqlLayout> DeleteTableAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_external_cleanup_requirement(
        &mut self,
        table: &BoundTableInfo,
    ) -> StorageResult<bool> {
        let page = self
            .attempt
            .query(
                crate::table_reclaim_sql_statements::load_external_cleanup_requirement(
                    self.engine,
                    table.history_id,
                ),
                crate::ReadProtection::Conflict,
                storage_provider::CATALOG_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() != 1 {
            return Err(StorageError::internal(
                "SQL cleanup requirement has no unique catalog row",
            ));
        }
        match page.rows[0].value(crate::ColumnIndex::new(0))? {
            SqlValue::Integer(0) => Ok(false),
            SqlValue::Integer(1) => Ok(true),
            _ => Err(StorageError::internal(
                "Invalid SQL external cleanup requirement",
            )),
        }
    }

    async fn delete_table_layout(
        &mut self,
        table: &BoundTableInfo,
        phase: TableReclaimPhase,
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
                    "DELETE FROM {relation_sys_gsi_builds} WHERE table_id = {}",
                    engine.engine().placeholder(1),
                    relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
                ),
                vec![SqlValue::Blob(table.history_id.as_ref().to_vec())],
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
            statements.push(SqlStatement::new(
                format!(
                    "UPDATE {relation_tables} SET table_status = 'DELETING', stream_enabled = \
                     FALSE WHERE table_name = {} AND id = {} AND table_status <> 'DELETING'",
                    engine.engine().placeholder(1),
                    engine.engine().placeholder(2),
                    relation_tables = engine.relation_name("tables"),
                ),
                vec![
                    SqlValue::Text(name_value.clone()),
                    SqlValue::Text(history.clone()),
                ],
            ));
            let target = TableReclaimTarget {
                history_id: table.history_id,
                table_name: name.clone(),
                phase,
                gsi_targets: targets,
            };
            statements.push(SqlStatement::new(
                format!(
                    "INSERT INTO {relation_sys_table_reclaim} (history_id, payload) VALUES ({}, \
                     {})",
                    engine.engine().placeholder(1),
                    engine.engine().placeholder(2),
                    relation_sys_table_reclaim = engine.relation_name("sys_table_reclaim"),
                ),
                vec![
                    SqlValue::Text(history),
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
