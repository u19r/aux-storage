//! UpdateTable policy stays in storage-provider; this module persists its SQL
//! decision.
use std::sync::Arc;

use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, GsiReadinessSnapshot, StreamTrimScope, TableUpdateChange,
    TableUpdateRetention, TableUpdateTransaction, table_retention_scope_id,
};
use storage_types::{
    BoundTableInfo, IndexName, StorageError, StorageResult, TableName, TimestampMillis,
};

use super::{layout, sql_statements};
use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlValue,
    catalog::SqlCatalogAttempt,
    gsi::read_gsi_readiness,
    retention::{load_stream_trim_states, write_stream_retention},
};

impl<A: SqlAttempt, L: SqlLayout> TableUpdateTransaction for SqlCatalogAttempt<'_, A, L> {
    type Catalog = BoundTableInfo;

    async fn observe_table_update(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<Self::Catalog>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let Some(stamp) = catalog.read_catalog_stamp(name).await? else {
                return Ok(None);
            };
            Ok(Some(
                catalog
                    .read_table_at_stamp(name, stamp)
                    .await?
                    .as_ref()
                    .clone(),
            ))
        }
        .await;
        settlement.complete(result)
    }

    async fn observe_table_retention(
        &mut self,
        table: &Self::Catalog,
    ) -> StorageResult<TableUpdateRetention> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let scope = StreamTrimScope::table(
                table_retention_scope_id(table.history_id),
                table.info.table_name.clone(),
            );
            let state = load_stream_trim_states(
                catalog.attempt,
                catalog.engine,
                std::slice::from_ref(&scope),
                catalog.protection,
            )
            .await?
            .pop()
            .flatten();
            Ok(TableUpdateRetention { scope, state })
        }
        .await;
        settlement.complete(result)
    }

    async fn load_index_name_lock(
        &mut self,
        table: &Self::Catalog,
        name: &IndexName,
    ) -> StorageResult<Option<TimestampMillis>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let page = catalog
                .attempt
                .query(
                    sql_statements::load_index_name_lock(catalog.engine, table, name),
                    ReadProtection::Conflict,
                    CATALOG_READ_BYTES,
                )
                .await?;
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL GSI name reservation is invalid",
                ));
            }
            page.rows
                .first()
                .map(|row| match row.value(ColumnIndex::new(0))? {
                    SqlValue::Integer(value) => Ok(TimestampMillis::from_timestamp(*value)),
                    _ => Err(StorageError::internal(
                        "SQL GSI name reservation has invalid type",
                    )),
                })
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn persist_table_update(
        &mut self,
        current: Self::Catalog,
        change: TableUpdateChange,
    ) -> StorageResult<Self::Catalog> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let (plan, retention, name_locked_until) = change.into_parts();
            let (_, targets) = read_gsi_readiness(
                catalog.attempt,
                catalog.engine,
                &current,
                catalog.protection,
                None,
            )
            .await?;
            if let Some(storage_common::GsiLifecycleAction::Delete(delete)) =
                plan.lifecycle.as_ref()
            {
                let target = targets
                    .iter()
                    .find(|target| {
                        target.table_name == current.table_name
                            && target.index_name == delete.index_name
                    })
                    .ok_or_else(|| {
                        StorageError::internal("SQL deleted index has no physical incarnation")
                    })?;
                storage_provider::register_index_retirement(
                    catalog,
                    current.history_id,
                    target.clone(),
                )
                .await?;
            }
            layout::write_layout(
                catalog.attempt,
                catalog.engine,
                layout::GsiLayoutChange {
                    current: &current,
                    plan: &plan,
                    targets: &targets,
                    name_locked_until,
                },
            )
            .await?;
            if let Some(write) = retention {
                write_stream_retention(catalog.attempt, catalog.engine, write).await?;
            }
            let next = BoundTableInfo {
                history_id: current.history_id,
                info: Arc::new(plan.next_table),
            };
            let changed = catalog
                .attempt
                .execute(sql_statements::save_catalog(catalog.engine, &next)?)
                .await?;
            if changed != 1 {
                return Err(StorageError::internal(
                    "SQL table update lost its catalog incarnation",
                ));
            }
            Ok(next)
        }
        .await;
        settlement.complete(result)
    }

    async fn observe_table_update_readiness(
        &mut self,
        table: &Self::Catalog,
    ) -> StorageResult<GsiReadinessSnapshot> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let (snapshot, targets) = read_gsi_readiness(
                catalog.attempt,
                catalog.engine,
                table,
                catalog.protection,
                None,
            )
            .await?;
            catalog.bind_gsi_targets(targets)?;
            Ok(snapshot)
        }
        .await;
        settlement.complete(result)
    }
}
