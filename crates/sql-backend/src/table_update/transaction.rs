//! UpdateTable policy stays in storage-provider; this module persists its SQL
//! decision.
use std::sync::Arc;

use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, StreamTrimScope, TableUpdateChange, TableUpdateRetention,
    TableUpdateTransaction, table_retention_scope_id,
};
use storage_types::{
    BoundTableInfo, IndexName, StorageError, StorageResult, StreamBinding, TableBinding, TableName,
    TimestampMillis,
};

use super::{layout, sql_statements};
use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlValue,
    catalog::SqlCatalogAttempt,
    retention::{load_stream_trim_states, write_stream_retention},
};

impl<A: SqlAttempt, L: SqlLayout> TableUpdateTransaction for SqlCatalogAttempt<'_, A, L> {
    async fn load_stream_source(&mut self) -> StorageResult<stream_provider::SystemStreamHistory> {
        stream_provider::StreamAttempt::read_system_page_start_snapshot(self).await
    }

    async fn observe_table_update(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<BoundTableInfo>> {
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
        table: &BoundTableInfo,
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
        table: &BoundTableInfo,
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
        current: BoundTableInfo,
        change: TableUpdateChange,
    ) -> StorageResult<BoundTableInfo> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .take_exclusive_catalog_lock(&current.info.table_name)
                .await?;
            let (plan, retention, name_locked_until) = change.into_parts();
            let targets = storage_provider::gsi_build_targets_from_catalog(&current)?;
            // SQL keeps a native index per GSI; reclaim drops it.
            if let Some(retirement) = storage_provider::deleted_index_retirement(
                &current,
                &plan,
                <Self as storage_provider::maintenance::GsiPort>::NATIVE_INDEX,
            )? {
                storage_provider::register_index_retirement(
                    catalog,
                    current.history_id,
                    retirement.target,
                    retirement.native_index,
                )
                .await?;
            }
            let layout::LayoutEffects {
                binding: gsi_binding,
                alters,
            } = layout::write_layout(
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
            // Without a GSI set change the stored GSI binding stays: a caller
            // can hold a table observed before a phase change of this attempt.
            let binding = TableBinding {
                stream: StreamBinding::for_info(&plan.next_table),
                ..gsi_binding.clone().unwrap_or(current.binding)
            };
            let next = BoundTableInfo {
                history_id: current.history_id,
                info: Arc::new(plan.next_table),
                binding,
            };
            let changed = catalog
                .execute_catalog_change(
                    &next.info.table_name,
                    vec![sql_statements::save_catalog(
                        catalog.engine,
                        &next,
                        gsi_binding.is_some(),
                    )?],
                )
                .await?;
            if changed != [1] {
                return Err(StorageError::internal(
                    "SQL table update lost its catalog incarnation",
                ));
            }
            catalog.stage_layout_ddl(alters)?;
            Ok(next)
        }
        .await;
        settlement.complete(result)
    }
}
