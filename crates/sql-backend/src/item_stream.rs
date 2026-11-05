//! SQL binding of the shared attempt-scoped Dynamo stream capability.

use storage_backfill::GsiProjectionWriteBatch;
use storage_provider::{DynamoStreamWriteAttempt, GsiWriteSource, ItemPrimaryEffects};
use storage_types::{BoundTableInfo, StorageResult, SystemStreamPosition, SystemStreamScope};
use stream_provider::DynamoStreamWritePlan;

use crate::{
    SqlAttempt, SqlLayout,
    catalog::{SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    gsi::{GsiWriteInput, prepare_gsi_writes},
    stream_write,
};

impl<A: SqlAttempt, L: SqlLayout> storage_provider::TableHistoryRetentionAttempt
    for SqlCatalogAttempt<'_, A, L>
{
    async fn load_table_history_retention(
        &mut self,
        table: &BoundTableInfo,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<Option<storage_provider::StreamTrimState>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let scope = storage_provider::StreamTrimScope::table(
            storage_provider::table_retention_scope_id(table.history_id),
            table.table_name.clone(),
        );
        let result = crate::retention::load_stream_trim_states(
            catalog.attempt,
            catalog.engine,
            &[scope],
            protection,
        )
        .await
        .map(|states| states.into_iter().next().flatten());
        settlement.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> DynamoStreamWriteAttempt for SqlCatalogAttempt<'_, A, L> {
    fn stage_item_history(
        &mut self,
        entry: Option<storage_types::ItemHistoryEntry>,
        delete: Option<storage_types::ItemHistoryReference>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            let mut statements = Vec::with_capacity(2);
            if let Some(entry) = entry {
                statements.push((
                    stream_write::sql_statements::item(
                        catalog.engine,
                        stream_provider::StreamWriteKind::Item(entry.try_into()?),
                    )?,
                    SqlDynamoCount::ExactlyOne,
                ));
            }
            if let Some(delete) = delete {
                statements.push((
                    stream_write::sql_statements::delete_history_row(
                        catalog.engine,
                        delete.address().physical_stream_name()?,
                        delete.version().into(),
                    ),
                    SqlDynamoCount::ExactlyOne,
                ));
            }
            catalog.append_dynamo_statements(SqlDynamoWriteKind::Stream, statements)
        })();
        settlement.complete(result)
    }

    async fn prepare_gsi_writes(
        &mut self,
        table: &BoundTableInfo,
        effects: &ItemPrimaryEffects,
        writes: GsiProjectionWriteBatch,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut prepared =
                std::mem::replace(&mut catalog.prepared_writes, Ok(Default::default()))?;
            let targets = catalog.gsi_targets().to_vec();
            let statements = prepare_gsi_writes(
                catalog.engine,
                GsiWriteInput {
                    table,
                    source: GsiWriteSource::Item(effects),
                    writes,
                    targets: &targets,
                    prepared: &mut prepared.gsi,
                },
            )?;
            catalog.prepared_writes = Ok(prepared);
            for statement in statements {
                catalog.stage_gsi_upsert(statement)?;
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn reserve_stream_position(
        &mut self,
        routing_key: &[u8],
    ) -> StorageResult<(SystemStreamScope, SystemStreamPosition)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { catalog.reserve_stream_position(routing_key) }.await;
        settlement.complete(result)
    }

    fn stage_stream_write(&mut self, plan: DynamoStreamWritePlan) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            let statements = stream_write::dynamo_stream_statements(catalog.engine, plan)?;
            catalog.append_dynamo_statements(
                SqlDynamoWriteKind::Stream,
                statements
                    .into_iter()
                    .map(|statement| (statement, SqlDynamoCount::ExactlyOne)),
            )?;
            Ok(())
        })();
        settlement.complete(result)
    }

    async fn flush_dynamo_write(&mut self) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = catalog.flush_dynamo_statements().await;
        settlement.complete(result)
    }
}
