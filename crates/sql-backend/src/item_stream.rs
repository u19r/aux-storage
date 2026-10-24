//! SQL binding of the shared attempt-scoped Dynamo stream capability.

use storage_backfill::GsiProjectionWriteBatch;
use storage_provider::{
    DynamoStreamWriteAttempt, GsiWriteSource, ItemPrimaryEffects, StreamTrimStateWrite,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult, SystemStreamPosition};
use stream_provider::DynamoStreamWritePlan;

use crate::{
    SqlAttempt, SqlLayout,
    catalog::{SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    gsi::{GsiWriteInput, prepare_gsi_writes},
    retention, stream_write,
};

impl<A: SqlAttempt, L: SqlLayout> DynamoStreamWriteAttempt for SqlCatalogAttempt<'_, A, L> {
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
                catalog,
                GsiWriteInput {
                    table,
                    source: GsiWriteSource::Item(effects),
                    writes,
                    targets: &targets,
                    prepared: &mut prepared.gsi,
                },
            )
            .await?;
            catalog.prepared_writes = Ok(prepared);
            catalog.append_dynamo_statements(
                SqlDynamoWriteKind::Gsi,
                statements
                    .into_iter()
                    .map(|statement| (statement, SqlDynamoCount::ExactlyOne)),
            )?;
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_gsi_and_stream(
        &mut self,
        table: &BoundTableInfo,
        effects: &ItemPrimaryEffects,
        writes: GsiProjectionWriteBatch,
        routing_key: &[u8],
    ) -> StorageResult<SystemStreamPosition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog.prepare_gsi_writes(table, effects, writes).await?;
            catalog.reserve_stream_position(routing_key)
        }
        .await;
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

    fn stage_item_stream_retention(
        &mut self,
        table: &BoundTableInfo,
        write: StreamTrimStateWrite,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            if write.state.scope.table_name != table.info.table_name {
                return Err(StorageError::internal(
                    "SQL item stream retention has another table",
                ));
            }
            let statements = retention::stream_retention_statements(catalog.engine, write)?;
            catalog.append_dynamo_statements(
                SqlDynamoWriteKind::Retention,
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
