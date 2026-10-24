//! KV binding of the shared attempt-scoped Dynamo stream capability.

use storage_backfill::GsiProjectionWriteBatch;
use storage_provider::{
    DynamoStreamWriteAttempt, GsiWriteSource, ItemPrimaryEffects, StreamTrimStateWrite,
};
use storage_types::{BoundTableInfo, StorageResult, SystemStreamPosition};
use stream_provider::DynamoStreamWritePlan;

use crate::{
    KvAttempt,
    catalog::KvCatalogAttempt,
    gsi::{GsiWriteInput, prepare_gsi_writes},
    retention, stream_write,
};

impl<A: KvAttempt> DynamoStreamWriteAttempt for KvCatalogAttempt<'_, A> {
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
            let identity = catalog.identity_for(&table.info.table_name)?.clone();
            prepare_gsi_writes(
                catalog,
                GsiWriteInput {
                    identity: &identity,
                    table,
                    source: GsiWriteSource::Item(effects),
                    writes,
                    prepared: &mut prepared.gsi,
                },
            )
            .await?;
            catalog.prepared_writes = Ok(prepared);
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
            catalog
                .dynamo_history
                .as_mut()
                .ok_or_else(|| {
                    storage_types::StorageError::internal("KV stream history was not prepared")
                })?
                .reserve(&*catalog.attempt, routing_key)
                .await
        }
        .await;
        settlement.complete(result)
    }

    fn stage_stream_write(&mut self, plan: DynamoStreamWritePlan) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            if plan.record.position.offset().versionstamp().is_some() {
                return stream_write::stage_commit_ordered_dynamo(catalog.attempt, plan);
            }
            catalog.append_dynamo_mutations(stream_write::dynamo_stream_mutations(plan)?)?;
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
            let identity = catalog.identity_for(&table.info.table_name)?.clone();
            catalog.append_dynamo_mutations(retention::stream_retention_mutations(
                &identity, write,
            )?)?;
            Ok(())
        })();
        settlement.complete(result)
    }

    async fn flush_dynamo_write(&mut self) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = std::future::ready(catalog.flush_dynamo_mutations()).await;
        settlement.complete(result)
    }
}
