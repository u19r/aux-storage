//! KV binding of the shared attempt-scoped Dynamo stream capability.

use storage_backfill::GsiProjectionWriteBatch;
use storage_provider::{DynamoStreamWriteAttempt, GsiWriteSource, ItemPrimaryEffects};
use storage_types::{BoundTableInfo, StorageResult, SystemStreamPosition, SystemStreamScope};
use stream_provider::DynamoStreamWritePlan;

use crate::{
    KvAttempt,
    catalog::KvCatalogAttempt,
    gsi::{GsiWriteInput, prepare_gsi_writes},
    stream_write,
};

impl<A: KvAttempt> storage_provider::TableHistoryRetentionAttempt for KvCatalogAttempt<'_, A> {
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
        let result =
            crate::retention::load_stream_trim_states(catalog.attempt, &[scope], protection)
                .await
                .map(|states| states.into_iter().next().flatten());
        settlement.complete(result)
    }
}

impl<A: KvAttempt> DynamoStreamWriteAttempt for KvCatalogAttempt<'_, A> {
    fn stage_item_history(
        &mut self,
        entry: Option<storage_types::ItemHistoryEntry>,
        delete: Option<storage_types::ItemHistoryReference>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            let mut mutations = Vec::with_capacity(2);
            if let Some(entry) = entry {
                let row = stream_provider::StreamItem::try_from(entry)?;
                mutations.push(crate::KvMutation::Put {
                    key: stream_write::key_layout::item_row_key(&row)?,
                    value: stream_provider::StreamRowValue::from_planned(
                        stream_provider::StreamWriteKind::Item(row),
                    )?
                    .to_bytes()?,
                });
            }
            if let Some(delete) = delete {
                mutations.push(crate::KvMutation::Delete {
                    key: stream_write::key_layout::item_key(
                        &delete.address().physical_stream_name()?,
                        delete.version().into(),
                    )?,
                });
            }
            catalog.append_dynamo_mutations(mutations)
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
        let result = (|| {
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
            )?;
            catalog.prepared_writes = Ok(prepared);
            Ok(())
        })();
        settlement.complete(result)
    }

    async fn reserve_stream_position(
        &mut self,
        routing_key: &[u8],
    ) -> StorageResult<(SystemStreamScope, SystemStreamPosition)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
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
            if plan.record.position().offset().versionstamp().is_some() {
                return stream_write::stage_commit_ordered_dynamo(catalog.attempt, plan);
            }
            catalog.append_dynamo_mutations(stream_write::dynamo_stream_mutations(plan)?)?;
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
