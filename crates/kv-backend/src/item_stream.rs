//! KV binding of the shared attempt-scoped Dynamo stream capability.

use storage_backfill::GsiProjectionWriteBatch;
use storage_provider::DynamoStreamWriteAttempt;
use storage_types::{BoundTableInfo, StorageResult};
use stream_provider::{PositionedStreamEntry, StreamEntryInput};

use crate::{
    KvAttempt,
    catalog::KvCatalogAttempt,
    gsi::{GsiWriteInput, prepare_gsi_writes},
    stream_write,
};

impl<A: KvAttempt> DynamoStreamWriteAttempt for KvCatalogAttempt<'_, A> {
    async fn prepare_gsi_writes(
        &mut self,
        table: &BoundTableInfo,
        writes: GsiProjectionWriteBatch,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            let mut prepared =
                std::mem::replace(&mut catalog.prepared_writes, Ok(Default::default()))?;
            prepare_gsi_writes(
                catalog,
                GsiWriteInput {
                    table,
                    writes,
                    prepared: &mut prepared.gsi,
                },
            )?;
            catalog.prepared_writes = Ok(prepared);
            Ok(())
        })();
        settlement.complete(result)
    }

    async fn prepare_logical_stream_write(
        &mut self,
        table: &BoundTableInfo,
        entry: StreamEntryInput<'_>,
        routing_key: &[u8],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let root = catalog.attempt.root_attempt();
            let routes = root.writer_routes().ok_or_else(|| {
                storage_types::StorageError::internal("KV writer routes were not prepared")
            })?;
            let ordinal = root.reserve_system_event_number()?;
            let lineage = routes.writer_lineage(routing_key)?;
            let position = if matches!(
                root.backend_engine(),
                storage_common::diagnostics::NativeEngine::FoundationDb
            ) {
                lineage.position(format!("{:020}-vsffff{ordinal:04x}", u64::MAX))
            } else {
                let sequence = root.native_snapshot_sequence().await?.ok_or_else(|| {
                    storage_types::StorageError::from(
                        storage_types::StorageEnum::HistoryUnavailable,
                    )
                })?;
                let offset = sequence
                    .checked_add(1)
                    .and_then(|next| next.checked_add(u64::from(ordinal)))
                    .ok_or_else(|| {
                        storage_types::StorageError::from(
                            storage_types::StorageEnum::HistoryUnavailable,
                        )
                    })?;
                lineage.position(format!("{offset:020}-ns"))
            }
            .map_err(|_| {
                storage_types::StorageError::from(storage_types::StorageEnum::HistoryUnavailable)
            })?;
            let value = PositionedStreamEntry::encode_input(
                &table.table_name,
                routes.source_scope(),
                &entry,
            )?;
            if position.offset().versionstamp().is_some() {
                return stream_write::stage_commit_ordered_dynamo(
                    catalog.attempt,
                    (table.history_id, &position),
                    entry.created_at(),
                    value,
                );
            }
            catalog.append_dynamo_mutations(stream_write::dynamo_stream_mutations(
                (table.history_id, &position),
                entry.created_at(),
                value,
            )?)?;
            Ok(())
        }
        .await;
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
