//! SQL binding of the shared attempt-scoped Dynamo stream capability.

use storage_backfill::GsiProjectionWriteBatch;
use storage_provider::DynamoStreamWriteAttempt;
use storage_types::{BoundTableInfo, StorageError, StorageResult};
use stream_provider::StreamEntryInput;

use crate::{
    SqlAttempt, SqlLayout,
    catalog::{DynamoStreamPositions, SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    stream_write,
};

impl<A: SqlAttempt, L: SqlLayout> DynamoStreamWriteAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn prepare_gsi_writes(
        &mut self,
        _table: &BoundTableInfo,
        _writes: GsiProjectionWriteBatch,
    ) -> StorageResult<()> {
        Ok(())
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
        let result = (|| {
            let history = catalog
                .dynamo_history
                .as_mut()
                .ok_or_else(|| StorageError::internal("SQL stream history was not prepared"))?;
            let scope = history.as_ref().source_scope();
            match &mut catalog.dynamo_positions {
                DynamoStreamPositions::Native(allocation, transaction_offset) => {
                    if catalog.catalog_lock.xid_before_ordering_lock() {
                        return Err(StorageError::internal(
                            "Postgres transaction ID was assigned before the item lock",
                        ));
                    }
                    let lineage = history.as_ref().writer_lineage(routing_key)?;
                    let event = (allocation.reserve()?, *transaction_offset);
                    catalog.dynamo_writes.stage(
                        || {
                            Ok((
                                stream_write::sql_statements::native_entry(
                                    catalog.engine,
                                    table,
                                    lineage,
                                    (&entry, scope),
                                    event,
                                )?,
                                SqlDynamoCount::ExactlyOne,
                                SqlDynamoWriteKind::Stream,
                            ))
                        },
                        || {
                            stream_write::sql_statements::counted_native_entry(
                                catalog.engine,
                                table,
                                lineage,
                                (&entry, scope),
                                event,
                            )
                        },
                    )?;
                }
                DynamoStreamPositions::Sequential(positions) => {
                    let scope = scope.clone();
                    let allocation = history.reserve_write(routing_key)?;
                    let shard = allocation.position.shard_id();
                    if let Some((_, _, next)) =
                        positions.iter_mut().find(|(known, _, _)| known == shard)
                    {
                        *next = allocation.next_offset;
                    } else {
                        positions.push((
                            shard.clone(),
                            allocation.expected_offset,
                            allocation.next_offset,
                        ));
                    }
                    catalog.dynamo_writes.stage(
                        || {
                            Ok((
                                stream_write::sql_statements::entry(
                                    catalog.engine,
                                    table,
                                    &allocation.position,
                                    (&entry, &scope),
                                )?,
                                SqlDynamoCount::ExactlyOne,
                                SqlDynamoWriteKind::Stream,
                            ))
                        },
                        || {
                            stream_write::sql_statements::counted_entry(
                                catalog.engine,
                                table,
                                &allocation.position,
                                (&entry, &scope),
                            )
                        },
                    )?;
                }
            }
            Ok(())
        })();
        settlement.complete(result)
    }

    async fn finish_dynamo_write(&mut self) -> StorageResult<()> {
        self.finish_dynamo_statements().await
    }

    async fn flush_dynamo_write(&mut self) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = catalog.flush_dynamo_statements().await;
        settlement.complete(result)
    }
}
