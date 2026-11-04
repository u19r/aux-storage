//! Attempt-scoped stream capability shared by every Dynamo write verb.

use std::future::Future;

use storage_backfill::GsiProjectionWriteBatch;
use storage_types::{
    BoundTableInfo, ItemHistoryEntry, ItemHistoryReference, StorageResult, SystemStreamPosition,
    SystemStreamScope,
};
use stream_provider::{DynamoStreamWriteInput, DynamoStreamWritePlan, LogicalSystemStreamMutation};

use crate::ItemPrimaryEffects;

/// Prepare stream effects on the operation's native attempt. `stage_*` methods
/// only append local effects and cannot await database work. The family flush
/// submits the complete batch before the native attempt commits; KV registers
/// mutations synchronously, while SQL executes its statement batch.
/// The shared executor chooses the routing key and logical record, while the
/// family lowerer protects the sequence and encodes physical rows.
pub trait DynamoStreamWriteAttempt:
    stream_provider::ItemHistoryReadAttempt + crate::TableHistoryRetentionAttempt + Sized
{
    /// Exact history effects join the primary/GSI/TTL batch.
    fn stage_item_history(
        &mut self,
        entry: Option<ItemHistoryEntry>,
        delete: Option<ItemHistoryReference>,
    ) -> StorageResult<()>;

    /// Prepare image dependencies and projections without inventing a stream
    /// event.
    fn prepare_gsi_writes(
        &mut self,
        table: &BoundTableInfo,
        effects: &ItemPrimaryEffects,
        writes: GsiProjectionWriteBatch,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn reserve_stream_position(
        &mut self,
        routing_key: &[u8],
    ) -> impl Future<Output = StorageResult<(SystemStreamScope, SystemStreamPosition)>> + Send;

    fn stage_stream_write(&mut self, plan: DynamoStreamWritePlan) -> StorageResult<()>;

    /// Submit the prepared physical effects after all protected reads finish.
    fn flush_dynamo_write(&mut self) -> impl Future<Output = StorageResult<()>> + Send;

    fn prepare_logical_stream_write(
        &mut self,
        table: &BoundTableInfo,
        mutation: LogicalSystemStreamMutation,
        publication: (SystemStreamScope, SystemStreamPosition),
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            let mut settlement =
                infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
            let attempt = settlement.as_mut();
            let result = async {
                let plan = DynamoStreamWritePlan::try_from(DynamoStreamWriteInput {
                    table,
                    mutation,
                    source_scope: publication.0,
                    position: publication.1,
                })?;
                attempt.stage_stream_write(plan)
            }
            .await;
            settlement.complete(result)
        }
    }
}
