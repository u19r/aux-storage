//! Attempt-scoped stream capability shared by every Dynamo write verb.

use std::future::Future;

use storage_backfill::GsiProjectionWriteBatch;
use storage_types::{BoundTableInfo, StorageResult, SystemStreamPosition};
use stream_provider::{DynamoStreamWriteInput, DynamoStreamWritePlan, LogicalSystemStreamMutation};

use crate::{ItemPrimaryEffects, StreamTrimStateWrite};

/// Prepare stream effects on the operation's native attempt. `stage_*` methods
/// only append local effects and cannot await database work. The family flush
/// submits the complete batch before the native attempt commits; KV registers
/// mutations synchronously, while SQL executes its statement batch.
/// The shared executor chooses the routing key and logical record, while the
/// family lowerer protects the sequence and encodes physical rows.
pub trait DynamoStreamWriteAttempt:
    stream_provider::retained_image::RetainedOwnershipAttempt + Sized
{
    /// Prepare image dependencies and projections without inventing a stream
    /// event.
    fn prepare_gsi_writes(
        &mut self,
        table: &BoundTableInfo,
        effects: &ItemPrimaryEffects,
        writes: GsiProjectionWriteBatch,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn prepare_gsi_and_stream(
        &mut self,
        table: &BoundTableInfo,
        effects: &ItemPrimaryEffects,
        writes: GsiProjectionWriteBatch,
        routing_key: &[u8],
    ) -> impl Future<Output = StorageResult<SystemStreamPosition>> + Send;

    fn stage_stream_write(&mut self, plan: DynamoStreamWritePlan) -> StorageResult<()>;

    fn stage_item_stream_retention(
        &mut self,
        table: &BoundTableInfo,
        write: StreamTrimStateWrite,
    ) -> StorageResult<()>;

    /// Submit the prepared physical effects after all protected reads finish.
    fn flush_dynamo_write(&mut self) -> impl Future<Output = StorageResult<()>> + Send;

    fn prepare_logical_stream_write(
        &mut self,
        table: &BoundTableInfo,
        mutation: LogicalSystemStreamMutation,
        position: SystemStreamPosition,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            let mut settlement =
                infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
            let attempt = settlement.as_mut();
            let result = async {
                let plan = DynamoStreamWritePlan::prepare(
                    attempt,
                    DynamoStreamWriteInput {
                        table,
                        mutation,
                        position,
                    },
                )
                .await?;
                attempt.stage_stream_write(plan)
            }
            .await;
            settlement.complete(result)
        }
    }
}
