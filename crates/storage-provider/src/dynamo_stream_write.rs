//! Attempt-scoped stream capability shared by every Dynamo write verb.

use std::future::Future;

use storage_backfill::GsiProjectionWriteBatch;
use storage_types::{BoundTableInfo, StorageResult};
use stream_provider::StreamEntryInput;

/// Prepare stream effects on the operation's native attempt. `stage_*` methods
/// only append local effects and cannot await database work. The family flush
/// submits the complete batch before the native attempt commits; KV registers
/// mutations synchronously, while SQL executes its statement batch.
/// The shared executor chooses the routing key and logical record, while the
/// family lowerer protects the sequence and encodes physical rows.
pub trait DynamoStreamWriteAttempt:
    stream_provider::StreamAttempt + crate::maintenance::StreamRetentionPort + Sized
{
    /// Prepare image dependencies and projections without inventing a stream
    /// event.
    fn prepare_gsi_writes(
        &mut self,
        table: &BoundTableInfo,
        writes: GsiProjectionWriteBatch,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Submit the prepared physical effects after all protected reads finish.
    fn flush_dynamo_write(&mut self) -> impl Future<Output = StorageResult<()>> + Send;

    /// Complete the operation after all effects and the response are prepared.
    fn finish_dynamo_write(&mut self) -> impl Future<Output = StorageResult<()>> + Send {
        self.flush_dynamo_write()
    }

    fn prepare_logical_stream_write(
        &mut self,
        table: &BoundTableInfo,
        entry: StreamEntryInput<'_>,
        routing_key: &[u8],
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
