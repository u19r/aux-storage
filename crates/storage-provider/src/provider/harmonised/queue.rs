use async_trait::async_trait;
use storage_types::StorageResult;

use super::models::{QueuePage, QueueReadInput, QueueReceipts, QueueWriteInput};

/// Foreground queue writes execute through the provider owner. Queue sends
/// need no pre-transaction derivation: wire shapes arrive validated at the
/// request boundary and batch bounds are proven by `QueueWriteInput`
/// construction, so no separate staging representation is needed.
#[async_trait]
pub trait QueueRepository: Send + Sync {
    async fn execute_queue_writes(&self, input: QueueWriteInput) -> StorageResult<QueueReceipts>;

    async fn fetch_queue_messages(&self, query: QueueReadInput) -> StorageResult<QueuePage>;
}
