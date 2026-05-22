use async_trait::async_trait;
use storage_types::StorageError;

use crate::{BackfillBatchOutcome, BackfillState, GsiBackfillDescriptor};

#[async_trait]
pub trait BackfillDriver: Send + Sync {
    /// Enumerate all registered backfill descriptors alongside their current
    /// persisted state. Implementations should filter out descriptors already
    /// marked as `Done` unless additional work (e.g., catch-up) is required.
    async fn enumerate_states(
        &self,
    ) -> Result<Vec<(GsiBackfillDescriptor, BackfillState)>, StorageError>;

    /// Persist state updates for a descriptor.
    async fn persist_state(
        &self,
        descriptor: &GsiBackfillDescriptor,
        state: &BackfillState,
    ) -> Result<(), StorageError>;

    /// Refresh state from storage. Returns `None` when the descriptor has been
    /// removed (for example when TTL is disabled).
    async fn reload_state(
        &self,
        descriptor: &GsiBackfillDescriptor,
    ) -> Result<Option<BackfillState>, StorageError>;

    /// Execute a single batch of work using the provided state. Implementations
    /// may internally adjust the batch size to respect backend constraints.
    async fn execute_batch(
        &self,
        descriptor: &GsiBackfillDescriptor,
        state: &BackfillState,
        batch_size: usize,
    ) -> Result<BackfillBatchOutcome, StorageError>;
}
