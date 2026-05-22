use async_trait::async_trait;
use storage_types::{StorageError, StorageResult};

use crate::{
    ResolvedSyncMutationBatch, SyncCommitMetadata, SyncMutationResponse, SyncProposalBatch,
    SyncProposalId, SyncProposalResponse,
};

#[async_trait]
pub trait SyncMutationResolver: Send + Sync {
    type Request: Send + Sync;

    async fn resolve_sync_mutation(
        &self,
        request: Self::Request,
    ) -> StorageResult<SyncProposalBatch>;
}

#[async_trait]
pub trait SyncApply: Send + Sync {
    async fn apply_resolved_sync_mutations(
        &self,
        metadata: SyncCommitMetadata,
        batch: ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<SyncMutationResponse>>;

    fn reject_ordinary_write_apply() -> StorageError {
        StorageError::internal(
            "sync apply must use resolved mutations and must not call ordinary provider write \
             paths",
        )
    }
}

#[async_trait]
pub trait SyncCommandDedupeStore: Send + Sync {
    async fn load_sync_command_response(
        &self,
        proposal_id: &SyncProposalId,
    ) -> StorageResult<Option<SyncProposalResponse>>;

    async fn save_sync_command_response(
        &self,
        response: &SyncProposalResponse,
    ) -> StorageResult<()>;
}
