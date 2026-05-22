use async_trait::async_trait;
use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillImport, LogicalBackfillManifest, LogicalBackfillResult,
};
use storage_types::StorageResult;

use crate::{ResolvedSyncMutationBatch, SyncApply, SyncCommitMetadata, SyncMutationResponse};

pub(crate) struct ResolvedOnlyApplyAdapter;

#[async_trait]
impl SyncApply for ResolvedOnlyApplyAdapter {
    async fn apply_resolved_sync_mutations(
        &self,
        _metadata: SyncCommitMetadata,
        batch: ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<SyncMutationResponse>> {
        Ok(batch
            .mutations
            .into_iter()
            .map(|mutation| SyncMutationResponse {
                response_json: Some(mutation.mutation_id().as_str().to_string()),
            })
            .collect())
    }
}

#[async_trait]
impl LogicalBackfillImport for ResolvedOnlyApplyAdapter {
    async fn import_logical_chunk(
        &self,
        _manifest: &LogicalBackfillManifest,
        _chunk: LogicalBackfillChunk,
    ) -> StorageResult<LogicalBackfillResult> {
        Ok(LogicalBackfillResult::ChunkImported)
    }
}
