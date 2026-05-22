use async_trait::async_trait;
use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncApply, SyncMutationResponse,
};
use storage_types::StorageResult;

use crate::DatabaseManager;

#[async_trait]
impl SyncApply for DatabaseManager {
    async fn apply_resolved_sync_mutations(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<SyncMutationResponse>> {
        if batch.mutations.iter().all(is_item_sync_mutation) {
            return self
                .storage_provider()
                .apply_resolved_sync_mutations(metadata, batch)
                .await;
        }

        let mut responses = Vec::with_capacity(batch.mutations.len());
        let mut item_mutations = Vec::new();
        for mutation in batch.mutations {
            match mutation {
                ResolvedSyncMutation::Put(_) | ResolvedSyncMutation::Delete(_) => {
                    item_mutations.push(mutation);
                }
                lifecycle => {
                    if !item_mutations.is_empty() {
                        responses.extend(
                            self.storage_provider()
                                .apply_resolved_sync_mutations(
                                    metadata.clone(),
                                    ResolvedSyncMutationBatch::new(std::mem::take(
                                        &mut item_mutations,
                                    )),
                                )
                                .await?,
                        );
                    }
                    responses.push(self.apply_lifecycle_sync_mutation(lifecycle).await?);
                }
            }
        }
        if !item_mutations.is_empty() {
            responses.extend(
                self.storage_provider()
                    .apply_resolved_sync_mutations(
                        metadata,
                        ResolvedSyncMutationBatch::new(item_mutations),
                    )
                    .await?,
            );
        }
        Ok(responses)
    }
}

fn is_item_sync_mutation(mutation: &ResolvedSyncMutation) -> bool {
    matches!(
        mutation,
        ResolvedSyncMutation::Put(_) | ResolvedSyncMutation::Delete(_)
    )
}
