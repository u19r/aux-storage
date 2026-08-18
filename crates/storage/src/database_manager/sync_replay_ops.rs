use storage_sync::{SyncApply, SyncMutationResolver, SyncWriteProposalRequest, SyncWriteRequest};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::database_manager::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID};

impl DatabaseManager {
    pub async fn last_resolved_sync_log_id(
        &self,
    ) -> StorageResult<Option<storage_sync::SyncLogId>> {
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, |provider| async move {
            let database_call = metrics_facade::begin_database_call("last_resolved_sync_log_id");
            let result = provider.last_resolved_sync_log_id().await;
            drop(database_call);
            result
        })
        .await
    }

    pub async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, move |provider| async move {
            let database_call = metrics_facade::begin_database_call("get_resolved_sync_log_entry");
            let result = provider.get_resolved_sync_log_entry(log_id).await;
            drop(database_call);
            result
        })
        .await
    }

    pub async fn replay_resolved_sync_log_entries(&self, limit: usize) -> StorageResult<usize> {
        let last_applied = self.last_resolved_sync_log_id().await?;
        let entries = self
            .run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, move |provider| async move {
                let database_call =
                    metrics_facade::begin_database_call("resolved_sync_log_entries_after");
                let result = provider
                    .resolved_sync_log_entries_after(last_applied, limit)
                    .await;
                drop(database_call);
                result
            })
            .await?;
        let mut applied = 0;
        for entry in entries {
            self.apply_resolved_sync_mutations(entry.metadata, entry.batch)
                .await?;
            applied += 1;
        }
        Ok(applied)
    }

    pub async fn run_single_node_sync_write(
        &self,
        request: SyncWriteProposalRequest,
    ) -> StorageResult<storage_sync::SyncProposalResponse> {
        let proposal = self.resolve_sync_mutation(request).await?;
        let next_index = self
            .last_resolved_sync_log_id()
            .await?
            .map(|log_id| log_id.index.saturating_add(1))
            .unwrap_or(1);
        let metadata = storage_sync::SyncCommitMetadata {
            log_id: storage_sync::SyncLogId::new(1, next_index),
            committed_at: TimestampMillis::now(),
            leader_node_id: "single-node".to_string(),
        };
        let metadata_ref = &metadata;
        let batch_ref = &proposal.batch;
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, move |provider| async move {
            let database_call =
                metrics_facade::begin_database_call("persist_resolved_sync_log_entry");
            let result = provider
                .persist_resolved_sync_log_entry(metadata_ref, batch_ref)
                .await;
            drop(database_call);
            result
        })
        .await?;
        let responses = self
            .apply_resolved_sync_mutations(metadata, proposal.batch)
            .await?;
        Ok(storage_sync::SyncProposalResponse::new(
            proposal.proposal_id,
            responses,
        ))
    }

    pub(super) async fn run_single_node_sync_write_request(
        &self,
        operation_name: &str,
        request: SyncWriteRequest,
    ) -> StorageResult<storage_sync::SyncProposalResponse> {
        let next_index = self
            .last_resolved_sync_log_id()
            .await?
            .map(|log_id| log_id.index.saturating_add(1))
            .unwrap_or(1);
        let proposal_id =
            storage_sync::SyncProposalId::new(format!("{operation_name}#{next_index}"))
                .map_err(|error| StorageError::validation(error.to_string()))?;
        self.run_single_node_sync_write(SyncWriteProposalRequest::new(proposal_id, request))
            .await
    }
}
