//! One KV retention repository over the same native attempt used by streams.

use std::future::Future;

use storage_provider::{
    StreamTrimDueMarker, StreamTrimScope, StreamTrimState,
    maintenance::{
        PreparedRetentionFinish, RetentionStateWrite, StreamRetentionObservation,
        StreamRetentionPort,
    },
};
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use super::{read, write};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> StreamRetentionPort for KvCatalogAttempt<'_, A> {
    fn load_stream_trim_observations(
        &mut self,
        markers: &[StreamTrimDueMarker],
    ) -> impl Future<Output = StorageResult<storage_types::Exact<StreamRetentionObservation>>> {
        read::load_stream_trim_observations(self.attempt, markers)
    }

    fn delete_stream_trim_markers(
        &mut self,
        finishes: &[PreparedRetentionFinish],
    ) -> impl Future<Output = StorageResult<storage_types::Exact<bool>>> {
        write::delete_stream_trim_markers(self.attempt, finishes)
    }

    fn store_stream_trim_states(
        &mut self,
        states: Vec<RetentionStateWrite>,
    ) -> impl Future<Output = StorageResult<()>> {
        std::future::ready(write::store_stream_trim_states(self.attempt, states))
    }

    fn list_due_stream_trim_markers(
        &mut self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> impl Future<Output = StorageResult<Vec<StreamTrimDueMarker>>> {
        read::list_due_stream_trim_markers(self.attempt, due_before, limit)
    }
    fn load_stream_trim_states(
        &mut self,
        scopes: &[StreamTrimScope],
    ) -> impl Future<Output = StorageResult<Vec<Option<StreamTrimState>>>> {
        read::load_stream_trim_states(self.attempt, scopes, ReadProtection::Snapshot)
    }

    async fn load_table_history_retention(
        &mut self,
        table: &storage_types::BoundTableInfo,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<Option<storage_provider::StreamTrimState>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let scope = storage_provider::StreamTrimScope::table(
            storage_provider::table_retention_scope_id(table.history_id),
            table.table_name.clone(),
        );
        let result =
            crate::retention::load_stream_trim_states(catalog.attempt, &[scope], protection)
                .await
                .map(|states| states.into_iter().next().flatten());
        settlement.complete(result)
    }
}
