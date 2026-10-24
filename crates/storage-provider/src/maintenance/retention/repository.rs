use async_trait::async_trait;
use storage_common::retry::retry_native_conflicts;
use storage_types::{StorageResult, TimestampMillis};
use stream_provider::{SystemStreamTrimPageRequest, SystemStreamTrimPageResult};

use super::{
    StreamRetentionPort, StreamRetentionTransaction, StreamTrimDueMarker, StreamTrimScope,
    StreamTrimScopeBoundaries, StreamTrimState,
};
use crate::{DYNAMO_WRITE_RETRY, DynamoPort};

#[async_trait]
pub trait StreamRetentionRepository: Send + Sync {
    /// Observe policy, reclaim rows, and finish markers in one native attempt.
    async fn trim_stream_marker_wave(
        &self,
        wave: super::StreamRetentionWave,
    ) -> StorageResult<super::StreamDurationTrimStats>;

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>>;

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>>;

    async fn load_stream_trim_boundaries(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>>;
}

#[async_trait]
impl<P: DynamoPort> StreamRetentionRepository for P
where for<'native> P::Attempt<'native>: StreamRetentionPort
{
    async fn trim_stream_marker_wave(
        &self,
        wave: super::StreamRetentionWave,
    ) -> StorageResult<super::StreamDurationTrimStats> {
        self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { wave.execute(attempt).await })
        })
        .await
    }
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        self.with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(StreamRetentionPort::list_due_stream_trim_markers(
                attempt, due_before, limit,
            ))
        })
        .await
    }
    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        let scopes = scopes.to_vec();
        self.with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(
                async move { StreamRetentionPort::load_stream_trim_states(attempt, &scopes).await },
            )
        })
        .await
    }
    async fn load_stream_trim_boundaries(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        let scopes = scopes.to_vec();
        self.with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                StreamRetentionTransaction::load_stream_trim_boundaries(attempt, &scopes).await
            })
        })
        .await
    }
}

#[async_trait]
pub trait SystemStreamTrimRepository: Send + Sync {
    async fn trim_system_stream_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult>;
}

#[async_trait]
impl<P: DynamoPort> SystemStreamTrimRepository for P
where for<'native> P::Attempt<'native>: stream_provider::SystemStreamTrimAttempt
{
    async fn trim_system_stream_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
                Box::pin(stream_provider::execute_trim_page(attempt, request))
            })
        })
        .await
    }
}
