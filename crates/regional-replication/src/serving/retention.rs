use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{
    DynamoPort,
    maintenance::{
        StreamDurationTrimStats, StreamRetentionPort, StreamRetentionRepository,
        StreamRetentionTransaction, StreamRetentionWave, StreamTrimDueMarker, StreamTrimScope,
        StreamTrimScopeBoundaries, StreamTrimState,
    },
    native_admission::NativeWork,
};
use storage_types::{AttemptMode, StorageResult, TimestampMillis};

use super::runtime::RegionalDynamo;
use crate::journal::NativeAttempt;

#[async_trait]
impl<P: DynamoPort> StreamRetentionRepository for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: StreamRetentionPort + PhysicalRecordAttempt
{
    async fn trim_stream_marker_wave(
        &self,
        wave: StreamRetentionWave,
    ) -> StorageResult<StreamDurationTrimStats> {
        Arc::clone(&self.journal)
            .with_admitted_attempt(
                (self.port.as_ref(), NativeWork::Background),
                NativeAttempt::Items(AttemptMode::ReadWrite),
                move |attempt, _| Box::pin(async move { wave.execute(attempt).await }),
            )
            .await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        Arc::clone(&self.journal)
            .with_admitted_attempt(
                (self.port.as_ref(), NativeWork::Background),
                NativeAttempt::Items(AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(StreamRetentionPort::list_due_stream_trim_markers(
                        attempt, due_before, limit,
                    ))
                },
            )
            .await
    }

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        let scopes = scopes.to_vec();
        Arc::clone(&self.journal)
            .with_admitted_attempt(
                (self.port.as_ref(), NativeWork::Background),
                NativeAttempt::Items(AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(async move {
                        StreamRetentionPort::load_stream_trim_states(attempt, &scopes).await
                    })
                },
            )
            .await
    }

    async fn load_stream_trim_boundaries(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        let scopes = scopes.to_vec();
        Arc::clone(&self.journal)
            .with_admitted_attempt(
                (self.port.as_ref(), NativeWork::Background),
                NativeAttempt::Items(AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(async move {
                        StreamRetentionTransaction::load_stream_trim_boundaries(attempt, &scopes)
                            .await
                    })
                },
            )
            .await
    }
}
