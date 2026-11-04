//! Scheduled retention decisions run entirely inside the caller's native
//! attempt.

use std::{collections::HashSet, future::Future};

use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use storage_types::{StorageResult, TimestampMillis};
use stream_provider::StreamAttempt;

use super::{
    StreamDurationTrimStats, StreamRetentionFinish, StreamTrimDueMarker, StreamTrimPageLimit,
    StreamTrimScope, StreamTrimState,
};

/// One bounded marker wave, with at most one marker for each scope.
/// The worker groups repeated scopes into separate waves. The adapter must
/// commit observations, deletes, and progress together, or roll back before
/// retrying.
#[derive(Clone)]
pub struct StreamRetentionWave {
    pub(super) markers: Vec<StreamTrimDueMarker>,
    pub due_before: TimestampMillis,
    pub observed_at: TimestampMillis,
    pub page_limit: StreamTrimPageLimit,
}

/// Protected facts for one exact discovered marker.
pub enum StreamRetentionObservation {
    Absent,
    Present(Option<StreamTrimState>),
}

impl From<(bool, Option<StreamTrimState>)> for StreamRetentionObservation {
    fn from((present, state): (bool, Option<StreamTrimState>)) -> Self {
        if present {
            Self::Present(state)
        } else {
            Self::Absent
        }
    }
}

/// Physical retention I/O joined to the initialized store's native attempt.
pub trait StreamRetentionPort:
    StreamAttempt
    + crate::GetItemAttempt
    + stream_provider::TableHistoryIndexAttempt
    + super::ItemHistoryMetadataAttempt
    + stream_provider::HistoryRowDeleteAttempt
{
    fn load_stream_trim_observations(
        &mut self,
        markers: &[StreamTrimDueMarker],
    ) -> impl Future<Output = StorageResult<Vec<StreamRetentionObservation>>> + Send;
    fn finish_stream_trim_markers(
        &mut self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
    fn list_due_stream_trim_markers(
        &mut self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> impl Future<Output = StorageResult<Vec<StreamTrimDueMarker>>> + Send;
    fn load_stream_trim_states(
        &mut self,
        scopes: &[StreamTrimScope],
    ) -> impl Future<Output = StorageResult<Vec<Option<StreamTrimState>>>> + Send;
}

impl StreamRetentionWave {
    pub fn try_new(
        markers: Vec<StreamTrimDueMarker>,
        due_before: TimestampMillis,
        observed_at: TimestampMillis,
        page_limit: StreamTrimPageLimit,
    ) -> StorageResult<Self> {
        if markers.len() > crate::constants::TABLE_CLEANUP_PAGE_ROWS {
            return Err(storage_types::StorageError::validation(
                "stream retention wave exceeds its bounded marker count",
            ));
        }
        let mut scopes = HashSet::with_capacity(markers.len());
        if markers
            .iter()
            .any(|marker| !scopes.insert(marker.scope.scope_id.as_str()))
        {
            return Err(storage_types::StorageError::validation(
                "stream retention wave contains duplicate scopes",
            ));
        }
        Ok(Self {
            markers,
            due_before,
            observed_at,
            page_limit,
        })
    }

    /// Read-only markers preserve the one-marker-per-scope construction
    /// invariant.
    pub fn markers(&self) -> &[StreamTrimDueMarker] {
        &self.markers
    }

    pub async fn execute(
        &self,
        transaction: &mut impl StreamRetentionPort,
    ) -> StorageResult<StreamDurationTrimStats> {
        let mut settlement = PhysicalRecordOperation::new(transaction);
        let transaction = settlement.as_mut();
        let result = async {
            if self.markers.is_empty() {
                return Ok(StreamDurationTrimStats::default());
            }
            super::cleanup_wave::execute_wave(self, transaction).await
        }
        .await;
        settlement.complete(result)
    }
}
