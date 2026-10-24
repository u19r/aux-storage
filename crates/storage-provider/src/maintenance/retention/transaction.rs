//! Scheduled retention decisions run entirely inside the caller's native
//! attempt.

use std::{collections::HashSet, future::Future};

use infrastructure_provider::physical_attempt::{PhysicalRecordAttempt, PhysicalRecordOperation};
use storage_types::{StorageResult, StreamItemId, TimestampMillis};
use stream_provider::{StreamAttempt, protected_cursor_floor_in_attempt};

use super::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamDurationTrimStats,
    StreamRetentionBoundaryDecision, StreamRetentionEvaluation, StreamRetentionFinish,
    StreamRetentionReclaim, StreamRetentionStateDecision, StreamTrimDueMarker, StreamTrimPageLimit,
    StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimScopeKind, StreamTrimState,
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

/// Borrowed native transaction I/O. Implementations must not open transactions.
pub trait StreamRetentionTransaction: PhysicalRecordAttempt {
    fn load_stream_trim_observations(
        &mut self,
        markers: &[StreamTrimDueMarker],
    ) -> impl Future<Output = StorageResult<Vec<StreamRetentionObservation>>> + Send;

    fn load_stream_trim_boundaries(
        &mut self,
        scopes: &[StreamTrimScope],
    ) -> impl Future<Output = StorageResult<Vec<StreamTrimScopeBoundaries>>> + Send;

    fn trim_stream_pages(
        &mut self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> impl Future<Output = StorageResult<Vec<StreamDurationTrimPageResult>>> + Send;

    fn finish_stream_trim_markers(
        &mut self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

/// Physical retention I/O joined to the initialized store's native attempt.
pub trait StreamRetentionPort: StreamAttempt {
    fn load_stream_trim_observations(
        &mut self,
        markers: &[StreamTrimDueMarker],
    ) -> impl Future<Output = StorageResult<Vec<StreamRetentionObservation>>> + Send;
    fn load_stream_trim_boundaries(
        &mut self,
        scopes: &[StreamTrimScope],
        floor: Option<StreamItemId>,
    ) -> impl Future<Output = StorageResult<Vec<StreamTrimScopeBoundaries>>> + Send;
    fn trim_stream_pages(
        &mut self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> impl Future<Output = StorageResult<Vec<StreamDurationTrimPageResult>>> + Send;
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

impl<P: StreamRetentionPort> StreamRetentionTransaction for P {
    fn load_stream_trim_observations(
        &mut self,
        markers: &[StreamTrimDueMarker],
    ) -> impl Future<Output = StorageResult<Vec<StreamRetentionObservation>>> {
        StreamRetentionPort::load_stream_trim_observations(self, markers)
    }
    async fn load_stream_trim_boundaries(
        &mut self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        let floor = protected_table_floor_for_scopes(self, scopes).await?;
        StreamRetentionPort::load_stream_trim_boundaries(self, scopes, floor).await
    }
    fn trim_stream_pages(
        &mut self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> impl Future<Output = StorageResult<Vec<StreamDurationTrimPageResult>>> {
        StreamRetentionPort::trim_stream_pages(self, requests)
    }
    fn finish_stream_trim_markers(
        &mut self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> impl Future<Output = StorageResult<()>> {
        StreamRetentionPort::finish_stream_trim_markers(self, finishes)
    }
}

/// Only table stream retention needs the protected global cursor floor.
/// Read it inside the same native attempt as the physical boundary page.
pub async fn protected_table_floor_for_scopes<A: StreamAttempt>(
    attempt: &mut A,
    scopes: &[StreamTrimScope],
) -> StorageResult<Option<StreamItemId>> {
    if scopes
        .iter()
        .any(|scope| scope.kind == StreamTrimScopeKind::Table)
    {
        Ok(protected_cursor_floor_in_attempt(attempt).await?)
    } else {
        Ok(None)
    }
}

impl StreamRetentionWave {
    pub fn try_new(
        markers: Vec<StreamTrimDueMarker>,
        due_before: TimestampMillis,
        observed_at: TimestampMillis,
        page_limit: StreamTrimPageLimit,
    ) -> StorageResult<Self> {
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
        transaction: &mut impl StreamRetentionTransaction,
    ) -> StorageResult<StreamDurationTrimStats> {
        let mut settlement = PhysicalRecordOperation::new(transaction);
        let transaction = settlement.as_mut();
        let result = async {
            if self.markers.is_empty() {
                return Ok(StreamDurationTrimStats::default());
            }
            let finishes = self.decide_wave(transaction).await?;
            let mut stats = StreamDurationTrimStats {
                due_markers_scanned: self.markers.len(),
                ..StreamDurationTrimStats::default()
            };
            stats.record_finishes(&finishes);
            transaction.finish_stream_trim_markers(finishes).await?;
            Ok(stats)
        }
        .await;
        settlement.complete(result)
    }

    async fn decide_wave(
        &self,
        transaction: &mut impl StreamRetentionTransaction,
    ) -> StorageResult<Vec<StreamRetentionFinish>> {
        let states = transaction
            .load_stream_trim_observations(&self.markers)
            .await?;
        if states.len() != self.markers.len() {
            return Err(storage_types::StorageError::internal(
                "stream retention state batch returned an invalid result count",
            ));
        }
        let decisions = self
            .markers
            .iter()
            .cloned()
            .zip(states)
            .filter_map(|(marker, observation)| {
                let StreamRetentionObservation::Present(state) = observation else {
                    return None;
                };
                Some(StreamRetentionEvaluation::decide_state(
                    marker,
                    state,
                    self.due_before,
                    self.observed_at,
                    self.page_limit,
                ))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        self.complete_decisions(transaction, decisions).await
    }

    async fn complete_decisions(
        &self,
        transaction: &mut impl StreamRetentionTransaction,
        decisions: Vec<StreamRetentionStateDecision>,
    ) -> StorageResult<Vec<StreamRetentionFinish>> {
        let boundaries = Self::observe_boundaries(transaction, &decisions).await?;
        let (mut finishes, reclaims) = Self::evaluate_boundaries(decisions, boundaries)?;
        if reclaims.is_empty() {
            return Ok(finishes);
        }
        let requests = reclaims
            .iter()
            .map(|reclaim| reclaim.request.clone())
            .collect();
        let pages = transaction.trim_stream_pages(requests).await?;
        if pages.len() != reclaims.len() {
            return Err(storage_types::StorageError::internal(
                "stream retention trim batch returned an invalid result count",
            ));
        }
        for (reclaim, page) in reclaims.into_iter().zip(pages) {
            finishes.push(reclaim.complete(page)?);
        }
        Ok(finishes)
    }

    async fn observe_boundaries(
        transaction: &mut impl StreamRetentionTransaction,
        decisions: &[StreamRetentionStateDecision],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        let scopes = decisions
            .iter()
            .filter_map(|decision| match decision {
                StreamRetentionStateDecision::Finish(_) => None,
                StreamRetentionStateDecision::ReadBoundaries { scope, .. } => Some(scope.clone()),
            })
            .collect::<Vec<_>>();
        let boundaries = if scopes.is_empty() {
            Vec::new()
        } else {
            transaction.load_stream_trim_boundaries(&scopes).await?
        };
        if boundaries.len() != scopes.len() {
            return Err(storage_types::StorageError::internal(
                "stream retention boundary batch returned an invalid result count",
            ));
        }
        Ok(boundaries)
    }

    fn evaluate_boundaries(
        decisions: Vec<StreamRetentionStateDecision>,
        boundaries: Vec<StreamTrimScopeBoundaries>,
    ) -> StorageResult<(Vec<StreamRetentionFinish>, Vec<StreamRetentionReclaim>)> {
        let mut boundaries = boundaries.into_iter();
        let mut finishes = Vec::with_capacity(decisions.len());
        let mut reclaims = Vec::new();
        for decision in decisions {
            let decision = match decision {
                StreamRetentionStateDecision::Finish(finish) => {
                    finishes.push(finish);
                    continue;
                }
                StreamRetentionStateDecision::ReadBoundaries { evaluation, .. } => {
                    let boundary = boundaries.next().ok_or_else(|| {
                        storage_types::StorageError::internal(
                            "stream retention boundary batch is incomplete",
                        )
                    })?;
                    evaluation.decide_boundaries(boundary)?
                }
            };
            match decision {
                StreamRetentionBoundaryDecision::Finish(finish) => finishes.push(finish),
                StreamRetentionBoundaryDecision::Reclaim(reclaim) => reclaims.push(reclaim),
            }
        }
        Ok((finishes, reclaims))
    }
}
