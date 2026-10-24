use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamRetentionDuration,
    TimestampMillis,
};

use super::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamRetentionBoundaryDecision,
    StreamRetentionEvaluation, StreamRetentionFinish, StreamRetentionFinishReason,
    StreamRetentionHold, StreamRetentionReclaim, StreamRetentionStateDecision,
    StreamTrimDeletionBound, StreamTrimDueMarker, StreamTrimMarkerOutcome, StreamTrimPageLimit,
    StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimScopeKind, StreamTrimState,
    StreamTrimStateUpdate, StreamTrimStateWrite,
};

impl From<StreamItemId> for StreamRetentionHold {
    fn from(position: StreamItemId) -> Self {
        if position == StreamItemId::default() {
            Self::StreamHead
        } else {
            Self::Position(position)
        }
    }
}

impl StreamRetentionHold {
    /// Merges the row returned by the requested native read into the current
    /// trim floor. `StreamHead` accepts an empty stream; `Position` requires
    /// the exact persisted boundary to remain present.
    pub fn merge_trim_floor(
        self,
        retained: Option<StreamItemId>,
        current: Option<StreamItemId>,
    ) -> StorageResult<Option<StreamItemId>> {
        let retained = match self {
            Self::StreamHead => retained,
            Self::Position(expected) if retained == Some(expected) => Some(expected),
            Self::Position(_) => {
                return Err(StorageError::internal(
                    "active stream retention hold boundary is missing",
                ));
            }
        };
        Ok(match (current, retained) {
            (Some(current), Some(retained)) => Some(current.min(retained)),
            (current, retained) => current.or(retained),
        })
    }
}

impl StreamTrimScope {
    pub fn table(scope_id: impl Into<String>, table_name: storage_types::TableName) -> Self {
        Self {
            kind: StreamTrimScopeKind::Table,
            scope_id: scope_id.into(),
            table_name,
            item_key_hash: None,
        }
    }

    pub fn item(
        scope_id: impl Into<String>,
        table_name: storage_types::TableName,
        item_key_hash: impl Into<String>,
    ) -> Self {
        Self {
            kind: StreamTrimScopeKind::Item,
            scope_id: scope_id.into(),
            table_name,
            item_key_hash: Some(item_key_hash.into()),
        }
    }
}

impl StreamTrimState {
    pub fn next_policy_version(current: Option<&Self>) -> StorageResult<u64> {
        current.map_or(Ok(1), |state| {
            state
                .policy_version
                .checked_add(1)
                .ok_or_else(|| StorageError::validation("stream trim policy version overflow"))
        })
    }

    pub fn has_finite_due_work(&self) -> bool {
        self.effective_retention != StreamRetentionDuration::Forever && self.next_due_at.is_some()
    }

    pub fn marker_matches(&self, marker: &StreamTrimDueMarker) -> bool {
        self.scope.kind == marker.scope.kind
            && self.scope.scope_id == marker.scope.scope_id
            && self.scope.table_name == marker.scope.table_name
            && self.scope.item_key_hash == marker.scope.item_key_hash
            && self.policy_version == marker.policy_version
            && self.next_due_at.is_some_and(|due_at| {
                marker.due_bucket
                    == due_bucket_for(due_at, crate::constants::STREAM_TRIM_DUE_BUCKET_MILLIS)
            })
    }

    pub fn validated_marker_outcome(
        &self,
        marker: &StreamTrimDueMarker,
    ) -> StreamTrimMarkerOutcome {
        if !self.marker_matches(marker) {
            return StreamTrimMarkerOutcome::Stale;
        }
        if self.effective_retention == StreamRetentionDuration::Forever {
            return StreamTrimMarkerOutcome::Forever;
        }
        StreamTrimMarkerOutcome::Current
    }

    pub fn finish_page(
        &mut self,
        first_remaining_version: Option<ItemStreamVersion>,
        first_remaining_timestamp: Option<TimestampMillis>,
        observed_at: TimestampMillis,
    ) -> Option<StreamTrimDueMarker> {
        self.oldest_retained_version =
            monotonic_oldest_version(self.oldest_retained_version, first_remaining_version);
        self.oldest_retained_timestamp =
            monotonic_oldest_timestamp(self.oldest_retained_timestamp, first_remaining_timestamp);
        self.next_due_at = first_remaining_timestamp.and_then(|_| {
            next_due_from_first_remaining(self.oldest_retained_timestamp, self.effective_retention)
        });
        self.updated_at = observed_at;
        self.next_due_at
            .map(|due_at| StreamTrimDueMarker::new(due_at, self.scope.clone(), self.policy_version))
    }
}

impl StreamTrimStateWrite {
    /// A new item policy keeps the monotonic trim floor and invalidates every
    /// marker written by the preceding policy, even when the duration repeats.
    pub fn replace_item_policy(mut self, current: Option<&StreamTrimState>) -> StorageResult<Self> {
        if self.state.scope.kind != StreamTrimScopeKind::Item {
            return Err(StorageError::internal(
                "item retention replacement has a table scope",
            ));
        }
        let policy_version = StreamTrimState::next_policy_version(current)?;
        self = self.preserve_existing_history(current)?;
        self.state.policy_version = policy_version;
        if let Some(marker) = &mut self.next_marker {
            marker.policy_version = policy_version;
        }
        Ok(self)
    }

    /// Keep the prior trim floor while making existing rows eligible for an
    /// immediate evaluation under the replacement duration.
    pub(crate) fn preserve_existing_history(
        mut self,
        current: Option<&StreamTrimState>,
    ) -> StorageResult<Self> {
        let Some(current) = current else {
            return Ok(self);
        };
        if self.state.scope != current.scope {
            return Err(StorageError::internal(
                "stream retention replacement has another scope",
            ));
        }
        self.state.oldest_retained_version = current.oldest_retained_version;
        self.state.oldest_retained_timestamp = current.oldest_retained_timestamp;
        self.state.latest_version = current.latest_version;
        self.state.latest_timestamp = current.latest_timestamp;
        self.state.next_due_at = match self.state.effective_retention {
            StreamRetentionDuration::Forever => None,
            StreamRetentionDuration::FiniteHours(_) => Some(self.state.updated_at),
        };
        self.next_marker = self.state.next_due_at.map(|due_at| {
            StreamTrimDueMarker::new(due_at, self.state.scope.clone(), self.state.policy_version)
        });
        Ok(self)
    }
}

impl StreamTrimDueMarker {
    pub fn new(due_at: TimestampMillis, scope: StreamTrimScope, policy_version: u64) -> Self {
        Self {
            due_bucket: due_bucket_for(due_at, crate::constants::STREAM_TRIM_DUE_BUCKET_MILLIS),
            scope,
            policy_version,
        }
    }

    pub fn rescheduled_after(
        &self,
        due_before: TimestampMillis,
        now: TimestampMillis,
    ) -> StorageResult<Self> {
        let horizon = due_before.max(now);
        let current_bucket =
            due_bucket_for(horizon, crate::constants::STREAM_TRIM_DUE_BUCKET_MILLIS);
        let next_bucket = current_bucket
            .timestamp_millis()
            .checked_add(crate::constants::STREAM_TRIM_DUE_BUCKET_MILLIS)
            .ok_or_else(|| StorageError::validation("stream trim due bucket overflow"))?;
        Ok(Self::new(
            TimestampMillis::from_timestamp(next_bucket),
            self.scope.clone(),
            self.policy_version,
        ))
    }
}

fn due_bucket_for(due_at: TimestampMillis, bucket_millis: i64) -> TimestampMillis {
    debug_assert!(bucket_millis > 0);
    let due_ms = due_at.timestamp_millis();
    TimestampMillis::from_timestamp(due_ms.div_euclid(bucket_millis) * bucket_millis)
}

fn next_due_from_first_remaining(
    first_remaining_timestamp: Option<TimestampMillis>,
    retention: StreamRetentionDuration,
) -> Option<TimestampMillis> {
    let first_remaining_timestamp = first_remaining_timestamp?;
    match retention {
        StreamRetentionDuration::Forever => None,
        StreamRetentionDuration::FiniteHours(hours) => {
            Some(first_remaining_timestamp + (i64::from(hours) * 60 * 60 * 1000))
        }
    }
}

impl StreamTrimDeletionBound {
    pub fn for_item_history(
        requested_max: Option<StreamItemId>,
        observation: super::models::ItemHistoryRetention,
    ) -> storage_types::StorageResult<Self> {
        let predecessor = observation.protected_predecessor()?;
        Ok(Self::for_reclamation(
            requested_max,
            observation.latest,
            None,
            predecessor,
        ))
    }

    pub fn for_reclamation(
        requested_max: Option<StreamItemId>,
        latest_item_id: Option<StreamItemId>,
        protected_item_id: Option<StreamItemId>,
        retained_table_pointer_item_id: Option<StreamItemId>,
    ) -> Self {
        let boundaries = [
            latest_item_id,
            protected_item_id,
            retained_table_pointer_item_id,
        ];
        match boundaries.into_iter().flatten().min() {
            None => requested_max
                .map(Self::Inclusive)
                .unwrap_or(Self::Unbounded),
            Some(boundary) => match previous_stream_item_id(boundary) {
                Some(boundary) => Self::Inclusive(
                    requested_max.map_or(boundary, |requested| requested.min(boundary)),
                ),
                None => Self::DeleteNone,
            },
        }
    }
}

fn previous_stream_item_id(item_id: StreamItemId) -> Option<StreamItemId> {
    let mut bytes = *item_id.as_bytes();
    for byte in bytes.iter_mut().rev() {
        if *byte > 0 {
            *byte -= 1;
            return Some(StreamItemId::from(bytes));
        }
        *byte = u8::MAX;
    }
    None
}

fn monotonic_oldest_version(
    previous: Option<ItemStreamVersion>,
    observed: Option<ItemStreamVersion>,
) -> Option<ItemStreamVersion> {
    match (previous, observed) {
        (Some(previous), Some(observed)) => Some(previous.max(observed)),
        (Some(previous), None) => Some(previous),
        (None, observed) => observed,
    }
}

fn monotonic_oldest_timestamp(
    previous: Option<TimestampMillis>,
    observed: Option<TimestampMillis>,
) -> Option<TimestampMillis> {
    match (previous, observed) {
        (Some(previous), Some(observed)) => Some(previous.max(observed)),
        (Some(previous), None) => Some(previous),
        (None, observed) => observed,
    }
}

impl StreamRetentionEvaluation {
    pub fn decide_state(
        marker: StreamTrimDueMarker,
        state: Option<StreamTrimState>,
        due_before: TimestampMillis,
        observed_at: TimestampMillis,
        page_limit: StreamTrimPageLimit,
    ) -> StorageResult<StreamRetentionStateDecision> {
        let Some(mut state) = state else {
            return Ok(StreamRetentionStateDecision::Finish(
                StreamRetentionFinish {
                    marker,
                    update: None,
                    reason: StreamRetentionFinishReason::MissingState,
                },
            ));
        };
        match state.validated_marker_outcome(&marker) {
            StreamTrimMarkerOutcome::Stale => Ok(StreamRetentionStateDecision::Finish(
                StreamRetentionFinish {
                    marker,
                    update: None,
                    reason: StreamRetentionFinishReason::StaleMarker,
                },
            )),
            StreamTrimMarkerOutcome::Forever => {
                let expected_state = state.clone();
                state.next_due_at = None;
                Ok(StreamRetentionStateDecision::Finish(
                    StreamRetentionFinish {
                        marker,
                        update: Some(StreamTrimStateUpdate {
                            expected_state,
                            write: StreamTrimStateWrite {
                                state,
                                next_marker: None,
                            },
                        }),
                        reason: StreamRetentionFinishReason::Forever,
                    },
                ))
            }
            StreamTrimMarkerOutcome::Current => {
                if state.effective_retention.cutoff_at(observed_at).is_none() {
                    return Err(StorageError::internal(
                        "current stream retention state has no finite cutoff",
                    ));
                }
                Ok(StreamRetentionStateDecision::ReadBoundaries {
                    scope: state.scope.clone(),
                    evaluation: Self {
                        marker,
                        state,
                        due_before,
                        observed_at,
                        page_limit,
                    },
                })
            }
        }
    }

    pub fn decide_boundaries(
        mut self,
        boundaries: StreamTrimScopeBoundaries,
    ) -> StorageResult<StreamRetentionBoundaryDecision> {
        let deletion_bound = StreamTrimDeletionBound::for_reclamation(
            None,
            boundaries.latest_item_id,
            boundaries
                .protected_boundary
                .map(|boundary| boundary.item_id),
            boundaries
                .retained_table_pointer_boundary
                .map(|boundary| boundary.item_id),
        );
        if matches!(deletion_bound, StreamTrimDeletionBound::DeleteNone) {
            let expected_state = self.state.clone();
            let next_marker = self
                .marker
                .rescheduled_after(self.due_before, self.observed_at)?;
            self.state.next_due_at = Some(next_marker.due_bucket);
            return Ok(StreamRetentionBoundaryDecision::Finish(
                StreamRetentionFinish {
                    marker: self.marker,
                    update: Some(StreamTrimStateUpdate {
                        expected_state,
                        write: StreamTrimStateWrite {
                            state: self.state,
                            next_marker: Some(next_marker),
                        },
                    }),
                    reason: StreamRetentionFinishReason::Protected,
                },
            ));
        }
        let cutoff_timestamp = self
            .state
            .effective_retention
            .cutoff_at(self.observed_at)
            .ok_or_else(|| {
                StorageError::internal("current stream retention state has no finite cutoff")
            })?;
        let request = StreamDurationTrimPageRequest {
            scope: self.state.scope.clone(),
            cutoff_timestamp,
            max_deleted_item_id: match deletion_bound {
                StreamTrimDeletionBound::Unbounded => None,
                StreamTrimDeletionBound::Inclusive(max_deleted_item_id) => {
                    Some(max_deleted_item_id)
                }
                StreamTrimDeletionBound::DeleteNone => unreachable!("handled above"),
            },
            page_limit: self.page_limit,
        };
        Ok(StreamRetentionBoundaryDecision::Reclaim(
            StreamRetentionReclaim {
                request,
                marker: self.marker,
                state: self.state,
                deletion_bound,
                due_before: self.due_before,
                observed_at: self.observed_at,
            },
        ))
    }
}

impl StreamRetentionReclaim {
    pub fn complete(
        mut self,
        page: StreamDurationTrimPageResult,
    ) -> StorageResult<StreamRetentionFinish> {
        let expected_state = self.state.clone();
        let next_marker = self.state.finish_page(
            page.first_remaining_version,
            page.first_remaining_timestamp,
            self.observed_at,
        );
        let next_marker = if page.deleted_rows == 0
            && matches!(self.deletion_bound, StreamTrimDeletionBound::Inclusive(_))
            && next_marker
                .as_ref()
                .is_some_and(|next_marker| next_marker.due_bucket <= self.due_before)
        {
            let next_marker = self
                .marker
                .rescheduled_after(self.due_before, self.observed_at)?;
            self.state.next_due_at = Some(next_marker.due_bucket);
            Some(next_marker)
        } else {
            next_marker
        };
        Ok(StreamRetentionFinish {
            marker: self.marker,
            update: Some(StreamTrimStateUpdate {
                expected_state,
                write: StreamTrimStateWrite {
                    state: self.state,
                    next_marker,
                },
            }),
            reason: StreamRetentionFinishReason::Page {
                deleted_rows: page.deleted_rows,
            },
        })
    }
}

impl super::models::ItemHistoryRetention {
    pub(super) fn protected_predecessor(&self) -> StorageResult<Option<StreamItemId>> {
        let predecessor = match self.referenced {
            None => None,
            Some(referenced) => {
                if self.at_reference[0] != Some(referenced)
                    || self.latest.is_none_or(|latest| latest < referenced)
                    || self.at_reference[1].is_some_and(|previous| previous >= referenced)
                {
                    return Err(storage_types::StorageEnum::HistoryUnavailable.into());
                }
                self.at_reference[1].or(Some(referenced))
            }
        };
        Ok(predecessor)
    }
}
