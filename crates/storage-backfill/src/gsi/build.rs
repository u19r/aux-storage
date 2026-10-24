use storage_types::SystemStreamCheckpoint;

use super::{
    GsiActivationObservation, GsiActive, GsiBlocked, GsiBuild, GsiBuildCandidates,
    GsiBuildDiscovery, GsiBuildProjectionObservation, GsiCatchingUp, GsiChange, GsiFailureReason,
    GsiLifecycleError, GsiObservation, GsiPageLimit, GsiProjectionWrite, GsiProjectionWriteBatch,
    GsiReadRequest, GsiScanObservation, GsiScanPageEnd, GsiScanning, GsiSourceProjection, GsiState,
    GsiStreamObservation, GsiStreamPageEnd, models::GsiBuildProjectionChange,
};

impl<Cursor> GsiBuildCandidates<Cursor> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            scanned_through: None,
            selected: None,
        }
    }

    /// Select the first runnable build in the durable scheduler's key order.
    pub fn select_next_build_candidate(&mut self, build: GsiBuild<Cursor>) -> GsiBuildDiscovery {
        self.scanned_through = Some(build.target.clone());
        if matches!(build.state, GsiState::Blocked(_)) {
            return GsiBuildDiscovery::Continue;
        }
        self.selected = Some(build);
        GsiBuildDiscovery::SelectionComplete
    }

    #[must_use]
    pub fn schedule(self) -> Option<GsiBuild<Cursor>> {
        self.selected
    }
}

impl<Cursor> Default for GsiBuildCandidates<Cursor> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Cursor> GsiState<Cursor> {
    #[must_use]
    pub const fn permits_projection(&self) -> bool {
        !matches!(self, Self::Blocked(_))
    }

    #[must_use]
    pub const fn permits_queries(&self) -> bool {
        matches!(self, Self::Active(_))
    }

    #[must_use]
    pub fn required_stream_history(&self) -> &SystemStreamCheckpoint {
        match self {
            Self::Scanning(state) => &state.low,
            Self::CatchingUp(state) => &state.cursor,
            Self::Active(state) => &state.cursor,
            Self::Blocked(state) => &state.retained_from,
        }
    }
}

impl<Cursor> GsiBuild<Cursor> {
    pub fn history_hold_change<'a>(
        &'a self,
        next: &'a GsiState<Cursor>,
    ) -> Option<(&'a SystemStreamCheckpoint, &'a SystemStreamCheckpoint)> {
        let old = self.state.required_stream_history();
        let new = next.required_stream_history();
        (old != new).then_some((old, new))
    }

    pub fn block_unavailable_history(&self) -> GsiChange<Cursor> {
        GsiChange::Persist {
            projections: GsiProjectionWriteBatch(Vec::new()),
            next_state: GsiState::Blocked(GsiBlocked {
                reason: GsiFailureReason::HistoryUnavailable,
                retained_from: self.state.required_stream_history().clone(),
            }),
        }
    }

    #[must_use]
    pub fn new(target: super::GsiBuildTarget, low: SystemStreamCheckpoint) -> Self {
        Self {
            target,
            state: GsiState::Scanning(GsiScanning { low, after: None }),
        }
    }

    #[must_use]
    pub fn new_active(target: super::GsiBuildTarget, cursor: SystemStreamCheckpoint) -> Self {
        Self {
            target,
            state: GsiState::Active(GsiActive {
                cursor,
                pending_high: None,
            }),
        }
    }

    pub fn read_request(
        &self,
        limit: GsiPageLimit,
    ) -> Result<GsiReadRequest<&Cursor>, GsiLifecycleError> {
        match &self.state {
            GsiState::Scanning(state) => Ok(GsiReadRequest::Scan {
                after: state.after.as_ref(),
                limit,
            }),
            GsiState::CatchingUp(state) if state.cursor != state.high => {
                Ok(GsiReadRequest::CatchUp {
                    after: state.cursor.clone(),
                    through: state.high.clone(),
                    limit,
                })
            }
            GsiState::CatchingUp(state) if state.cursor == state.high => {
                Ok(GsiReadRequest::InspectActivation {
                    required_from: state.cursor.clone(),
                    through: state.high.clone(),
                })
            }
            GsiState::CatchingUp(_) => Err(GsiLifecycleError::InvalidBoundary),
            GsiState::Active(state) => Ok(GsiReadRequest::Continue {
                after: state.cursor.clone(),
                pending_high: state.pending_high.clone(),
                limit,
            }),
            GsiState::Blocked(_) => Err(GsiLifecycleError::InvalidPhase),
        }
    }
}

impl<Cursor: PartialEq> GsiBuild<Cursor> {
    pub fn decide_next_change(
        &self,
        observation: GsiObservation<Cursor>,
    ) -> Result<GsiChange<Cursor>, GsiLifecycleError> {
        match (&self.state, observation) {
            (GsiState::Scanning(state), GsiObservation::Scan(observation)) => {
                Self::decide_scan_page_change(state, observation)
            }
            (GsiState::CatchingUp(state), GsiObservation::Stream(observation)) => {
                Self::decide_catch_up_page_change(state, observation)
            }
            (GsiState::Active(state), GsiObservation::Stream(observation)) => {
                Self::decide_active_continuation_change(state, observation)
            }
            (GsiState::CatchingUp(state), GsiObservation::Activation(observation)) => {
                Self::decide_activation_change(&self.target, state, observation)
            }
            _ => Err(GsiLifecycleError::ObservationMismatch),
        }
    }

    fn decide_scan_page_change(
        state: &GsiScanning<Cursor>,
        observation: GsiScanObservation<Cursor>,
    ) -> Result<GsiChange<Cursor>, GsiLifecycleError> {
        if state.after.as_ref() != observation.expected_after.as_ref() {
            return Err(GsiLifecycleError::ProgressConflict);
        }
        let Ok(projections) = Self::decide_projection_writes(observation.projections) else {
            return Ok(Self::blocked(
                state.low.clone(),
                GsiFailureReason::ProjectionConflict,
            ));
        };
        if let GsiScanPageEnd::Complete(high) = &observation.end
            && high.validate_scope(state.low.scope()).is_err()
        {
            return Ok(Self::blocked(
                state.low.clone(),
                GsiFailureReason::HistoryUnavailable,
            ));
        }
        let next_state = match state
            .advance_page(observation.end)
            .map_err(|_| GsiLifecycleError::ProgressConflict)?
        {
            crate::ScanTransition::Scanning(next) => GsiState::Scanning(next),
            crate::ScanTransition::CatchingUp(next) => GsiState::CatchingUp(next),
        };
        Ok(Self::persist(projections, next_state))
    }

    fn decide_catch_up_page_change(
        state: &GsiCatchingUp,
        observation: GsiStreamObservation,
    ) -> Result<GsiChange<Cursor>, GsiLifecycleError> {
        if observation.expected_cursor != state.cursor || observation.through != state.high {
            return Err(GsiLifecycleError::ProgressConflict);
        }
        let Ok(projections) = Self::decide_projection_writes(observation.projections) else {
            return Ok(Self::blocked(
                state.cursor.clone(),
                GsiFailureReason::ProjectionConflict,
            ));
        };
        let next = state
            .advance_page(observation.end)
            .map_err(|_| GsiLifecycleError::InvalidBoundary)?;
        let next_state = GsiState::CatchingUp(next);
        Ok(Self::persist(projections, next_state))
    }

    fn decide_active_continuation_change(
        state: &GsiActive,
        observation: GsiStreamObservation,
    ) -> Result<GsiChange<Cursor>, GsiLifecycleError> {
        if observation.expected_cursor != state.cursor {
            return Err(GsiLifecycleError::ProgressConflict);
        }
        if state
            .pending_high
            .as_ref()
            .is_some_and(|high| *high != observation.through)
        {
            return Err(GsiLifecycleError::ProgressConflict);
        }
        let Ok(projections) = Self::decide_projection_writes(observation.projections) else {
            return Ok(Self::blocked(
                state.cursor.clone(),
                GsiFailureReason::ProjectionConflict,
            ));
        };
        let cursor = match observation.end {
            GsiStreamPageEnd::Continue(next)
                if next != state.cursor && next != observation.through =>
            {
                next
            }
            GsiStreamPageEnd::Continue(_) => return Err(GsiLifecycleError::InvalidBoundary),
            GsiStreamPageEnd::ReachedThrough => observation.through.clone(),
        };
        if cursor == state.cursor && projections.0.is_empty() && state.pending_high.is_none() {
            return Ok(GsiChange::Idle);
        }
        Ok(Self::persist(
            projections,
            GsiState::Active(GsiActive {
                pending_high: (cursor != observation.through).then_some(observation.through),
                cursor,
            }),
        ))
    }

    fn decide_activation_change(
        target: &super::GsiBuildTarget,
        state: &GsiCatchingUp,
        observation: GsiActivationObservation,
    ) -> Result<GsiChange<Cursor>, GsiLifecycleError> {
        if state.cursor != state.high {
            return Err(GsiLifecycleError::InvalidPhase);
        }
        if observation.target_incarnation != target.incarnation {
            return Ok(GsiChange::Superseded);
        }
        if observation.projected_through != state.high {
            return Err(GsiLifecycleError::ProgressConflict);
        }
        Ok(GsiChange::Persist {
            projections: GsiProjectionWriteBatch(Vec::new()),
            next_state: GsiState::Active(GsiActive {
                cursor: state.high.clone(),
                pending_high: None,
            }),
        })
    }

    fn decide_projection_writes(
        observations: Vec<GsiBuildProjectionObservation>,
    ) -> Result<GsiProjectionWriteBatch, GsiFailureReason> {
        // Destination keys this batch writes, with versions. A Missing
        // delete whose row lands later in the same batch (paired put/delete
        // observations for one item version step) reads no current state,
        // so without this the delete is dropped and the paired put leaves
        // the stale row behind. Gating on a strictly lower sibling version
        // keeps the common single-observation path free of tombstone writes.
        let batch_puts: Vec<(super::GsiProjectionKey, storage_types::ItemRevision)> = observations
            .iter()
            .filter_map(|sibling| match &sibling.source {
                GsiSourceProjection::Present { item_version, row } => {
                    Some((row.key.clone(), *item_version))
                }
                _ => None,
            })
            .collect();
        let mut writes = Vec::new();
        for observation in observations {
            // A delete for a key this batch itself writes below the delete
            // version must still apply even though the precondition read
            // found nothing yet.
            if let (
                GsiSourceProjection::Missing {
                    previous_key: Some(previous_key),
                    item_version,
                },
                None,
            ) = (&observation.source, &observation.current)
                && batch_puts
                    .iter()
                    .any(|(key, version)| *key == **previous_key && *version < *item_version)
            {
                Self::merge_projection_write(
                    &mut writes,
                    GsiProjectionWrite {
                        key: previous_key.as_ref().clone(),
                        row: None,
                        item_version: *item_version,
                    },
                );
                continue;
            }
            match observation.decide() {
                GsiBuildProjectionChange::IgnoreStale | GsiBuildProjectionChange::Replay => {}
                GsiBuildProjectionChange::FailClosed(reason) => return Err(reason),
                GsiBuildProjectionChange::Replace {
                    remove,
                    write,
                    item_version,
                } => {
                    let same_key = remove
                        .as_ref()
                        .zip(write.as_ref())
                        .is_some_and(|(remove, write)| **remove == write.key);
                    if let Some(key) = remove.filter(|_| !same_key) {
                        Self::merge_projection_write(
                            &mut writes,
                            GsiProjectionWrite {
                                key: *key,
                                row: None,
                                item_version,
                            },
                        );
                    }
                    if let Some(row) = write {
                        let key = row.key.clone();
                        Self::merge_projection_write(
                            &mut writes,
                            GsiProjectionWrite {
                                key,
                                row: Some(*row),
                                item_version,
                            },
                        );
                    }
                }
            }
        }
        Ok(GsiProjectionWriteBatch(writes))
    }

    fn merge_projection_write(writes: &mut Vec<GsiProjectionWrite>, write: GsiProjectionWrite) {
        let existing = writes
            .iter()
            .position(|candidate| candidate.key == write.key);
        match existing {
            Some(position) if writes[position].item_version <= write.item_version => {
                writes[position] = write;
            }
            Some(_) => {}
            None => writes.push(write),
        }
    }

    fn blocked(low: SystemStreamCheckpoint, reason: GsiFailureReason) -> GsiChange<Cursor> {
        Self::persist(
            GsiProjectionWriteBatch(Vec::new()),
            GsiState::Blocked(GsiBlocked {
                reason,
                retained_from: low,
            }),
        )
    }

    fn persist(
        projections: GsiProjectionWriteBatch,
        next_state: GsiState<Cursor>,
    ) -> GsiChange<Cursor> {
        GsiChange::Persist {
            projections,
            next_state,
        }
    }
}

impl GsiBuildProjectionObservation {
    pub fn attach_all(
        sources: Vec<GsiSourceProjection>,
        current: Vec<Option<super::GsiProjection<super::GsiProjectionKey>>>,
    ) -> Result<Vec<Self>, GsiLifecycleError> {
        if sources.len() != current.len() {
            return Err(GsiLifecycleError::ObservationMismatch);
        }
        Ok(sources
            .into_iter()
            .zip(current)
            .map(|(source, current)| Self { source, current })
            .collect())
    }

    #[must_use]
    pub(super) fn decide(self) -> GsiBuildProjectionChange {
        match self.source {
            GsiSourceProjection::Missing { item_version, .. } => {
                Self::decide_missing(item_version, self.current)
            }
            GsiSourceProjection::Present { item_version, row } => {
                Self::decide_present(item_version, row, self.current)
            }
        }
    }

    fn decide_missing(
        item_version: storage_types::ItemRevision,
        current: Option<super::GsiProjection<super::GsiProjectionKey>>,
    ) -> GsiBuildProjectionChange {
        let Some(current) = current else {
            return GsiBuildProjectionChange::Replace {
                remove: None,
                write: None,
                item_version,
            };
        };
        match item_version.cmp(&current.item_version) {
            std::cmp::Ordering::Less => GsiBuildProjectionChange::IgnoreStale,
            std::cmp::Ordering::Equal if current.projection.is_none() => {
                GsiBuildProjectionChange::Replay
            }
            std::cmp::Ordering::Equal => {
                GsiBuildProjectionChange::FailClosed(GsiFailureReason::ProjectionConflict)
            }
            std::cmp::Ordering::Greater => GsiBuildProjectionChange::Replace {
                remove: current.projection.map(Box::new),
                write: None,
                item_version,
            },
        }
    }

    fn decide_present(
        item_version: storage_types::ItemRevision,
        row: Box<super::GsiProjectedRow>,
        current: Option<super::GsiProjection<super::GsiProjectionKey>>,
    ) -> GsiBuildProjectionChange {
        let Some(current) = current else {
            return GsiBuildProjectionChange::Replace {
                remove: None,
                write: Some(row),
                item_version,
            };
        };
        match item_version.cmp(&current.item_version) {
            std::cmp::Ordering::Less => GsiBuildProjectionChange::IgnoreStale,
            std::cmp::Ordering::Equal if current.projection.as_ref() == Some(&row.key) => {
                GsiBuildProjectionChange::Replay
            }
            std::cmp::Ordering::Equal => {
                GsiBuildProjectionChange::FailClosed(GsiFailureReason::ProjectionConflict)
            }
            std::cmp::Ordering::Greater => GsiBuildProjectionChange::Replace {
                remove: current.projection.map(Box::new),
                write: Some(row),
                item_version,
            },
        }
    }
}
