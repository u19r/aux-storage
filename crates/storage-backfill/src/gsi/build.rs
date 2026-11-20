use storage_types::SystemStreamCheckpoint;

use super::{
    GsiActive, GsiBuild, GsiBuildCandidates, GsiBuildDiscovery, GsiChange, GsiLifecycleError,
    GsiNativeBuilding, GsiScanObservation, GsiScanPageEnd, GsiScanning, GsiState,
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
        if !build.state.is_runnable() {
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
    pub const fn is_runnable(&self) -> bool {
        !matches!(self, Self::Active(_))
    }
    pub const fn permits_queries(&self) -> bool {
        matches!(self, Self::Active(_))
    }
}

impl<Cursor> GsiBuild<Cursor> {
    pub fn new(target: super::GsiBuildTarget, low: SystemStreamCheckpoint) -> Self {
        Self {
            target,
            state: GsiState::Filling(GsiScanning { low, after: None }),
        }
    }
    pub fn new_active(target: super::GsiBuildTarget, cursor: SystemStreamCheckpoint) -> Self {
        Self {
            target,
            state: GsiState::Active(GsiActive { cursor }),
        }
    }
}

impl<Cursor: PartialEq> GsiBuild<Cursor> {
    pub fn advance_protected_page(
        &self,
        observation: GsiScanObservation<Cursor>,
    ) -> Result<GsiChange<Cursor>, GsiLifecycleError> {
        let GsiState::Filling(state) = &self.state else {
            return Err(GsiLifecycleError::InvalidPhase);
        };
        if state.after.as_ref() != observation.expected_after.as_ref() {
            return Err(GsiLifecycleError::ProgressConflict);
        }
        let next_state = match observation.end {
            GsiScanPageEnd::Continue(after) => GsiState::Filling(GsiScanning {
                low: state.low.clone(),
                after: Some(after),
            }),
            GsiScanPageEnd::Complete(()) => GsiState::Building(GsiNativeBuilding {
                low: state.low.clone(),
                retired: None,
            }),
        };
        Ok(GsiChange::Persist {
            projections: observation.projections,
            next_state,
        })
    }
}
