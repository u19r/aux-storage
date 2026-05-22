use crate::{
    CatchupState, CatchupStateError, GsiCatchupApplyCase, GsiCatchupOutcome, StreamDrainCheckpoint,
};

pub trait BackfillPolicy {
    type ScanObservation;
    type StreamRecord;
    type Projection;
    type KeyMapping;
    type TombstoneEvidence;

    fn policy_name(&self) -> &'static str;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GsiBackfillPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiScanObservation {
    pub apply_case: GsiCatchupApplyCase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiStreamRecord {
    pub checkpoint: StreamDrainCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiProjection {
    pub projects: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiKeyMapping {
    pub source_key: String,
    pub gsi_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiTombstoneEvidence {
    pub hidden: bool,
    pub isolated_from_query_prefix: bool,
}

impl BackfillPolicy for GsiBackfillPolicy {
    type ScanObservation = GsiScanObservation;
    type StreamRecord = GsiStreamRecord;
    type Projection = GsiProjection;
    type KeyMapping = GsiKeyMapping;
    type TombstoneEvidence = GsiTombstoneEvidence;

    fn policy_name(&self) -> &'static str {
        "gsi"
    }
}

#[derive(Debug, Clone)]
pub struct BackfillControl<P> {
    policy: P,
    state: CatchupState,
}

impl<P: BackfillPolicy> BackfillControl<P> {
    #[must_use]
    pub fn new(policy: P) -> Self {
        Self {
            policy,
            state: CatchupState::pending(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> &CatchupState {
        &self.state
    }

    #[must_use]
    pub fn policy_name(&self) -> &'static str {
        self.policy.policy_name()
    }

    pub fn capture_boundary(
        &mut self,
        stream_tail: impl Into<String>,
    ) -> Result<(), CatchupStateError> {
        self.state.capture_boundary(stream_tail)
    }

    pub fn protect_stream_boundary(&mut self) -> Result<(), CatchupStateError> {
        self.state.protect_stream_boundary()
    }

    pub fn mark_scan_complete(&mut self) -> Result<(), CatchupStateError> {
        self.state.mark_scan_complete()
    }

    pub fn mark_stream_drained(&mut self) -> Result<(), CatchupStateError> {
        self.state.mark_stream_drained()
    }

    pub fn activate(&mut self) -> Result<GsiCatchupOutcome, CatchupStateError> {
        self.state.activate()
    }

    pub fn begin_cleanup(&mut self) -> Result<(), CatchupStateError> {
        self.state.begin_cleanup()
    }

    pub fn finish_cleanup(&mut self) -> Result<(), CatchupStateError> {
        self.state.finish_cleanup()
    }
}

impl BackfillControl<GsiBackfillPolicy> {
    pub fn apply_scan_observation(
        &mut self,
        observation: &GsiScanObservation,
    ) -> Result<GsiCatchupOutcome, CatchupStateError> {
        self.state.apply_scan_observation(&observation.apply_case)
    }

    pub fn apply_stream_record(
        &mut self,
        record: &GsiStreamRecord,
    ) -> Result<(), CatchupStateError> {
        self.state.apply_stream_record(record.checkpoint.as_str())
    }
}
