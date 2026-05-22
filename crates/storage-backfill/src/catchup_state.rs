use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{GsiCatchupApplyCase, GsiCatchupOutcome, plan_gsi_catchup_apply};

pub trait CatchupApplyAdapter {
    type Error;

    fn apply_scan_observation(
        &mut self,
        apply_case: &GsiCatchupApplyCase,
    ) -> Result<GsiCatchupOutcome, Self::Error>;

    fn apply_stream_record(
        &mut self,
        checkpoint: &StreamDrainCheckpoint,
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtectedStreamBoundary(String);

impl ProtectedStreamBoundary {
    pub fn new(value: impl Into<String>) -> Result<Self, CatchupStateError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CatchupStateError::EmptyBoundary);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<ProtectedStreamBoundary> for String {
    fn from(value: ProtectedStreamBoundary) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamDrainCheckpoint(String);

impl StreamDrainCheckpoint {
    pub fn new(value: impl Into<String>) -> Result<Self, CatchupStateError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CatchupStateError::EmptyCheckpoint);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<StreamDrainCheckpoint> for String {
    fn from(value: StreamDrainCheckpoint) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatchupState {
    pub session: CatchupSessionState,
    pub protected_boundary: ProtectedBoundaryState,
    pub scan: ScanState,
    pub stream_drain: StreamDrainState,
    pub completion: CompletionState,
    pub cleanup: CleanupState,
}

impl CatchupState {
    #[must_use]
    pub const fn pending() -> Self {
        Self {
            session: CatchupSessionState::Pending,
            protected_boundary: ProtectedBoundaryState::Uncaptured,
            scan: ScanState::NotStarted,
            stream_drain: StreamDrainState::NotStarted,
            completion: CompletionState::Inactive,
            cleanup: CleanupState::NotStarted,
        }
    }

    pub fn capture_boundary(
        &mut self,
        stream_tail: impl Into<String>,
    ) -> Result<(), CatchupStateError> {
        if !matches!(self.protected_boundary, ProtectedBoundaryState::Uncaptured) {
            return Err(CatchupStateError::BoundaryAlreadyCaptured);
        }
        let stream_tail = ProtectedStreamBoundary::new(stream_tail)?;
        self.protected_boundary = ProtectedBoundaryState::Captured {
            stream_tail: stream_tail.into(),
        };
        self.session = CatchupSessionState::BoundaryCaptured;
        Ok(())
    }

    pub fn protect_stream_boundary(&mut self) -> Result<(), CatchupStateError> {
        let ProtectedBoundaryState::Captured { stream_tail } = &self.protected_boundary else {
            return Err(CatchupStateError::BoundaryNotCaptured);
        };
        self.protected_boundary = ProtectedBoundaryState::Protected {
            stream_tail: stream_tail.clone(),
        };
        self.session = CatchupSessionState::BoundaryProtected;
        Ok(())
    }

    pub fn apply_scan_observation(
        &mut self,
        apply_case: &GsiCatchupApplyCase,
    ) -> Result<GsiCatchupOutcome, CatchupStateError> {
        if !matches!(
            self.protected_boundary,
            ProtectedBoundaryState::Protected { .. }
        ) {
            return Err(CatchupStateError::BoundaryNotProtected);
        }
        if matches!(self.scan, ScanState::Complete) {
            return Err(CatchupStateError::ScanAlreadyComplete);
        }
        self.scan = ScanState::Scanning {
            scan_lek: None,
            observations_applied: self.scan.observations_applied().saturating_add(1),
        };
        Ok(plan_gsi_catchup_apply(apply_case))
    }

    pub fn mark_scan_complete(&mut self) -> Result<(), CatchupStateError> {
        if !matches!(self.scan, ScanState::Scanning { .. }) {
            return Err(CatchupStateError::ScanNotStarted);
        }
        self.scan = ScanState::Complete;
        self.session = CatchupSessionState::ScanComplete;
        Ok(())
    }

    pub fn apply_stream_record(
        &mut self,
        checkpoint: impl Into<String>,
    ) -> Result<(), CatchupStateError> {
        if !matches!(self.scan, ScanState::Complete) {
            return Err(CatchupStateError::ScanNotComplete);
        }
        let checkpoint = StreamDrainCheckpoint::new(checkpoint)?;
        self.stream_drain = StreamDrainState::Draining {
            checkpoint: Some(checkpoint.into()),
            records_applied: self.stream_drain.records_applied().saturating_add(1),
        };
        self.session = CatchupSessionState::DrainingStream;
        Ok(())
    }

    pub fn mark_stream_drained(&mut self) -> Result<(), CatchupStateError> {
        if !matches!(self.stream_drain, StreamDrainState::Draining { .. }) {
            return Err(CatchupStateError::StreamDrainNotStarted);
        }
        self.stream_drain = StreamDrainState::Complete;
        self.session = CatchupSessionState::StreamDrained;
        Ok(())
    }

    pub fn activate(&mut self) -> Result<GsiCatchupOutcome, CatchupStateError> {
        if !matches!(self.scan, ScanState::Complete) {
            return Err(CatchupStateError::ScanNotComplete);
        }
        if !matches!(self.stream_drain, StreamDrainState::Complete) {
            return Err(CatchupStateError::StreamDrainNotComplete);
        }
        self.completion = CompletionState::Active;
        self.session = CatchupSessionState::Active;
        Ok(GsiCatchupOutcome::ActivationAllowed)
    }

    pub fn release_stream_boundary(&mut self) -> Result<(), CatchupStateError> {
        if !matches!(self.completion, CompletionState::Active) {
            return Err(CatchupStateError::NotActive);
        }
        self.protected_boundary = ProtectedBoundaryState::Released;
        Ok(())
    }

    pub fn begin_cleanup(&mut self) -> Result<(), CatchupStateError> {
        if !matches!(self.completion, CompletionState::Active) {
            return Err(CatchupStateError::NotActive);
        }
        if matches!(self.cleanup, CleanupState::Complete) {
            return Err(CatchupStateError::CleanupAlreadyComplete);
        }
        self.cleanup = CleanupState::Cleaning;
        Ok(())
    }

    pub fn finish_cleanup(&mut self) -> Result<(), CatchupStateError> {
        if !matches!(self.cleanup, CleanupState::Cleaning) {
            return Err(CatchupStateError::CleanupNotStarted);
        }
        self.cleanup = CleanupState::Complete;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CatchupSessionState {
    #[default]
    Pending,
    BoundaryCaptured,
    BoundaryProtected,
    ScanComplete,
    DrainingStream,
    StreamDrained,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtectedBoundaryState {
    #[default]
    Uncaptured,
    Captured {
        stream_tail: String,
    },
    Protected {
        stream_tail: String,
    },
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScanState {
    #[default]
    NotStarted,
    Scanning {
        scan_lek: Option<String>,
        observations_applied: usize,
    },
    Complete,
}

impl ScanState {
    const fn observations_applied(&self) -> usize {
        match self {
            Self::Scanning {
                observations_applied,
                ..
            } => *observations_applied,
            Self::NotStarted | Self::Complete => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamDrainState {
    #[default]
    NotStarted,
    Draining {
        checkpoint: Option<String>,
        records_applied: usize,
    },
    Complete,
}

impl StreamDrainState {
    const fn records_applied(&self) -> usize {
        match self {
            Self::Draining {
                records_applied, ..
            } => *records_applied,
            Self::NotStarted | Self::Complete => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CompletionState {
    #[default]
    Inactive,
    Active,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CleanupState {
    #[default]
    NotStarted,
    Cleaning,
    Complete,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CatchupStateError {
    #[error("boundary already captured")]
    BoundaryAlreadyCaptured,
    #[error("boundary cannot be empty")]
    EmptyBoundary,
    #[error("boundary not captured")]
    BoundaryNotCaptured,
    #[error("boundary not protected")]
    BoundaryNotProtected,
    #[error("scan not started")]
    ScanNotStarted,
    #[error("scan already complete")]
    ScanAlreadyComplete,
    #[error("scan not complete")]
    ScanNotComplete,
    #[error("stream drain not started")]
    StreamDrainNotStarted,
    #[error("stream drain checkpoint cannot be empty")]
    EmptyCheckpoint,
    #[error("stream drain not complete")]
    StreamDrainNotComplete,
    #[error("catch-up is not active")]
    NotActive,
    #[error("cleanup not started")]
    CleanupNotStarted,
    #[error("cleanup already complete")]
    CleanupAlreadyComplete,
}
