use serde::{Deserialize, Serialize};

/// Compact GSI catch-up apply input shared with the Quint MBT model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GsiCatchupApplyCase {
    pub current_version: i64,
    pub observation_version: i64,
    pub current_projects: bool,
    pub observation_projects: bool,
    pub history_available: bool,
    pub scan_complete: bool,
    pub drain_complete: bool,
}

/// Rust-visible GSI catch-up decision result.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GsiCatchupOutcome {
    RejectedMissingHistory,
    RejectedStaleObservation,
    ActivationAllowed,
    AppliedProjection,
    AppliedTombstone,
}

impl GsiCatchupOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RejectedMissingHistory => "rejected_missing_history",
            Self::RejectedStaleObservation => "rejected_stale_observation",
            Self::ActivationAllowed => "activation_allowed",
            Self::AppliedProjection => "applied_projection",
            Self::AppliedTombstone => "applied_tombstone",
        }
    }
}

#[must_use]
pub const fn plan_gsi_catchup_apply(apply_case: &GsiCatchupApplyCase) -> GsiCatchupOutcome {
    if !apply_case.history_available {
        GsiCatchupOutcome::RejectedMissingHistory
    } else if apply_case.observation_version < apply_case.current_version {
        GsiCatchupOutcome::RejectedStaleObservation
    } else if apply_case.scan_complete && apply_case.drain_complete {
        GsiCatchupOutcome::ActivationAllowed
    } else if apply_case.observation_projects {
        GsiCatchupOutcome::AppliedProjection
    } else {
        GsiCatchupOutcome::AppliedTombstone
    }
}
