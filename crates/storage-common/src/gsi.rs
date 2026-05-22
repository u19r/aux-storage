//! Shared GSI lifecycle types & constants (backend agnostic).
//! Keep logic minimal; orchestration remains backend-specific until unified.
use bg_jobs::{BackgroundJobName, DatabaseJobKind};
use serde::{Deserialize, Serialize};

/// Standard job names to avoid drift.
pub const GSI_UPDATE_JOB: BackgroundJobName = BackgroundJobName::Database {
    kind: DatabaseJobKind::GsiUpdate,
};
pub const GSI_BACKFILL_JOB: BackgroundJobName = BackgroundJobName::Database {
    kind: DatabaseJobKind::GsiBackfill,
};
pub const TTL_SWEEP_JOB: BackgroundJobName = BackgroundJobName::Database {
    kind: DatabaseJobKind::TtlSweep,
};
pub const STREAM_TRIM_JOB: BackgroundJobName = BackgroundJobName::Database {
    kind: DatabaseJobKind::StreamTrim,
};

/// Backfill state machine (can be extended later).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GsiBackfillPhase {
    /// Initial scan phase copying existing items.
    Backfilling,
    /// Catching up via captured stream tail.
    CatchingUp,
    /// Fully in sync; future maintenance via update job only.
    Done,
}

impl GsiBackfillPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done)
    }
}
