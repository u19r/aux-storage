#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncCrashBoundary {
    AfterAdmissionBeforeAppend,
    AfterRaftAppendBeforeApply,
    AfterApplyBeforeResponse,
    FollowerCrashDuringCatchup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncCrashRecoveryGate {
    pub boundary: SyncCrashBoundary,
    pub entry_committed: bool,
    pub apply_durable: bool,
    pub response_sent: bool,
    pub catchup_checkpoint_durable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncCrashRecoveryDecision {
    RetryNoAcknowledgement,
    ReplayCommittedEntry,
    ReturnDurableResponse,
    ResumeCatchupFromCheckpoint,
    RestartCatchupChunkIdempotently,
}

#[must_use]
pub fn plan_crash_recovery(gate: SyncCrashRecoveryGate) -> SyncCrashRecoveryDecision {
    match gate.boundary {
        SyncCrashBoundary::AfterAdmissionBeforeAppend => {
            SyncCrashRecoveryDecision::RetryNoAcknowledgement
        }
        SyncCrashBoundary::AfterRaftAppendBeforeApply => {
            if gate.entry_committed {
                SyncCrashRecoveryDecision::ReplayCommittedEntry
            } else {
                SyncCrashRecoveryDecision::RetryNoAcknowledgement
            }
        }
        SyncCrashBoundary::AfterApplyBeforeResponse => {
            if gate.apply_durable {
                SyncCrashRecoveryDecision::ReturnDurableResponse
            } else if gate.entry_committed {
                SyncCrashRecoveryDecision::ReplayCommittedEntry
            } else {
                SyncCrashRecoveryDecision::RetryNoAcknowledgement
            }
        }
        SyncCrashBoundary::FollowerCrashDuringCatchup => {
            if gate.catchup_checkpoint_durable {
                SyncCrashRecoveryDecision::ResumeCatchupFromCheckpoint
            } else {
                SyncCrashRecoveryDecision::RestartCatchupChunkIdempotently
            }
        }
    }
}
