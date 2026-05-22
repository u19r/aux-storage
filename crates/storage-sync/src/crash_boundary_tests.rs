use crate::{
    SyncCrashBoundary, SyncCrashRecoveryDecision, SyncCrashRecoveryGate, plan_crash_recovery,
};

#[test]
fn crash_after_admission_before_append_requires_retry_without_acknowledgement() {
    assert_eq!(
        plan_crash_recovery(gate(SyncCrashBoundary::AfterAdmissionBeforeAppend)),
        SyncCrashRecoveryDecision::RetryNoAcknowledgement
    );
}

#[test]
fn crash_after_raft_append_replays_only_committed_entries() {
    let mut committed = gate(SyncCrashBoundary::AfterRaftAppendBeforeApply);
    committed.entry_committed = true;
    assert_eq!(
        plan_crash_recovery(committed),
        SyncCrashRecoveryDecision::ReplayCommittedEntry
    );

    let mut uncommitted = gate(SyncCrashBoundary::AfterRaftAppendBeforeApply);
    uncommitted.entry_committed = false;
    assert_eq!(
        plan_crash_recovery(uncommitted),
        SyncCrashRecoveryDecision::RetryNoAcknowledgement
    );
}

#[test]
fn crash_after_apply_before_response_uses_durable_response_or_replay() {
    let mut durable = gate(SyncCrashBoundary::AfterApplyBeforeResponse);
    durable.entry_committed = true;
    durable.apply_durable = true;
    assert_eq!(
        plan_crash_recovery(durable),
        SyncCrashRecoveryDecision::ReturnDurableResponse
    );

    let mut not_durable = gate(SyncCrashBoundary::AfterApplyBeforeResponse);
    not_durable.entry_committed = true;
    not_durable.apply_durable = false;
    assert_eq!(
        plan_crash_recovery(not_durable),
        SyncCrashRecoveryDecision::ReplayCommittedEntry
    );
}

#[test]
fn follower_crash_during_catchup_resumes_checkpoint_or_replays_chunk() {
    let mut checkpointed = gate(SyncCrashBoundary::FollowerCrashDuringCatchup);
    checkpointed.catchup_checkpoint_durable = true;
    assert_eq!(
        plan_crash_recovery(checkpointed),
        SyncCrashRecoveryDecision::ResumeCatchupFromCheckpoint
    );

    let mut uncheckpointed = gate(SyncCrashBoundary::FollowerCrashDuringCatchup);
    uncheckpointed.catchup_checkpoint_durable = false;
    assert_eq!(
        plan_crash_recovery(uncheckpointed),
        SyncCrashRecoveryDecision::RestartCatchupChunkIdempotently
    );
}

fn gate(boundary: SyncCrashBoundary) -> SyncCrashRecoveryGate {
    SyncCrashRecoveryGate {
        boundary,
        entry_committed: false,
        apply_durable: false,
        response_sent: false,
        catchup_checkpoint_durable: false,
    }
}
