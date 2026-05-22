#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncCrashBoundary, SyncCrashRecoveryDecision, SyncCrashRecoveryGate, plan_crash_recovery,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtCrashRecoveryGate {
    boundary: String,
    #[serde(rename = "entryCommitted")]
    entry_committed: bool,
    #[serde(rename = "applyDurable")]
    apply_durable: bool,
    #[serde(rename = "responseSent")]
    response_sent: bool,
    #[serde(rename = "catchupCheckpointDurable")]
    catchup_checkpoint_durable: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncCrashRecoveryState {
    #[serde(rename = "lastGate")]
    last_gate: MbtCrashRecoveryGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncCrashRecoveryDriver> for SyncCrashRecoveryState {
    fn from_driver(driver: &SyncCrashRecoveryDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncCrashRecoveryDriver {
    last_gate: MbtCrashRecoveryGate,
    last_decision: String,
}

impl Default for SyncCrashRecoveryDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtCrashRecoveryGate {
                boundary: "after_admission_before_append".to_string(),
                entry_committed: false,
                apply_durable: false,
                response_sent: false,
                catchup_checkpoint_durable: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncCrashRecoveryDriver {
    type State = SyncCrashRecoveryState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                boundary: String,
                entryCommitted: bool,
                applyDurable: bool,
                responseSent: bool,
                catchupCheckpointDurable: bool,
            ) => {
                self.check(MbtCrashRecoveryGate {
                    boundary,
                    entry_committed: entryCommitted,
                    apply_durable: applyDurable,
                    response_sent: responseSent,
                    catchup_checkpoint_durable: catchupCheckpointDurable,
                });
            },
            step(
                boundary: String?,
                entryCommitted: bool?,
                applyDurable: bool?,
                responseSent: bool?,
                catchupCheckpointDurable: bool?,
            ) => {
                if let (
                    Some(boundary),
                    Some(entry_committed),
                    Some(apply_durable),
                    Some(response_sent),
                    Some(catchup_checkpoint_durable),
                ) = (
                    boundary,
                    entryCommitted,
                    applyDurable,
                    responseSent,
                    catchupCheckpointDurable,
                ) {
                    self.check(MbtCrashRecoveryGate {
                        boundary,
                        entry_committed,
                        apply_durable,
                        response_sent,
                        catchup_checkpoint_durable,
                    });
                }
            },
        })
    }
}

impl SyncCrashRecoveryDriver {
    fn check(&mut self, gate: MbtCrashRecoveryGate) {
        let decision = plan_crash_recovery(SyncCrashRecoveryGate {
            boundary: crash_boundary(&gate.boundary),
            entry_committed: gate.entry_committed,
            apply_durable: gate.apply_durable,
            response_sent: gate.response_sent,
            catchup_checkpoint_durable: gate.catchup_checkpoint_durable,
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn crash_boundary(name: &str) -> SyncCrashBoundary {
    match name {
        "after_admission_before_append" => SyncCrashBoundary::AfterAdmissionBeforeAppend,
        "after_raft_append_before_apply" => SyncCrashBoundary::AfterRaftAppendBeforeApply,
        "after_apply_before_response" => SyncCrashBoundary::AfterApplyBeforeResponse,
        "follower_crash_during_catchup" => SyncCrashBoundary::FollowerCrashDuringCatchup,
        _ => SyncCrashBoundary::AfterAdmissionBeforeAppend,
    }
}

fn decision_name(decision: SyncCrashRecoveryDecision) -> &'static str {
    match decision {
        SyncCrashRecoveryDecision::RetryNoAcknowledgement => "retry_no_acknowledgement",
        SyncCrashRecoveryDecision::ReplayCommittedEntry => "replay_committed_entry",
        SyncCrashRecoveryDecision::ReturnDurableResponse => "return_durable_response",
        SyncCrashRecoveryDecision::ResumeCatchupFromCheckpoint => "resume_catchup_from_checkpoint",
        SyncCrashRecoveryDecision::RestartCatchupChunkIdempotently => {
            "restart_catchup_chunk_idempotently"
        }
    }
}

#[quint_run(
    spec = "../../quint/sync_crash_recovery_mbt.qnt",
    max_samples = 96,
    max_steps = 8,
    seed = "0xc4a5"
)]
fn sync_crash_recovery_mbt_matches_rust_boundary() -> impl Driver {
    SyncCrashRecoveryDriver::default()
}
