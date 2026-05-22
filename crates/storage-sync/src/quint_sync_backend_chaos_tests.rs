#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncBackendChaosBackend, SyncBackendChaosDecision, SyncBackendChaosFault, SyncBackendChaosGate,
    plan_backend_chaos,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtBackendChaosGate {
    backend: String,
    fault: String,
    #[serde(rename = "durableCommitConfigured")]
    durable_commit_configured: bool,
    #[serde(rename = "conflictRetryBudgetRemaining")]
    conflict_retry_budget_remaining: u8,
    #[serde(rename = "commitCompleted")]
    commit_completed: bool,
    #[serde(rename = "reopenSucceeded")]
    reopen_succeeded: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncBackendChaosState {
    #[serde(rename = "lastGate")]
    last_gate: MbtBackendChaosGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncBackendChaosDriver> for SyncBackendChaosState {
    fn from_driver(driver: &SyncBackendChaosDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncBackendChaosDriver {
    last_gate: MbtBackendChaosGate,
    last_decision: String,
}

impl Default for SyncBackendChaosDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtBackendChaosGate {
                backend: "sqlite".to_string(),
                fault: "fsync_heavy_write".to_string(),
                durable_commit_configured: true,
                conflict_retry_budget_remaining: 1,
                commit_completed: true,
                reopen_succeeded: true,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncBackendChaosDriver {
    type State = SyncBackendChaosState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                backend: String,
                fault: String,
                durableCommitConfigured: bool,
                conflictRetryBudgetRemaining: u8,
                commitCompleted: bool,
                reopenSucceeded: bool,
            ) => {
                self.check(MbtBackendChaosGate {
                    backend,
                    fault,
                    durable_commit_configured: durableCommitConfigured,
                    conflict_retry_budget_remaining: conflictRetryBudgetRemaining,
                    commit_completed: commitCompleted,
                    reopen_succeeded: reopenSucceeded,
                });
            },
            step(
                backend: String?,
                fault: String?,
                durableCommitConfigured: bool?,
                conflictRetryBudgetRemaining: u8?,
                commitCompleted: bool?,
                reopenSucceeded: bool?,
            ) => {
                if let (
                    Some(backend),
                    Some(fault),
                    Some(durable_commit_configured),
                    Some(conflict_retry_budget_remaining),
                    Some(commit_completed),
                    Some(reopen_succeeded),
                ) = (
                    backend,
                    fault,
                    durableCommitConfigured,
                    conflictRetryBudgetRemaining,
                    commitCompleted,
                    reopenSucceeded,
                ) {
                    self.check(MbtBackendChaosGate {
                        backend,
                        fault,
                        durable_commit_configured,
                        conflict_retry_budget_remaining,
                        commit_completed,
                        reopen_succeeded,
                    });
                }
            },
        })
    }
}

impl SyncBackendChaosDriver {
    fn check(&mut self, gate: MbtBackendChaosGate) {
        let decision = plan_backend_chaos(SyncBackendChaosGate {
            backend: backend(&gate.backend),
            fault: fault(&gate.fault),
            durable_commit_configured: gate.durable_commit_configured,
            conflict_retry_budget_remaining: gate.conflict_retry_budget_remaining,
            commit_completed: gate.commit_completed,
            reopen_succeeded: gate.reopen_succeeded,
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn backend(name: &str) -> SyncBackendChaosBackend {
    match name {
        "sqlite" => SyncBackendChaosBackend::Sqlite,
        "rocksdb" => SyncBackendChaosBackend::Rocksdb,
        "postgres" => SyncBackendChaosBackend::Postgres,
        "foundationdb" => SyncBackendChaosBackend::Foundationdb,
        "turso" => SyncBackendChaosBackend::Turso,
        _ => SyncBackendChaosBackend::Sqlite,
    }
}

fn fault(name: &str) -> SyncBackendChaosFault {
    match name {
        "fsync_heavy_write" => SyncBackendChaosFault::FsyncHeavyWrite,
        "transaction_conflict" => SyncBackendChaosFault::TransactionConflict,
        "slow_commit" => SyncBackendChaosFault::SlowCommit,
        "failed_reopen" => SyncBackendChaosFault::FailedReopen,
        _ => SyncBackendChaosFault::FsyncHeavyWrite,
    }
}

fn decision_name(decision: SyncBackendChaosDecision) -> &'static str {
    match decision {
        SyncBackendChaosDecision::RequireDurableCommitEvidence => "require_durable_commit_evidence",
        SyncBackendChaosDecision::UseBackendDurabilityContract => "use_backend_durability_contract",
        SyncBackendChaosDecision::RetryTransactionConflict => "retry_transaction_conflict",
        SyncBackendChaosDecision::RejectAfterRetryBudget => "reject_after_retry_budget",
        SyncBackendChaosDecision::WithholdAckUntilCommitCompletes => {
            "withhold_ack_until_commit_completes"
        }
        SyncBackendChaosDecision::FailClosedOnReopen => "fail_closed_on_reopen",
        SyncBackendChaosDecision::UnsupportedFaultForBackend => "unsupported_fault_for_backend",
    }
}

#[quint_run(
    spec = "../../quint/sync_backend_chaos_mbt.qnt",
    max_samples = 120,
    max_steps = 8,
    seed = "0xbac0"
)]
fn sync_backend_chaos_mbt_matches_rust_boundary() -> impl Driver {
    SyncBackendChaosDriver::default()
}
