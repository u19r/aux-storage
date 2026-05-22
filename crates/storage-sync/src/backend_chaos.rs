#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncBackendChaosBackend {
    Sqlite,
    Rocksdb,
    Postgres,
    Foundationdb,
    Turso,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncBackendChaosFault {
    FsyncHeavyWrite,
    TransactionConflict,
    SlowCommit,
    FailedReopen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncBackendChaosGate {
    pub backend: SyncBackendChaosBackend,
    pub fault: SyncBackendChaosFault,
    pub durable_commit_configured: bool,
    pub conflict_retry_budget_remaining: u8,
    pub commit_completed: bool,
    pub reopen_succeeded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncBackendChaosDecision {
    RequireDurableCommitEvidence,
    UseBackendDurabilityContract,
    RetryTransactionConflict,
    RejectAfterRetryBudget,
    WithholdAckUntilCommitCompletes,
    FailClosedOnReopen,
    UnsupportedFaultForBackend,
}

#[must_use]
pub fn plan_backend_chaos(gate: SyncBackendChaosGate) -> SyncBackendChaosDecision {
    match gate.fault {
        SyncBackendChaosFault::FsyncHeavyWrite => durable_write_decision(gate),
        SyncBackendChaosFault::TransactionConflict => {
            if gate.conflict_retry_budget_remaining > 0 {
                SyncBackendChaosDecision::RetryTransactionConflict
            } else {
                SyncBackendChaosDecision::RejectAfterRetryBudget
            }
        }
        SyncBackendChaosFault::SlowCommit => {
            if gate.commit_completed {
                durable_write_decision(gate)
            } else {
                SyncBackendChaosDecision::WithholdAckUntilCommitCompletes
            }
        }
        SyncBackendChaosFault::FailedReopen => {
            if gate.reopen_succeeded {
                durable_write_decision(gate)
            } else {
                SyncBackendChaosDecision::FailClosedOnReopen
            }
        }
    }
}

fn durable_write_decision(gate: SyncBackendChaosGate) -> SyncBackendChaosDecision {
    if !gate.durable_commit_configured {
        return SyncBackendChaosDecision::UnsupportedFaultForBackend;
    }

    if backend_has_local_fsync_surface(gate.backend) {
        SyncBackendChaosDecision::RequireDurableCommitEvidence
    } else {
        SyncBackendChaosDecision::UseBackendDurabilityContract
    }
}

const fn backend_has_local_fsync_surface(backend: SyncBackendChaosBackend) -> bool {
    matches!(
        backend,
        SyncBackendChaosBackend::Sqlite
            | SyncBackendChaosBackend::Rocksdb
            | SyncBackendChaosBackend::Turso
    )
}
