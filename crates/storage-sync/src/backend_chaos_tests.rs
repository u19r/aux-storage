use crate::{
    SyncBackendChaosBackend, SyncBackendChaosDecision, SyncBackendChaosFault, SyncBackendChaosGate,
    plan_backend_chaos,
};

#[test]
fn local_fsync_backends_require_durable_commit_evidence() {
    for backend in [
        SyncBackendChaosBackend::Sqlite,
        SyncBackendChaosBackend::Rocksdb,
        SyncBackendChaosBackend::Turso,
    ] {
        assert_eq!(
            plan_backend_chaos(gate(backend, SyncBackendChaosFault::FsyncHeavyWrite)),
            SyncBackendChaosDecision::RequireDurableCommitEvidence
        );
    }
}

#[test]
fn external_durable_backends_use_backend_durability_contract() {
    for backend in [
        SyncBackendChaosBackend::Postgres,
        SyncBackendChaosBackend::Foundationdb,
    ] {
        assert_eq!(
            plan_backend_chaos(gate(backend, SyncBackendChaosFault::FsyncHeavyWrite)),
            SyncBackendChaosDecision::UseBackendDurabilityContract
        );
    }
}

#[test]
fn transaction_conflict_retries_until_budget_is_exhausted() {
    let mut retry = gate(
        SyncBackendChaosBackend::Sqlite,
        SyncBackendChaosFault::TransactionConflict,
    );
    retry.conflict_retry_budget_remaining = 1;
    assert_eq!(
        plan_backend_chaos(retry),
        SyncBackendChaosDecision::RetryTransactionConflict
    );

    let mut reject = retry;
    reject.conflict_retry_budget_remaining = 0;
    assert_eq!(
        plan_backend_chaos(reject),
        SyncBackendChaosDecision::RejectAfterRetryBudget
    );
}

#[test]
fn slow_commit_withholds_ack_until_commit_completes() {
    let mut slow = gate(
        SyncBackendChaosBackend::Rocksdb,
        SyncBackendChaosFault::SlowCommit,
    );
    slow.commit_completed = false;
    assert_eq!(
        plan_backend_chaos(slow),
        SyncBackendChaosDecision::WithholdAckUntilCommitCompletes
    );

    slow.commit_completed = true;
    assert_eq!(
        plan_backend_chaos(slow),
        SyncBackendChaosDecision::RequireDurableCommitEvidence
    );
}

#[test]
fn failed_reopen_fails_closed_before_serving_durable_responses() {
    let mut failed = gate(
        SyncBackendChaosBackend::Postgres,
        SyncBackendChaosFault::FailedReopen,
    );
    failed.reopen_succeeded = false;
    assert_eq!(
        plan_backend_chaos(failed),
        SyncBackendChaosDecision::FailClosedOnReopen
    );

    failed.reopen_succeeded = true;
    assert_eq!(
        plan_backend_chaos(failed),
        SyncBackendChaosDecision::UseBackendDurabilityContract
    );
}

#[test]
fn disabled_durable_commit_configuration_is_not_launch_safe() {
    let mut disabled = gate(
        SyncBackendChaosBackend::Sqlite,
        SyncBackendChaosFault::FsyncHeavyWrite,
    );
    disabled.durable_commit_configured = false;
    assert_eq!(
        plan_backend_chaos(disabled),
        SyncBackendChaosDecision::UnsupportedFaultForBackend
    );
}

fn gate(backend: SyncBackendChaosBackend, fault: SyncBackendChaosFault) -> SyncBackendChaosGate {
    SyncBackendChaosGate {
        backend,
        fault,
        durable_commit_configured: true,
        conflict_retry_budget_remaining: 1,
        commit_completed: true,
        reopen_succeeded: true,
    }
}
