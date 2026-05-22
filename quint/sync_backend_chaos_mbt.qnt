/// Compact MBT surface for sync-replication backend durability chaos.
///
/// The model keeps backend-specific durability vocabulary strict: local engines
/// with fsync-like control need direct durable commit evidence, externally
/// durable engines use their backend contract, conflict faults retry only within
/// budget, slow commits do not acknowledge early, and failed reopen fails closed.
module sync_backend_chaos_mbt {
  type Gate = {
    backend: str,
    fault: str,
    durableCommitConfigured: bool,
    conflictRetryBudgetRemaining: int,
    commitCompleted: bool,
    reopenSucceeded: bool,
  }

  var lastGate: Gate
  var lastDecision: str

  pure def localFsyncSurface(backend) =
    backend == "sqlite" or backend == "rocksdb" or backend == "turso"

  pure def durableWriteDecision(gate) =
    if (not(gate.durableCommitConfigured)) {
      "unsupported_fault_for_backend"
    } else if (localFsyncSurface(gate.backend)) {
      "require_durable_commit_evidence"
    } else {
      "use_backend_durability_contract"
    }

  pure def expectedDecision(gate) =
    if (gate.fault == "fsync_heavy_write") {
      durableWriteDecision(gate)
    } else if (gate.fault == "transaction_conflict") {
      if (gate.conflictRetryBudgetRemaining > 0) {
        "retry_transaction_conflict"
      } else {
        "reject_after_retry_budget"
      }
    } else if (gate.fault == "slow_commit") {
      if (gate.commitCompleted) {
        durableWriteDecision(gate)
      } else {
        "withhold_ack_until_commit_completes"
      }
    } else {
      if (gate.reopenSucceeded) {
        durableWriteDecision(gate)
      } else {
        "fail_closed_on_reopen"
      }
    }

  action init = all {
    lastGate' = {
      backend: "sqlite",
      fault: "fsync_heavy_write",
      durableCommitConfigured: true,
      conflictRetryBudgetRemaining: 1,
      commitCompleted: true,
      reopenSucceeded: true,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet backend = Set("sqlite", "rocksdb", "postgres", "foundationdb", "turso").oneOf()
    nondet fault = Set("fsync_heavy_write", "transaction_conflict", "slow_commit", "failed_reopen").oneOf()
    nondet durableCommitConfigured = Set(false, true).oneOf()
    nondet conflictRetryBudgetRemaining = Set(0, 1, 2).oneOf()
    nondet commitCompleted = Set(false, true).oneOf()
    nondet reopenSucceeded = Set(false, true).oneOf()
    val gate = {
      backend: backend,
      fault: fault,
      durableCommitConfigured: durableCommitConfigured,
      conflictRetryBudgetRemaining: conflictRetryBudgetRemaining,
      commitCompleted: commitCompleted,
      reopenSucceeded: reopenSucceeded,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "require_durable_commit_evidence" or localFsyncSurface(lastGate.backend),
      lastDecision != "use_backend_durability_contract" or not(localFsyncSurface(lastGate.backend)),
      lastDecision != "retry_transaction_conflict" or lastGate.conflictRetryBudgetRemaining > 0,
      lastDecision != "withhold_ack_until_commit_completes" or not(lastGate.commitCompleted),
      lastDecision != "fail_closed_on_reopen" or not(lastGate.reopenSucceeded),
    }

  run sqliteFsyncRequiresEvidence =
    true.expect(expectedDecision({
      backend: "sqlite",
      fault: "fsync_heavy_write",
      durableCommitConfigured: true,
      conflictRetryBudgetRemaining: 1,
      commitCompleted: true,
      reopenSucceeded: true,
    }) == "require_durable_commit_evidence")

  run foundationdbUsesDurabilityContract =
    true.expect(expectedDecision({
      backend: "foundationdb",
      fault: "fsync_heavy_write",
      durableCommitConfigured: true,
      conflictRetryBudgetRemaining: 1,
      commitCompleted: true,
      reopenSucceeded: true,
    }) == "use_backend_durability_contract")

  run conflictRejectsWhenRetryBudgetIsExhausted =
    true.expect(expectedDecision({
      backend: "postgres",
      fault: "transaction_conflict",
      durableCommitConfigured: true,
      conflictRetryBudgetRemaining: 0,
      commitCompleted: true,
      reopenSucceeded: true,
    }) == "reject_after_retry_budget")

  run slowCommitDoesNotAcknowledgeEarly =
    true.expect(expectedDecision({
      backend: "rocksdb",
      fault: "slow_commit",
      durableCommitConfigured: true,
      conflictRetryBudgetRemaining: 1,
      commitCompleted: false,
      reopenSucceeded: true,
    }) == "withhold_ack_until_commit_completes")

  run failedReopenFailsClosed =
    true.expect(expectedDecision({
      backend: "turso",
      fault: "failed_reopen",
      durableCommitConfigured: true,
      conflictRetryBudgetRemaining: 1,
      commitCompleted: true,
      reopenSucceeded: false,
    }) == "fail_closed_on_reopen")
}
