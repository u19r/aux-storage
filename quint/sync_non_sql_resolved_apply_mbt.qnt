/// MBT surface for non-SQL resolved sync apply readiness.
///
/// Phase 8 must not promote a validation-only non-SQL learner unless the backend
/// can apply every committed storage side effect from resolved log entries and
/// can replay entries idempotently after restart.
module sync_non_sql_resolved_apply_mbt {
  type ApplyGate = {
    backendDecision: str,
    destinationBackend: str,
    tableLifecycleApply: bool,
    itemPutDeleteApply: bool,
    durableRevisionApply: bool,
    streamApply: bool,
    ttlApply: bool,
    gsiApply: bool,
    syncControlPlaneApply: bool,
    logEntryPersistence: bool,
    replayIdempotency: bool,
  }

  var lastGate: ApplyGate
  var lastDecision: str

  pure def isNonSqlBackend(backend) =
    Set("rocksdb", "foundationdb", "postgres", "turso").contains(backend)

  pure def expectedDecision(gate) =
    if (gate.backendDecision == "rejected") {
      "backend_pair_rejected"
    } else if (not(isNonSqlBackend(gate.destinationBackend))) {
      "destination_not_non_sql"
    } else if (not(gate.tableLifecycleApply)) {
      "table_lifecycle_apply_missing"
    } else if (not(gate.itemPutDeleteApply)) {
      "item_put_delete_apply_missing"
    } else if (not(gate.durableRevisionApply)) {
      "durable_revision_apply_missing"
    } else if (not(gate.streamApply)) {
      "stream_apply_missing"
    } else if (not(gate.ttlApply)) {
      "ttl_apply_missing"
    } else if (not(gate.gsiApply)) {
      "gsi_apply_missing"
    } else if (not(gate.syncControlPlaneApply)) {
      "sync_control_plane_apply_missing"
    } else if (not(gate.logEntryPersistence)) {
      "log_entry_persistence_missing"
    } else if (not(gate.replayIdempotency)) {
      "replay_idempotency_missing"
    } else {
      "allow"
    }

  action init = all {
    lastGate' = {
      backendDecision: "rejected",
      destinationBackend: "",
      tableLifecycleApply: false,
      itemPutDeleteApply: false,
      durableRevisionApply: false,
      streamApply: false,
      ttlApply: false,
      gsiApply: false,
      syncControlPlaneApply: false,
      logEntryPersistence: false,
      replayIdempotency: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet backendDecision = Set("production_supported", "validation_only", "rejected").oneOf()
    nondet destinationBackend = Set("", "sqlite", "rocksdb", "foundationdb", "postgres", "turso", "remote").oneOf()
    nondet tableLifecycleApply = Set(false, true).oneOf()
    nondet itemPutDeleteApply = Set(false, true).oneOf()
    nondet durableRevisionApply = Set(false, true).oneOf()
    nondet streamApply = Set(false, true).oneOf()
    nondet ttlApply = Set(false, true).oneOf()
    nondet gsiApply = Set(false, true).oneOf()
    nondet syncControlPlaneApply = Set(false, true).oneOf()
    nondet logEntryPersistence = Set(false, true).oneOf()
    nondet replayIdempotency = Set(false, true).oneOf()
    val gate = {
      backendDecision: backendDecision,
      destinationBackend: destinationBackend,
      tableLifecycleApply: tableLifecycleApply,
      itemPutDeleteApply: itemPutDeleteApply,
      durableRevisionApply: durableRevisionApply,
      streamApply: streamApply,
      ttlApply: ttlApply,
      gsiApply: gsiApply,
      syncControlPlaneApply: syncControlPlaneApply,
      logEntryPersistence: logEntryPersistence,
      replayIdempotency: replayIdempotency,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "allow"
      or all {
        lastGate.backendDecision != "rejected",
        isNonSqlBackend(lastGate.destinationBackend),
        lastGate.tableLifecycleApply,
        lastGate.itemPutDeleteApply,
        lastGate.durableRevisionApply,
        lastGate.streamApply,
        lastGate.ttlApply,
        lastGate.gsiApply,
        lastGate.syncControlPlaneApply,
        lastGate.logEntryPersistence,
        lastGate.replayIdempotency,
      }

  run rocksdbCanApplyAfterFullSurface =
    true.expect(expectedDecision({
      backendDecision: "validation_only",
      destinationBackend: "rocksdb",
      tableLifecycleApply: true,
      itemPutDeleteApply: true,
      durableRevisionApply: true,
      streamApply: true,
      ttlApply: true,
      gsiApply: true,
      syncControlPlaneApply: true,
      logEntryPersistence: true,
      replayIdempotency: true,
    }) == "allow")

  run sqliteIsNotNonSqlPhase8Destination =
    true.expect(expectedDecision({
      backendDecision: "production_supported",
      destinationBackend: "sqlite",
      tableLifecycleApply: true,
      itemPutDeleteApply: true,
      durableRevisionApply: true,
      streamApply: true,
      ttlApply: true,
      gsiApply: true,
      syncControlPlaneApply: true,
      logEntryPersistence: true,
      replayIdempotency: true,
    }) == "destination_not_non_sql")

  run replayIdempotencyIsRequired =
    true.expect(expectedDecision({
      backendDecision: "validation_only",
      destinationBackend: "rocksdb",
      tableLifecycleApply: true,
      itemPutDeleteApply: true,
      durableRevisionApply: true,
      streamApply: true,
      ttlApply: true,
      gsiApply: true,
      syncControlPlaneApply: true,
      logEntryPersistence: true,
      replayIdempotency: false,
    }) == "replay_idempotency_missing")
}
