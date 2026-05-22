/// Shared logical backfill model for sync learner catchup and multi-region bootstrap.
///
/// The model focuses on the common scan-plus-stream reconciliation rule:
/// `ItemStreamVersion` is the only per-key ordering value, tombstones compare
/// with present records, duplicate chunks are idempotent, stale manifests fail
/// closed, activation requires a protected boundary plus final drain, and trim
/// cannot cross an active session's protected system-stream cursor.
module logical_backfill {
  pure val KEYS = Set("a", "b")
  pure val NONE_VERSION = 0
  pure val TOMBSTONE = false
  pure val PRESENT = true

  var sourceVersion: str -> int
  var sourcePresent: str -> bool
  var destinationVersion: str -> int
  var destinationPresent: str -> bool
  var protectedCursor: int
  var sourceCursor: int
  var trimCursor: int
  var boundaryCaptured: bool
  var scanComplete: bool
  var drainComplete: bool
  var importActive: bool
  var activated: bool
  var cleanupStarted: bool
  var cleanupComplete: bool
  var sessionFailedClosed: bool
  var sessionRestarted: bool
  var lastOutcome: str

  pure def initialVersions = Map("a" -> NONE_VERSION, "b" -> NONE_VERSION)
  pure def initialPresent = Map("a" -> false, "b" -> false)

  pure def nextVersionFor(versions, key) =
    versions.get(key) + 1

  pure def maxCursor(left, right) =
    if (left > right) left else right

  pure def trimFloorFor(importing, boundaryHeld, protected, sourceTail) =
    if (importing and boundaryHeld and protected > NONE_VERSION)
      protected - 1
    else
      sourceTail

  pure def applyLogicalRecord(destVersion, destPresent, key, version, present) =
    if (version > destVersion.get(key))
      {
        version: destVersion.set(key, version),
        present: destPresent.set(key, present),
        outcome: if (present) "applied_present_record" else "applied_tombstone",
      }
    else if (version == destVersion.get(key))
      {
        version: destVersion,
        present: destPresent,
        outcome: "duplicate_chunk_idempotent",
      }
    else
      {
        version: destVersion,
        present: destPresent,
        outcome: "rejected_stale_record",
      }

  pure def canActivate(importing, boundaryHeld, scanned, drained, failedClosed, sourceVersions, destVersions, sourcePresentMap, destPresentMap) =
    importing
      and boundaryHeld
      and scanned
      and drained
      and not(failedClosed)
      and KEYS.forall(key =>
        destVersions.get(key) == sourceVersions.get(key)
          and destPresentMap.get(key) == sourcePresentMap.get(key)
      )

  pure def activationReady =
    canActivate(
      importActive,
      boundaryCaptured,
      scanComplete,
      drainComplete,
      sessionFailedClosed,
      sourceVersion,
      destinationVersion,
      sourcePresent,
      destinationPresent,
    )

  def destinationNeverAheadOfSource =
    KEYS.forall(key => destinationVersion.get(key) <= sourceVersion.get(key))

  def activationRequiresCompleteBackfill =
    not(activated) or activationReady

  def cleanupRequiresActivation =
    not(cleanupStarted) or activated

  def cleanupCompletionRequiresCleanupStarted =
    not(cleanupComplete) or cleanupStarted

  def trimPreservesProtectedCursor =
    not(importActive and boundaryCaptured and protectedCursor > NONE_VERSION)
      or trimCursor < protectedCursor

  def failedClosedSessionCannotActivate =
    not(sessionFailedClosed and activated)

  def inv =
    destinationNeverAheadOfSource
      and activationRequiresCompleteBackfill
      and cleanupRequiresActivation
      and cleanupCompletionRequiresCleanupStarted
      and trimPreservesProtectedCursor
      and failedClosedSessionCannotActivate

  action init = all {
    sourceVersion' = initialVersions,
    sourcePresent' = initialPresent,
    destinationVersion' = initialVersions,
    destinationPresent' = initialPresent,
    protectedCursor' = NONE_VERSION,
    sourceCursor' = NONE_VERSION,
    trimCursor' = NONE_VERSION,
    boundaryCaptured' = false,
    scanComplete' = false,
    drainComplete' = false,
    importActive' = false,
    activated' = false,
    cleanupStarted' = false,
    cleanupComplete' = false,
    sessionFailedClosed' = false,
    sessionRestarted' = false,
    lastOutcome' = "not_started",
  }

  action StartImport = {
    val boundaryAvailable = sourceCursor == NONE_VERSION or trimCursor < sourceCursor
    all {
      sourceVersion' = sourceVersion,
      sourcePresent' = sourcePresent,
      destinationVersion' = destinationVersion,
      destinationPresent' = destinationPresent,
      protectedCursor' = sourceCursor,
      sourceCursor' = sourceCursor,
      trimCursor' = trimCursor,
      boundaryCaptured' = boundaryAvailable,
      scanComplete' = false,
      drainComplete' = false,
      importActive' = boundaryAvailable,
      activated' = false,
      cleanupStarted' = false,
      cleanupComplete' = false,
      sessionFailedClosed' = not(boundaryAvailable),
      sessionRestarted' = sessionRestarted,
      lastOutcome' = if (boundaryAvailable) "protected_cursor" else "session_failed_closed",
    }
  }

  action SourcePut = {
    nondet key = KEYS.oneOf()
    val version = nextVersionFor(sourceVersion, key)
    all {
      sourceVersion' = sourceVersion.set(key, version),
      sourcePresent' = sourcePresent.set(key, PRESENT),
      destinationVersion' = destinationVersion,
      destinationPresent' = destinationPresent,
      protectedCursor' = protectedCursor,
      sourceCursor' = maxCursor(sourceCursor, version),
      trimCursor' = trimCursor,
      boundaryCaptured' = boundaryCaptured,
      scanComplete' = scanComplete,
      drainComplete' = false,
      importActive' = importActive,
      activated' = false,
      cleanupStarted' = false,
      cleanupComplete' = false,
      sessionFailedClosed' = sessionFailedClosed,
      sessionRestarted' = sessionRestarted,
      lastOutcome' = "source_put",
    }
  }

  action SourceDelete = {
    nondet key = KEYS.oneOf()
    val version = nextVersionFor(sourceVersion, key)
    all {
      sourceVersion' = sourceVersion.set(key, version),
      sourcePresent' = sourcePresent.set(key, TOMBSTONE),
      destinationVersion' = destinationVersion,
      destinationPresent' = destinationPresent,
      protectedCursor' = protectedCursor,
      sourceCursor' = maxCursor(sourceCursor, version),
      trimCursor' = trimCursor,
      boundaryCaptured' = boundaryCaptured,
      scanComplete' = scanComplete,
      drainComplete' = false,
      importActive' = importActive,
      activated' = false,
      cleanupStarted' = false,
      cleanupComplete' = false,
      sessionFailedClosed' = sessionFailedClosed,
      sessionRestarted' = sessionRestarted,
      lastOutcome' = "source_delete",
    }
  }

  action ImportScanRecord = {
    nondet key = KEYS.oneOf()
    val result = applyLogicalRecord(
      destinationVersion,
      destinationPresent,
      key,
      sourceVersion.get(key),
      sourcePresent.get(key),
    )
    all {
      sourceVersion' = sourceVersion,
      sourcePresent' = sourcePresent,
      destinationVersion' = result.version,
      destinationPresent' = result.present,
      protectedCursor' = protectedCursor,
      sourceCursor' = sourceCursor,
      trimCursor' = trimCursor,
      boundaryCaptured' = boundaryCaptured,
      scanComplete' = scanComplete,
      drainComplete' = drainComplete,
      importActive' = importActive,
      activated' = false,
      cleanupStarted' = false,
      cleanupComplete' = false,
      sessionFailedClosed' = sessionFailedClosed,
      sessionRestarted' = sessionRestarted,
      lastOutcome' = result.outcome,
    }
  }

  action ImportDuplicateChunk = {
    nondet key = KEYS.oneOf()
    val result = applyLogicalRecord(
      destinationVersion,
      destinationPresent,
      key,
      destinationVersion.get(key),
      destinationPresent.get(key),
    )
    all {
      sourceVersion' = sourceVersion,
      sourcePresent' = sourcePresent,
      destinationVersion' = result.version,
      destinationPresent' = result.present,
      protectedCursor' = protectedCursor,
      sourceCursor' = sourceCursor,
      trimCursor' = trimCursor,
      boundaryCaptured' = boundaryCaptured,
      scanComplete' = scanComplete,
      drainComplete' = drainComplete,
      importActive' = importActive,
      activated' = false,
      cleanupStarted' = false,
      cleanupComplete' = false,
      sessionFailedClosed' = sessionFailedClosed,
      sessionRestarted' = sessionRestarted,
      lastOutcome' = result.outcome,
    }
  }

  action RejectStaleManifest = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = false,
    scanComplete' = scanComplete,
    drainComplete' = false,
    importActive' = false,
    activated' = false,
    cleanupStarted' = false,
    cleanupComplete' = false,
    sessionFailedClosed' = false,
    sessionRestarted' = false,
    lastOutcome' = "rejected_stale_manifest",
  }

  action CompleteScan = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = true,
    drainComplete' = drainComplete,
    importActive' = importActive,
    activated' = false,
    cleanupStarted' = false,
    cleanupComplete' = false,
    sessionFailedClosed' = sessionFailedClosed,
    sessionRestarted' = sessionRestarted,
    lastOutcome' = "scan_complete",
  }

  action DrainToCursor = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = sourceVersion,
    destinationPresent' = sourcePresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = scanComplete,
    drainComplete' = true,
    importActive' = importActive,
    activated' = false,
    cleanupStarted' = false,
    cleanupComplete' = false,
    sessionFailedClosed' = sessionFailedClosed,
    sessionRestarted' = sessionRestarted,
    lastOutcome' = "drain_complete",
  }

  action TryActivate = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = scanComplete,
    drainComplete' = drainComplete,
    importActive' = importActive,
    activated' = activationReady,
    cleanupStarted' = if (activationReady) cleanupStarted else false,
    cleanupComplete' = if (activationReady) cleanupComplete else false,
    sessionFailedClosed' = sessionFailedClosed,
    sessionRestarted' = sessionRestarted,
    lastOutcome' = if (activationReady) "activated" else "activation_blocked",
  }

  action CleanupTombstones = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = scanComplete,
    drainComplete' = drainComplete,
    importActive' = importActive,
    activated' = activated,
    cleanupStarted' = activated,
    cleanupComplete' = activated,
    sessionFailedClosed' = sessionFailedClosed,
    sessionRestarted' = sessionRestarted,
    lastOutcome' = if (activated) "tombstones_cleaned" else "cleanup_blocked",
  }

  action TrimExpiredUnprotected = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimFloorFor(importActive, boundaryCaptured, protectedCursor, sourceCursor),
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = scanComplete,
    drainComplete' = drainComplete,
    importActive' = importActive,
    activated' = activated,
    cleanupStarted' = cleanupStarted,
    cleanupComplete' = cleanupComplete,
    sessionFailedClosed' = sessionFailedClosed,
    sessionRestarted' = sessionRestarted,
    lastOutcome' = if (importActive and boundaryCaptured and protectedCursor > NONE_VERSION)
      "trim_blocked_by_protected_cursor"
    else
      "trimmed_unprotected_stream",
  }

  action StallDestinationPastRetention = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = scanComplete,
    drainComplete' = false,
    importActive' = importActive,
    activated' = false,
    cleanupStarted' = false,
    cleanupComplete' = false,
    sessionFailedClosed' = importActive and boundaryCaptured,
    sessionRestarted' = sessionRestarted,
    lastOutcome' = if (importActive and boundaryCaptured) "session_failed_closed" else "stall_without_session",
  }

  action RestartSession = all {
    sourceVersion' = sourceVersion,
    sourcePresent' = sourcePresent,
    destinationVersion' = destinationVersion,
    destinationPresent' = destinationPresent,
    protectedCursor' = protectedCursor,
    sourceCursor' = sourceCursor,
    trimCursor' = trimCursor,
    boundaryCaptured' = boundaryCaptured,
    scanComplete' = scanComplete,
    drainComplete' = drainComplete,
    importActive' = importActive,
    activated' = activated,
    cleanupStarted' = cleanupStarted,
    cleanupComplete' = cleanupComplete,
    sessionFailedClosed' = sessionFailedClosed,
    sessionRestarted' = importActive and boundaryCaptured,
    lastOutcome' = if (importActive and boundaryCaptured) "session_restarted" else "restart_without_session",
  }

  action step = any {
    StartImport,
    SourcePut,
    SourceDelete,
    ImportScanRecord,
    ImportDuplicateChunk,
    RejectStaleManifest,
    CompleteScan,
    DrainToCursor,
    TryActivate,
    CleanupTombstones,
    TrimExpiredUnprotected,
    StallDestinationPastRetention,
    RestartSession,
  }

  val success_witness = activated
  val rejection_witness = lastOutcome == "activation_blocked"
  val retry_witness = lastOutcome == "duplicate_chunk_idempotent"
  val tombstone_witness = lastOutcome == "applied_tombstone"
  val stale_cursor_witness = lastOutcome == "rejected_stale_manifest"
  val cleanup_witness = cleanupComplete and lastOutcome == "tombstones_cleaned"
  val trim_protected_witness = lastOutcome == "trim_blocked_by_protected_cursor"
  val stalled_session_witness = sessionFailedClosed and lastOutcome == "session_failed_closed"
  val restart_witness = sessionRestarted and lastOutcome == "session_restarted"

  run newerRecordWins =
    true.expect(
      applyLogicalRecord(initialVersions, initialPresent, "a", 3, PRESENT).outcome
        == "applied_present_record"
    )

  run olderRecordRejected =
    true.expect(
      applyLogicalRecord(
        Map("a" -> 3, "b" -> 0),
        Map("a" -> PRESENT, "b" -> false),
        "a",
        2,
        TOMBSTONE,
      ).outcome == "rejected_stale_record"
    )

  run activationRequiresDrain =
    false.expect(
      canActivate(true, true, true, false, false, initialVersions, initialVersions, initialPresent, initialPresent)
    )

  run emptySourceCanActivateAfterCapturedBoundary =
    true.expect(
      canActivate(true, true, true, true, false, initialVersions, initialVersions, initialPresent, initialPresent)
    )

  run cleanupRequiresActivationBeforeItCanComplete =
    false.expect(cleanupComplete and not(activated))

  run failedClosedSessionBlocksActivation =
    false.expect(
      canActivate(true, true, true, true, true, initialVersions, initialVersions, initialPresent, initialPresent)
    )
}
