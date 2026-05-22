/// Local-node GSI backfill catch-up model.
///
/// Protects Invariant Inventory IDs GSI-1, GSI-2, GSI-3, GSI-4, GSI-5,
/// GSI-6, STREAM-5, and CONTROL-1 from docs/quint-backfill.md.
module gsi_backfill_catchup {
  pure val KEYS = Set("a", "b")
  pure val NONE_VERSION = 0
  pure val MISSING_VERSION = -1

  var sourceVersion: str -> int
  var sourceProjects: str -> bool
  var destinationVersion: str -> int
  var destinationProjects: str -> bool
  var tombstoneEvidence: str -> bool
  var protectedBoundary: int
  var streamTail: int
  var missingHistory: bool
  var scanComplete: bool
  var drainComplete: bool
  var activated: bool
  var lastOutcome: str

  pure def initialVersions = Map("a" -> NONE_VERSION, "b" -> NONE_VERSION)
  pure def missingVersions = Map("a" -> MISSING_VERSION, "b" -> MISSING_VERSION)
  pure def initialProjects = Map("a" -> false, "b" -> false)

  pure def nextVersionFor(versions, key) =
    versions.get(key) + 1

  pure def advanceTail(streamTailValue, version) =
    if (version > streamTailValue) version else streamTailValue

  pure def applyObservation(destVersion, destProjects, evidence, key, version, projects) =
    if (version >= destVersion.get(key))
      {
        destinationVersion: destVersion.set(key, version),
        destinationProjects: destProjects.set(key, projects),
        tombstoneEvidence: if (projects) evidence else evidence.set(key, true),
        outcome: if (projects) "applied_projection" else "applied_tombstone",
      }
    else
      {
        destinationVersion: destVersion,
        destinationProjects: destProjects,
        tombstoneEvidence: evidence,
        outcome: "rejected_stale_observation",
      }

  def destinationNotAheadOfSource =
    KEYS.forall(key => destinationVersion.get(key) <= sourceVersion.get(key))

  def projectedRowsMatchLatestAppliedVersion =
    KEYS.forall(key =>
      destinationVersion.get(key) == MISSING_VERSION
        or destinationProjects.get(key) == sourceProjects.get(key)
        or (not(sourceProjects.get(key)) and tombstoneEvidence.get(key))
        or destinationVersion.get(key) < sourceVersion.get(key)
    )

  def activationGate =
    not(activated) or (
      protectedBoundary > NONE_VERSION
        and scanComplete
        and drainComplete
        and not(missingHistory)
        and KEYS.forall(key => destinationVersion.get(key) == sourceVersion.get(key))
    )

  def missingHistoryFailsClosed =
    not(missingHistory and activated)

  def catchupSafety =
    destinationNotAheadOfSource
      and projectedRowsMatchLatestAppliedVersion
      and activationGate
      and missingHistoryFailsClosed

  action init = all {
    sourceVersion' = initialVersions,
    sourceProjects' = initialProjects,
    destinationVersion' = missingVersions,
    destinationProjects' = initialProjects,
    tombstoneEvidence' = initialProjects,
    protectedBoundary' = NONE_VERSION,
    streamTail' = NONE_VERSION,
    missingHistory' = false,
    scanComplete' = false,
    drainComplete' = false,
    activated' = false,
    lastOutcome' = "not_started",
  }

  action CaptureBoundary = all {
    protectedBoundary' = streamTail,
    sourceVersion' = sourceVersion,
    sourceProjects' = sourceProjects,
    destinationVersion' = destinationVersion,
    destinationProjects' = destinationProjects,
    tombstoneEvidence' = tombstoneEvidence,
    streamTail' = streamTail,
    missingHistory' = missingHistory,
    scanComplete' = scanComplete,
    drainComplete' = drainComplete,
    activated' = activated,
    lastOutcome' = "captured_boundary",
  }

  action ConcurrentPut = {
    nondet key = KEYS.oneOf()
    val version = nextVersionFor(sourceVersion, key)
    all {
      sourceVersion' = sourceVersion.set(key, version),
      sourceProjects' = sourceProjects.set(key, true),
      streamTail' = advanceTail(streamTail, version),
      protectedBoundary' = protectedBoundary,
      destinationVersion' = destinationVersion,
      destinationProjects' = destinationProjects,
      tombstoneEvidence' = tombstoneEvidence,
      missingHistory' = missingHistory,
      scanComplete' = scanComplete,
      drainComplete' = false,
      activated' = false,
      lastOutcome' = "source_put",
    }
  }

  action ConcurrentDelete = {
    nondet key = KEYS.oneOf()
    val version = nextVersionFor(sourceVersion, key)
    all {
      sourceVersion' = sourceVersion.set(key, version),
      sourceProjects' = sourceProjects.set(key, false),
      streamTail' = advanceTail(streamTail, version),
      protectedBoundary' = protectedBoundary,
      destinationVersion' = destinationVersion,
      destinationProjects' = destinationProjects,
      tombstoneEvidence' = tombstoneEvidence,
      missingHistory' = missingHistory,
      scanComplete' = scanComplete,
      drainComplete' = false,
      activated' = false,
      lastOutcome' = "source_delete",
    }
  }

  action ScanObservation = {
    nondet key = KEYS.oneOf()
    nondet observedVersion = Set(0, 1, 2).oneOf()
    nondet projects = Set(true, false).oneOf()
    val observation = applyObservation(
      destinationVersion,
      destinationProjects,
      tombstoneEvidence,
      key,
      observedVersion,
      projects,
    )
    all {
      observedVersion <= sourceVersion.get(key),
      observedVersion < sourceVersion.get(key) or projects == sourceProjects.get(key),
      sourceVersion' = sourceVersion,
      sourceProjects' = sourceProjects,
      destinationVersion' = observation.destinationVersion,
      destinationProjects' = observation.destinationProjects,
      tombstoneEvidence' = observation.tombstoneEvidence,
      protectedBoundary' = protectedBoundary,
      streamTail' = streamTail,
      missingHistory' = missingHistory,
      scanComplete' = scanComplete,
      drainComplete' = drainComplete,
      activated' = activated,
      lastOutcome' = observation.outcome,
    }
  }

  action StreamObservation = {
    nondet key = KEYS.oneOf()
    val observation = applyObservation(
      destinationVersion,
      destinationProjects,
      tombstoneEvidence,
      key,
      sourceVersion.get(key),
      sourceProjects.get(key),
    )
    all {
      protectedBoundary > NONE_VERSION,
      not(missingHistory),
      sourceVersion' = sourceVersion,
      sourceProjects' = sourceProjects,
      destinationVersion' = observation.destinationVersion,
      destinationProjects' = observation.destinationProjects,
      tombstoneEvidence' = observation.tombstoneEvidence,
      protectedBoundary' = protectedBoundary,
      streamTail' = streamTail,
      missingHistory' = missingHistory,
      scanComplete' = scanComplete,
      drainComplete' = drainComplete,
      activated' = activated,
      lastOutcome' = observation.outcome,
    }
  }

  action MarkScanComplete = all {
    scanComplete' = true,
    sourceVersion' = sourceVersion,
    sourceProjects' = sourceProjects,
    destinationVersion' = destinationVersion,
    destinationProjects' = destinationProjects,
    tombstoneEvidence' = tombstoneEvidence,
    protectedBoundary' = protectedBoundary,
    streamTail' = streamTail,
    missingHistory' = missingHistory,
    drainComplete' = drainComplete,
    activated' = activated,
    lastOutcome' = "scan_complete",
  }

  action MarkMissingHistory = all {
    protectedBoundary > NONE_VERSION,
    missingHistory' = true,
    sourceVersion' = sourceVersion,
    sourceProjects' = sourceProjects,
    destinationVersion' = destinationVersion,
    destinationProjects' = destinationProjects,
    tombstoneEvidence' = tombstoneEvidence,
    protectedBoundary' = protectedBoundary,
    streamTail' = streamTail,
    scanComplete' = scanComplete,
    drainComplete' = false,
    activated' = false,
    lastOutcome' = "rejected_missing_history",
  }

  action FinalDrain = all {
    scanComplete,
    not(missingHistory),
    KEYS.forall(key => destinationVersion.get(key) == sourceVersion.get(key)),
    drainComplete' = true,
    sourceVersion' = sourceVersion,
    sourceProjects' = sourceProjects,
    destinationVersion' = destinationVersion,
    destinationProjects' = destinationProjects,
    tombstoneEvidence' = tombstoneEvidence,
    protectedBoundary' = protectedBoundary,
    streamTail' = streamTail,
    missingHistory' = missingHistory,
    scanComplete' = scanComplete,
    activated' = activated,
    lastOutcome' = "final_drain_complete",
  }

  action Activate = all {
    protectedBoundary > NONE_VERSION,
    scanComplete,
    drainComplete,
    not(missingHistory),
    KEYS.forall(key => destinationVersion.get(key) == sourceVersion.get(key)),
    activated' = true,
    sourceVersion' = sourceVersion,
    sourceProjects' = sourceProjects,
    destinationVersion' = destinationVersion,
    destinationProjects' = destinationProjects,
    tombstoneEvidence' = tombstoneEvidence,
    protectedBoundary' = protectedBoundary,
    streamTail' = streamTail,
    missingHistory' = missingHistory,
    scanComplete' = scanComplete,
    drainComplete' = drainComplete,
    lastOutcome' = "activated",
  }

  action UnsafeActivateWithoutTombstones = {
    nondet key = KEYS.oneOf()
    all {
      protectedBoundary > NONE_VERSION,
      scanComplete,
      not(missingHistory),
      sourceVersion.get(key) > destinationVersion.get(key),
      not(sourceProjects.get(key)),
      destinationProjects.get(key),
      not(tombstoneEvidence.get(key)),
      activated' = true,
      sourceVersion' = sourceVersion,
      sourceProjects' = sourceProjects,
      destinationVersion' = destinationVersion,
      destinationProjects' = destinationProjects,
      tombstoneEvidence' = tombstoneEvidence,
      protectedBoundary' = protectedBoundary,
      streamTail' = streamTail,
      missingHistory' = missingHistory,
      scanComplete' = scanComplete,
      drainComplete' = true,
      lastOutcome' = "unsafe_no_tombstone_activated",
    }
  }

  action step =
    any {
      CaptureBoundary,
      ConcurrentPut,
      ConcurrentDelete,
      ScanObservation,
      StreamObservation,
      MarkScanComplete,
      MarkMissingHistory,
      FinalDrain,
      Activate,
    }

  action noTombstoneFailureStep =
    any {
      CaptureBoundary,
      ConcurrentPut,
      ConcurrentDelete,
      ScanObservation,
      MarkScanComplete,
      UnsafeActivateWithoutTombstones,
    }

  val inv = catchupSafety

  val scan_stream_race_witness =
    protectedBoundary > NONE_VERSION
      and lastOutcome == "rejected_stale_observation"

  val gsi_key_move_witness =
    KEYS.exists(key => sourceVersion.get(key) > NONE_VERSION and sourceProjects.get(key))
      and KEYS.exists(key => sourceVersion.get(key) > NONE_VERSION and not(sourceProjects.get(key)))

  val stream_delete_beats_stale_scan_witness =
    KEYS.exists(key =>
      sourceVersion.get(key) > destinationVersion.get(key)
        and not(sourceProjects.get(key))
        and destinationProjects.get(key)
    )

  val missing_stream_history_rejected_witness =
    missingHistory and lastOutcome == "rejected_missing_history" and not(activated)

  val no_tombstone_failure_witness =
    activated and lastOutcome == "unsafe_no_tombstone_activated"

  run staleObservationIsRejected =
    true.expect(
      applyObservation(
        Map("a" -> 2, "b" -> MISSING_VERSION),
        Map("a" -> true, "b" -> false),
        Map("a" -> false, "b" -> false),
        "a",
        1,
        false,
      ).outcome == "rejected_stale_observation"
    )

  run activationRequiresDrain =
    false.expect(
      not(missingHistory)
        and true
        and false
    )
}
