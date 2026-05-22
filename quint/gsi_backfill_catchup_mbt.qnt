/// Compact MBT surface for GSI catch-up planner/apply Rust/spec comparison.
///
/// Protects Invariant Inventory IDs GSI-2, GSI-3, GSI-5, and GSI-6.
module gsi_backfill_catchup_mbt {
  pure val CURRENT_VERSION = 2
  pure val MISSING_HISTORY_VERSION = -1

  type ApplyCase = {
    currentVersion: int,
    observationVersion: int,
    currentProjects: bool,
    observationProjects: bool,
    historyAvailable: bool,
    scanComplete: bool,
    drainComplete: bool,
  }

  var lastCase: ApplyCase
  var lastOutcome: str

  pure def initialCase = {
    currentVersion: 0,
    observationVersion: 0,
    currentProjects: false,
    observationProjects: false,
    historyAvailable: true,
    scanComplete: false,
    drainComplete: false,
  }

  pure def expectedOutcome(applyCase) =
    if (not(applyCase.historyAvailable))
      "rejected_missing_history"
    else if (applyCase.observationVersion < applyCase.currentVersion)
      "rejected_stale_observation"
    else if (applyCase.scanComplete and applyCase.drainComplete)
      "activation_allowed"
    else if (applyCase.observationProjects)
      "applied_projection"
    else
      "applied_tombstone"

  action init = all {
    lastCase' = initialCase,
    lastOutcome' = "not_checked",
  }

  action Check = {
    nondet currentVersion = Set(0, 1, CURRENT_VERSION).oneOf()
    nondet observationVersion = Set(MISSING_HISTORY_VERSION, 0, 1, CURRENT_VERSION).oneOf()
    nondet currentProjects = Set(true, false).oneOf()
    nondet observationProjects = Set(true, false).oneOf()
    nondet historyAvailable = Set(true, false).oneOf()
    nondet scanComplete = Set(true, false).oneOf()
    nondet drainComplete = Set(true, false).oneOf()
    val applyCase = {
      currentVersion: currentVersion,
      observationVersion: observationVersion,
      currentProjects: currentProjects,
      observationProjects: observationProjects,
      historyAvailable: historyAvailable,
      scanComplete: scanComplete,
      drainComplete: drainComplete,
    }
    all {
      lastCase' = applyCase,
      lastOutcome' = expectedOutcome(applyCase),
    }
  }

  action step = Check

  val inv =
    lastOutcome != "activation_allowed" or (
      lastCase.historyAvailable and lastCase.scanComplete and lastCase.drainComplete
    )

  run staleObservationRejected =
    true.expect(
      expectedOutcome({
        currentVersion: CURRENT_VERSION,
        observationVersion: 1,
        currentProjects: true,
        observationProjects: false,
        historyAvailable: true,
        scanComplete: false,
        drainComplete: false,
      }) == "rejected_stale_observation"
    )

  run missingHistoryRejected =
    true.expect(
      expectedOutcome({
        currentVersion: CURRENT_VERSION,
        observationVersion: MISSING_HISTORY_VERSION,
        currentProjects: true,
        observationProjects: true,
        historyAvailable: false,
        scanComplete: true,
        drainComplete: true,
      }) == "rejected_missing_history"
    )
}
