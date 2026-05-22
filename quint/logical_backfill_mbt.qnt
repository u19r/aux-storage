/// Compact MBT surface for shared logical backfill import apply decisions.
///
/// Protects stale scan-image, duplicate replay, present-item, and tombstone
/// decisions at the storage-backfill boundary.
module logical_backfill_mbt {
  pure val NO_CURRENT_VERSION = -1

  type ApplyCase = {
    currentVersion: int,
    incomingVersion: int,
    incomingKind: str,
  }

  type PreflightCase = {
    destinationEmpty: bool,
    preflightMarkerPresent: bool,
  }

  var lastCase: ApplyCase
  var lastDecision: str
  var lastPreflightCase: PreflightCase
  var lastPreflightDecision: str

  pure def initialCase = {
    currentVersion: NO_CURRENT_VERSION,
    incomingVersion: 1,
    incomingKind: "present_item",
  }

  pure def expectedDecision(applyCase) =
    if (applyCase.currentVersion != NO_CURRENT_VERSION and applyCase.incomingVersion < applyCase.currentVersion)
      "ignore_stale"
    else if (applyCase.currentVersion != NO_CURRENT_VERSION and applyCase.incomingVersion == applyCase.currentVersion)
      "ignore_duplicate"
    else if (applyCase.incomingKind == "present_item")
      "apply_present_item"
    else
      "apply_tombstone"

  pure def expectedPreflightDecision(preflightCase) =
    if (preflightCase.preflightMarkerPresent)
      "allow_retry_after_preflight"
    else if (preflightCase.destinationEmpty)
      "allow_empty_destination"
    else
      "reject_non_empty_destination"

  action init = all {
    lastCase' = initialCase,
    lastDecision' = "not_checked",
    lastPreflightCase' = {
      destinationEmpty: true,
      preflightMarkerPresent: false,
    },
    lastPreflightDecision' = "not_checked",
  }

  action Check = {
    nondet currentVersion = Set(NO_CURRENT_VERSION, 1, 2).oneOf()
    nondet incomingVersion = Set(1, 2, 3).oneOf()
    nondet incomingKind = Set("present_item", "tombstone").oneOf()
    val applyCase = {
      currentVersion: currentVersion,
      incomingVersion: incomingVersion,
      incomingKind: incomingKind,
    }
    all {
      lastCase' = applyCase,
      lastDecision' = expectedDecision(applyCase),
      lastPreflightCase' = lastPreflightCase,
      lastPreflightDecision' = lastPreflightDecision,
    }
  }

  action CheckPreflight = {
    nondet destinationEmpty = Set(true, false).oneOf()
    nondet preflightMarkerPresent = Set(true, false).oneOf()
    val preflightCase = {
      destinationEmpty: destinationEmpty,
      preflightMarkerPresent: preflightMarkerPresent,
    }
    all {
      lastCase' = lastCase,
      lastDecision' = lastDecision,
      lastPreflightCase' = preflightCase,
      lastPreflightDecision' = expectedPreflightDecision(preflightCase),
    }
  }

  action step = any { Check, CheckPreflight }

  val inv =
    lastDecision != "ignore_stale" or (
      lastCase.currentVersion != NO_CURRENT_VERSION and lastCase.incomingVersion < lastCase.currentVersion
    )
      and (
        lastPreflightDecision != "reject_non_empty_destination"
          or (not(lastPreflightCase.destinationEmpty) and not(lastPreflightCase.preflightMarkerPresent))
      )

  run olderScanImageIgnored =
    true.expect(
      expectedDecision({
        currentVersion: 2,
        incomingVersion: 1,
        incomingKind: "present_item",
      }) == "ignore_stale"
    )

  run duplicateReplayIgnored =
    true.expect(
      expectedDecision({
        currentVersion: 2,
        incomingVersion: 2,
        incomingKind: "tombstone",
      }) == "ignore_duplicate"
    )

  run nonEmptyDestinationRejectedBeforePreflight =
    true.expect(
      expectedPreflightDecision({
        destinationEmpty: false,
        preflightMarkerPresent: false,
      }) == "reject_non_empty_destination"
    )

  run retryAllowedAfterPreflightMarker =
    true.expect(
      expectedPreflightDecision({
        destinationEmpty: false,
        preflightMarkerPresent: true,
      }) == "allow_retry_after_preflight"
    )
}
