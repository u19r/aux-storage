/// Compact MBT surface for custom stream duration Rust/spec comparison.
module custom_stream_duration_mbt {
  type DurationCase = {
    tableHours: int,
    itemHours: int,
    markerVersion: int,
    currentPolicyVersion: int,
    currentVersion: int,
    latestVersion: int,
    retainedTablePointerVersion: int,
    protectedBefore: int,
  }

  var lastCase: DurationCase
  var lastOutcome: str
  var effectiveHours: int
  var deleteCount: int
  var firstRemainingVersion: int
  var oldestRetainedVersion: int
  var nextDueHours: int
  var markerState: str
  var boundaryCeiling: int

  pure val FOREVER = -1
  pure val PAGE_SIZE = 2

  pure def initialCase = {
    tableHours: 2,
    itemHours: 2,
    markerVersion: 1,
    currentPolicyVersion: 1,
    currentVersion: 1,
    latestVersion: 1,
    retainedTablePointerVersion: 0,
    protectedBefore: 10,
  }

  pure def maxInt(left, right) =
    if (left > right) left else right

  pure def minInt(left, right) =
    if (left < right) left else right

  pure def effectiveRetentionHours(streamCase) =
    if (streamCase.tableHours == FOREVER or streamCase.itemHours == FOREVER)
      FOREVER
    else
      maxInt(streamCase.tableHours, streamCase.itemHours)

  pure def boundaryLimit(streamCase) =
    minInt(
      minInt(
        streamCase.latestVersion,
        if (streamCase.protectedBefore == 0) 999 else streamCase.protectedBefore
      ),
      if (streamCase.retainedTablePointerVersion == 0) 999 else streamCase.retainedTablePointerVersion
    )

  pure def boundedDeleteCount(streamCase, now) =
    if (effectiveRetentionHours(streamCase) == FOREVER or streamCase.currentVersion == 0)
      0
    else {
      val horizon = minInt(maxInt(0, now - effectiveRetentionHours(streamCase)), boundaryLimit(streamCase) - 1)
      if (horizon < streamCase.currentVersion)
        0
      else
        minInt(PAGE_SIZE, horizon - streamCase.currentVersion + 1)
    }

  pure def outcomeFor(streamCase, now) =
    if (streamCase.markerVersion != streamCase.currentPolicyVersion)
      "stale_marker_ignored"
    else if (effectiveRetentionHours(streamCase) == FOREVER)
      "forever_scope_skipped"
    else if (boundedDeleteCount(streamCase, now) == 0)
      "not_due_or_protected"
    else
      "bounded_page_deleted"

  pure def actualDeleteCount(streamCase, now) =
    if (outcomeFor(streamCase, now) == "bounded_page_deleted")
      boundedDeleteCount(streamCase, now)
    else
      0

  pure def firstRemaining(streamCase, now) =
    if (streamCase.currentVersion == 0)
      0
    else
      minInt(
        streamCase.latestVersion,
        streamCase.currentVersion + actualDeleteCount(streamCase, now)
      )

  pure def nextDue(streamCase, now) =
    if (streamCase.markerVersion != streamCase.currentPolicyVersion)
      now
    else if (effectiveRetentionHours(streamCase) == FOREVER)
      0
    else if (firstRemaining(streamCase, now) == 0)
      0
    else if (actualDeleteCount(streamCase, now) == 0)
      if (boundaryLimit(streamCase) <= 1)
        now + 1
      else {
        val candidate = firstRemaining(streamCase, now) + effectiveRetentionHours(streamCase)
        if (candidate <= now) now + 1 else candidate
      }
    else
      firstRemaining(streamCase, now) + effectiveRetentionHours(streamCase)

  pure def markerStateFor(streamCase, now) =
    if (streamCase.markerVersion != streamCase.currentPolicyVersion)
      "stale"
    else if (effectiveRetentionHours(streamCase) == FOREVER)
      "forever"
    else if (actualDeleteCount(streamCase, now) > 0)
      "trimmed"
    else
      "protected_or_not_due"

  action init = all {
    lastCase' = initialCase,
    lastOutcome' = "not_checked",
    effectiveHours' = effectiveRetentionHours(initialCase),
    deleteCount' = 0,
    firstRemainingVersion' = firstRemaining(initialCase, 5),
    oldestRetainedVersion' = initialCase.currentVersion,
    nextDueHours' = 5,
    markerState' = "pending",
    boundaryCeiling' = boundaryLimit(initialCase),
  }

  action Check = {
    nondet tableHours = Set(FOREVER, 1, 2, 3).oneOf()
    nondet itemHours = Set(FOREVER, 1, 2, 3).oneOf()
    nondet markerVersion = Set(1, 2).oneOf()
    nondet currentPolicyVersion = Set(1, 2).oneOf()
    nondet currentVersion = Set(0, 1, 2).oneOf()
    nondet latestVersion = Set(1, 2, 3).oneOf()
    nondet retainedTablePointerVersion = Set(0, 1, 2, 3).oneOf()
    nondet protectedBefore = Set(0, 1, 2, 3).oneOf()
    val streamCase = {
      tableHours: tableHours,
      itemHours: itemHours,
      markerVersion: markerVersion,
      currentPolicyVersion: currentPolicyVersion,
      currentVersion: currentVersion,
      latestVersion: maxInt(currentVersion, latestVersion),
      retainedTablePointerVersion: retainedTablePointerVersion,
      protectedBefore: protectedBefore,
    }
    all {
      retainedTablePointerVersion <= streamCase.latestVersion,
      lastCase' = streamCase,
      lastOutcome' = outcomeFor(streamCase, 5),
      effectiveHours' = effectiveRetentionHours(streamCase),
      deleteCount' = actualDeleteCount(streamCase, 5),
      firstRemainingVersion' = firstRemaining(streamCase, 5),
      oldestRetainedVersion' = firstRemaining(streamCase, 5),
      nextDueHours' = nextDue(streamCase, 5),
      markerState' = markerStateFor(streamCase, 5),
      boundaryCeiling' = boundaryLimit(streamCase),
    }
  }

  action step = Check

  val inv =
    (effectiveHours == FOREVER or effectiveHours >= lastCase.tableHours)
      and deleteCount <= PAGE_SIZE
      and (lastOutcome != "stale_marker_ignored" or deleteCount == 0)

  run shorterItemRetentionClampsToTableRetention =
    true.expect(effectiveRetentionHours({
      tableHours: 3,
      itemHours: 1,
      markerVersion: 1,
      currentPolicyVersion: 1,
      currentVersion: 1,
      latestVersion: 2,
      retainedTablePointerVersion: 0,
      protectedBefore: 10,
    }) == 3)

  run staleMarkerIsIgnored =
    true.expect(outcomeFor({
      tableHours: 1,
      itemHours: 1,
      markerVersion: 2,
      currentPolicyVersion: 1,
      currentVersion: 1,
      latestVersion: 3,
      retainedTablePointerVersion: 0,
      protectedBefore: 10,
    }, 5) == "stale_marker_ignored")

  run retainedTablePointerLimitsItemCleanup =
    true.expect(boundedDeleteCount({
      tableHours: 1,
      itemHours: 1,
      markerVersion: 1,
      currentPolicyVersion: 1,
      currentVersion: 1,
      latestVersion: 3,
      retainedTablePointerVersion: 2,
      protectedBefore: 10,
    }, 5) == 1)

  run zeroProtectedBoundaryDoesNotHideRetainedTablePointer =
    true.expect(boundedDeleteCount({
      tableHours: 1,
      itemHours: 2,
      markerVersion: 1,
      currentPolicyVersion: 1,
      currentVersion: 1,
      latestVersion: 3,
      retainedTablePointerVersion: 2,
      protectedBefore: 0,
    }, 5) == 1)

  run protectedNoProgressMovesToNextDueBucket =
    true.expect(nextDue({
      tableHours: 2,
      itemHours: 2,
      markerVersion: 1,
      currentPolicyVersion: 1,
      currentVersion: 1,
      latestVersion: 1,
      retainedTablePointerVersion: 1,
      protectedBefore: 3,
    }, 5) == 6)

  run protectedNoProgressAtNextHourMovesAgain =
    true.expect(nextDue({
      tableHours: 2,
      itemHours: 2,
      markerVersion: 1,
      currentPolicyVersion: 1,
      currentVersion: 1,
      latestVersion: 1,
      retainedTablePointerVersion: 1,
      protectedBefore: 3,
    }, 6) == 7)

  run inclusiveProtectedFutureDueIsPreserved =
    true.expect(nextDue({
      tableHours: 4,
      itemHours: 4,
      markerVersion: 1,
      currentPolicyVersion: 1,
      currentVersion: 3,
      latestVersion: 5,
      retainedTablePointerVersion: 0,
      protectedBefore: 3,
    }, 5) == 7)
}
