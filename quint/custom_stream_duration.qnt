/// Custom stream duration model.
///
/// Models the v1 semantic contract for table stream retention, item stream
/// retention, stale due markers, latest-row protection, protected boundaries,
/// and bounded cleanup. Versions double as timestamps in this compact model.
module custom_stream_duration {
  pure val KEYS = Set("hot", "cold")
  pure val NONE_VERSION = 0
  pure val FINITE_TABLE_RETENTION = 2
  pure val SHORT_ITEM_RETENTION = 1
  pure val LONG_ITEM_RETENTION = 3
  pure val PAGE_SIZE = 2

  type Retention = { kind: str, hours: int }
  type DueMarker = { scope: str, key: str, policyVersion: int }

  var now: int
  var tableRetention: Retention
  var tablePolicyVersion: int
  var itemRetention: str -> Retention
  var itemPolicyVersion: str -> int
  var tableRows: str -> int
  var itemRows: str -> int
  var latestItemVersion: str -> int
  var dueMarkers: Set[DueMarker]
  var protectedBefore: int
  var failedWriteTouchedPolicy: bool
  var lastOutcome: str
  var lastDeleteCount: int
  var lastTrimLimit: int

  pure def forever = { kind: "forever", hours: -1 }
  pure def finite(hours) = { kind: "finite", hours: hours }

  pure def initialItemRetention = Map("hot" -> finite(FINITE_TABLE_RETENTION), "cold" -> finite(FINITE_TABLE_RETENTION))
  pure def initialPolicyVersions = Map("hot" -> 1, "cold" -> 1)
  pure def initialRows = Map("hot" -> NONE_VERSION, "cold" -> NONE_VERSION)

  pure def maxInt(left, right) =
    if (left > right) left else right

  pure def minInt(left, right) =
    if (left < right) left else right

  pure def effectiveRetention(tablePolicy, itemPolicy) =
    if (tablePolicy.kind == "forever" or itemPolicy.kind == "forever")
      forever
    else
      finite(maxInt(tablePolicy.hours, itemPolicy.hours))

  pure def horizon(currentTime, retention) =
    if (retention.kind == "forever") NONE_VERSION else maxInt(NONE_VERSION, currentTime - retention.hours)

  pure def trimLimit(currentTime, retention, protectedFloor) =
    minInt(horizon(currentTime, retention), protectedFloor)

  pure def markerFor(scope, key, version) =
    { scope: scope, key: key, policyVersion: version }

  pure def addDueMarker(markers, marker, retention) =
    if (retention.kind == "forever") markers else markers.union(Set(marker))

  pure def countTrimmed(oldest, limit, latest) =
    if (oldest == NONE_VERSION or limit < oldest)
      0
    else if (latest > NONE_VERSION and limit >= latest)
      minInt(PAGE_SIZE, maxInt(0, latest - oldest))
    else
      minInt(PAGE_SIZE, limit - oldest + 1)

  pure def advanceOldest(oldest, deleted) =
    if (oldest == NONE_VERSION or deleted == 0) oldest else oldest + deleted

  pure def itemTrimAllowed(tablePolicy, itemPolicy, currentTime, itemVersion, protectedFloor) =
    effectiveRetention(tablePolicy, itemPolicy).kind != "forever"
      and itemVersion > NONE_VERSION
      and itemVersion <= trimLimit(currentTime, effectiveRetention(tablePolicy, itemPolicy), protectedFloor)

  def itemRetentionAtLeastTable =
    KEYS.forall(key =>
      effectiveRetention(tableRetention, itemRetention.get(key)).kind == "forever"
        or tableRetention.kind == "forever"
        or effectiveRetention(tableRetention, itemRetention.get(key)).hours >= tableRetention.hours
    )

  def retainedTablePointersHaveItemRows =
    KEYS.forall(key =>
      tableRows.get(key) == NONE_VERSION
        or itemRows.get(key) <= tableRows.get(key)
    )

  def latestRowsProtected =
    KEYS.forall(key =>
      latestItemVersion.get(key) == NONE_VERSION
        or itemRows.get(key) <= latestItemVersion.get(key)
    )

  def staleMarkersDoNotDelete =
    lastOutcome != "stale_marker_ignored" or lastDeleteCount == 0

  def protectedBoundaryBlocksTrim =
    not(lastOutcome == "table_marker_trimmed_bounded_page" or lastOutcome == "item_marker_trimmed_bounded_page")
      or lastTrimLimit <= protectedBefore

  def failedWritesHaveNoPolicySideEffects =
    not(failedWriteTouchedPolicy)

  def inv =
    itemRetentionAtLeastTable
      and retainedTablePointersHaveItemRows
      and latestRowsProtected
      and staleMarkersDoNotDelete
      and protectedBoundaryBlocksTrim
      and failedWritesHaveNoPolicySideEffects

  action init = all {
    now' = 1,
    tableRetention' = finite(FINITE_TABLE_RETENTION),
    tablePolicyVersion' = 1,
    itemRetention' = initialItemRetention,
    itemPolicyVersion' = initialPolicyVersions,
    tableRows' = initialRows,
    itemRows' = initialRows,
    latestItemVersion' = initialRows,
    dueMarkers' = Set(markerFor("table", "table", 1)),
    protectedBefore' = 10,
    failedWriteTouchedPolicy' = false,
    lastOutcome' = "initialized",
    lastDeleteCount' = 0,
    lastTrimLimit' = NONE_VERSION,
  }

  action SuccessfulWrite = {
    nondet key = KEYS.oneOf()
    val nextVersion = latestItemVersion.get(key) + 1
    all {
      now' = maxInt(now + 1, nextVersion),
      tableRetention' = tableRetention,
      tablePolicyVersion' = tablePolicyVersion,
      itemRetention' = itemRetention,
      itemPolicyVersion' = itemPolicyVersion,
      tableRows' = tableRows.set(key, if (tableRows.get(key) == NONE_VERSION) nextVersion else tableRows.get(key)),
      itemRows' = itemRows.set(key, if (itemRows.get(key) == NONE_VERSION) nextVersion else itemRows.get(key)),
      latestItemVersion' = latestItemVersion.set(key, nextVersion),
      dueMarkers' = dueMarkers,
      protectedBefore' = protectedBefore,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = "write_committed",
      lastDeleteCount' = 0,
      lastTrimLimit' = NONE_VERSION,
    }
  }

  action FailedWrite = all {
    now' = now,
    tableRetention' = tableRetention,
    tablePolicyVersion' = tablePolicyVersion,
    itemRetention' = itemRetention,
    itemPolicyVersion' = itemPolicyVersion,
    tableRows' = tableRows,
    itemRows' = itemRows,
    latestItemVersion' = latestItemVersion,
    dueMarkers' = dueMarkers,
    protectedBefore' = protectedBefore,
    failedWriteTouchedPolicy' = false,
    lastOutcome' = "failed_write_rolled_back",
    lastDeleteCount' = 0,
    lastTrimLimit' = NONE_VERSION,
  }

  action ShrinkTable = {
    val nextPolicyVersion = tablePolicyVersion + 1
    val newRetention = finite(1)
    all {
      now' = now,
      tableRetention' = newRetention,
      tablePolicyVersion' = nextPolicyVersion,
      itemRetention' = itemRetention,
      itemPolicyVersion' = itemPolicyVersion,
      tableRows' = tableRows,
      itemRows' = itemRows,
      latestItemVersion' = latestItemVersion,
      dueMarkers' = addDueMarker(dueMarkers, markerFor("table", "table", nextPolicyVersion), newRetention),
      protectedBefore' = protectedBefore,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = "table_policy_shrank",
      lastDeleteCount' = 0,
      lastTrimLimit' = NONE_VERSION,
    }
  }

  action ExpandTable = {
    val nextPolicyVersion = tablePolicyVersion + 1
    val newRetention = finite(LONG_ITEM_RETENTION)
    all {
      now' = now,
      tableRetention' = newRetention,
      tablePolicyVersion' = nextPolicyVersion,
      itemRetention' = itemRetention,
      itemPolicyVersion' = itemPolicyVersion,
      tableRows' = tableRows,
      itemRows' = itemRows,
      latestItemVersion' = latestItemVersion,
      dueMarkers' = addDueMarker(dueMarkers, markerFor("table", "table", nextPolicyVersion), newRetention),
      protectedBefore' = protectedBefore,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = "table_policy_expanded",
      lastDeleteCount' = 0,
      lastTrimLimit' = NONE_VERSION,
    }
  }

  action SetTableForever = all {
    now' = now,
    tableRetention' = forever,
    tablePolicyVersion' = tablePolicyVersion + 1,
    itemRetention' = itemRetention,
    itemPolicyVersion' = itemPolicyVersion,
    tableRows' = tableRows,
    itemRows' = itemRows,
    latestItemVersion' = latestItemVersion,
    dueMarkers' = dueMarkers,
    protectedBefore' = protectedBefore,
    failedWriteTouchedPolicy' = false,
    lastOutcome' = "table_policy_forever",
    lastDeleteCount' = 0,
    lastTrimLimit' = NONE_VERSION,
  }

  action SetItemRetention = {
    nondet key = KEYS.oneOf()
    nondet retention = Set(finite(SHORT_ITEM_RETENTION), finite(LONG_ITEM_RETENTION), forever).oneOf()
    val nextPolicyVersion = itemPolicyVersion.get(key) + 1
    all {
      now' = now,
      tableRetention' = tableRetention,
      tablePolicyVersion' = tablePolicyVersion,
      itemRetention' = itemRetention.set(key, retention),
      itemPolicyVersion' = itemPolicyVersion.set(key, nextPolicyVersion),
      tableRows' = tableRows,
      itemRows' = itemRows,
      latestItemVersion' = latestItemVersion,
      dueMarkers' = addDueMarker(dueMarkers, markerFor("item", key, nextPolicyVersion), effectiveRetention(tableRetention, retention)),
      protectedBefore' = protectedBefore,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = if (retention.kind == "forever") "item_policy_forever" else "item_policy_changed",
      lastDeleteCount' = 0,
      lastTrimLimit' = NONE_VERSION,
    }
  }

  action ProtectBoundary = {
    nondet boundary = Set(0, 1, 2, 3, 4, 5).oneOf()
    all {
      now' = now,
      tableRetention' = tableRetention,
      tablePolicyVersion' = tablePolicyVersion,
      itemRetention' = itemRetention,
      itemPolicyVersion' = itemPolicyVersion,
      tableRows' = tableRows,
      itemRows' = itemRows,
      latestItemVersion' = latestItemVersion,
      dueMarkers' = dueMarkers,
      protectedBefore' = boundary,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = "protected_boundary_set",
      lastDeleteCount' = 0,
      lastTrimLimit' = NONE_VERSION,
    }
  }

  action ProcessTableMarker = {
    nondet markerVersion = Set(1, 2, 3, 4, 5).oneOf()
    val marker = markerFor("table", "table", markerVersion)
    val currentMarker = markerVersion == tablePolicyVersion
    val limit = trimLimit(now, tableRetention, protectedBefore)
    nondet key = KEYS.oneOf()
    val tableDeleted = if (currentMarker) countTrimmed(tableRows.get(key), limit, NONE_VERSION) else 0
    val newTableRow = advanceOldest(tableRows.get(key), tableDeleted)
    val itemDeleted =
      if (currentMarker and itemTrimAllowed(tableRetention, itemRetention.get(key), now, itemRows.get(key), protectedBefore))
        countTrimmed(itemRows.get(key), minInt(limit, newTableRow - 1), latestItemVersion.get(key))
      else
        0
    all {
      dueMarkers.contains(marker),
      now' = now,
      tableRetention' = tableRetention,
      tablePolicyVersion' = tablePolicyVersion,
      itemRetention' = itemRetention,
      itemPolicyVersion' = itemPolicyVersion,
      tableRows' = tableRows.set(key, newTableRow),
      itemRows' = itemRows.set(key, advanceOldest(itemRows.get(key), itemDeleted)),
      latestItemVersion' = latestItemVersion,
      dueMarkers' = dueMarkers.exclude(Set(marker)),
      protectedBefore' = protectedBefore,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = if (currentMarker) "table_marker_trimmed_bounded_page" else "stale_marker_ignored",
      lastDeleteCount' = tableDeleted + itemDeleted,
      lastTrimLimit' = if (currentMarker) limit else NONE_VERSION,
    }
  }

  action ProcessItemMarker = {
    nondet key = KEYS.oneOf()
    nondet markerVersion = Set(1, 2, 3, 4, 5).oneOf()
    val marker = markerFor("item", key, markerVersion)
    val currentMarker = markerVersion == itemPolicyVersion.get(key)
    val retention = effectiveRetention(tableRetention, itemRetention.get(key))
    val tablePointerLimit = if (tableRows.get(key) == NONE_VERSION) trimLimit(now, retention, protectedBefore) else tableRows.get(key) - 1
    val itemLimit = minInt(trimLimit(now, retention, protectedBefore), tablePointerLimit)
    val itemDeleted =
      if (currentMarker and retention.kind != "forever")
        countTrimmed(itemRows.get(key), itemLimit, latestItemVersion.get(key))
      else
        0
    all {
      dueMarkers.contains(marker),
      now' = now,
      tableRetention' = tableRetention,
      tablePolicyVersion' = tablePolicyVersion,
      itemRetention' = itemRetention,
      itemPolicyVersion' = itemPolicyVersion,
      tableRows' = tableRows,
      itemRows' = itemRows.set(key, advanceOldest(itemRows.get(key), itemDeleted)),
      latestItemVersion' = latestItemVersion,
      dueMarkers' = dueMarkers.exclude(Set(marker)),
      protectedBefore' = protectedBefore,
      failedWriteTouchedPolicy' = false,
      lastOutcome' = if (currentMarker) "item_marker_trimmed_bounded_page" else "stale_marker_ignored",
      lastDeleteCount' = itemDeleted,
      lastTrimLimit' = if (currentMarker) itemLimit else NONE_VERSION,
    }
  }

  action step =
    any {
      SuccessfulWrite,
      FailedWrite,
      ShrinkTable,
      ExpandTable,
      SetTableForever,
      SetItemRetention,
      ProtectBoundary,
      ProcessTableMarker,
      ProcessItemMarker,
    }

  val table_shrink_witness =
    lastOutcome == "table_policy_shrank"

  val table_expansion_stale_marker_witness =
    lastOutcome == "stale_marker_ignored" and lastDeleteCount == 0

  val forever_witness =
    tableRetention.kind == "forever" or KEYS.exists(key => itemRetention.get(key).kind == "forever")

  val item_shorter_than_table_witness =
    KEYS.exists(key =>
      tableRetention.kind == "finite"
        and itemRetention.get(key).kind == "finite"
        and itemRetention.get(key).hours < tableRetention.hours
        and effectiveRetention(tableRetention, itemRetention.get(key)).hours == tableRetention.hours
    )

  val latest_row_protection_witness =
    KEYS.exists(key =>
      latestItemVersion.get(key) > NONE_VERSION
        and itemRows.get(key) == latestItemVersion.get(key)
        and lastOutcome == "item_marker_trimmed_bounded_page"
    )

  val protected_trim_witness =
    lastOutcome == "table_marker_trimmed_bounded_page" and protectedBefore < horizon(now, tableRetention)

  val hot_key_cleanup_witness =
    lastOutcome == "table_marker_trimmed_bounded_page" and lastDeleteCount >= PAGE_SIZE

  run maxRetentionKeepsItemAtLeastTable =
    true.expect(effectiveRetention(finite(72), finite(1)).hours == 72)

  run foreverDominatesFiniteRetention =
    true
      .expect(effectiveRetention(forever, finite(1)).kind == "forever")
      .expect(effectiveRetention(finite(1), forever).kind == "forever")

  run latestRowTrimCountPreservesCurrentRecord =
    true.expect(countTrimmed(1, 10, 3) == 2)

  run boundedPageDoesNotDeleteMoreThanPageSize =
    true.expect(countTrimmed(1, 10, NONE_VERSION) == PAGE_SIZE)
}
