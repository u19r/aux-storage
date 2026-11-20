/// One source A bootstraps its sole Joining peer B with a fuzzy scan.
/// Atomic native changes, retained source feed and pair-safe delete GC are premises.
module regional_bootstrap_closure_model {
  const BROKEN: str
  type State = { written: Set[int], sourceRows: int -> int,
    scan: int -> int, captured: Set[int], acked: Set[int], scanDone: bool,
    rows: int -> int, accepted: Set[int], cursor: int, retained: Set[int],
    bootstrapHold: bool, normalHold: bool, active: bool, reclaimed: bool }
  var s: State
  pure val itemKeys = Set(0, 1)
  pure val positions = List(0, 10, 20, 30, 90)
  pure val eventKeys = List(0, 1, 0, 1, 0)
  pure val stamps = List(1, 5, 20, 30, 90)
  pure def maximum(row: int, id: int): int =
    if (id == -1) row else if (row == -1 or stamps[id] > stamps[row]) id else row
  pure def isCovered(rows: int -> int, id: int): bool = {
    val row = rows.get(eventKeys[id])
    row != -1 and stamps[row] >= stamps[id]
  }
  action init = s' = { written: Set(1),
    sourceRows: Map(0 -> 0, 1 -> 1), scan: Map(0 -> -1, 1 -> -1),
    captured: Set(), acked: Set(), scanDone: false, rows: Map(0 -> -1, 1 -> -1),
    accepted: Set(), cursor: 10, retained: Set(1), bootstrapHold: true,
    normalHold: false, active: false, reclaimed: false }
  // A10 predates the hold: its winner is in the source scan, not the tail.
  action writeOrigin(id: int): bool = all { Set(2, 3, 4).contains(id),
    s.written.contains(id - 1), not(s.written.contains(id)),
    s' = s.with("written", s.written.union(Set(id)))
      .with("retained", s.retained.union(Set(id)))
      .with("sourceRows", s.sourceRows.set(eventKeys[id], maximum(s.sourceRows.get(eventKeys[id]), id))) }
  action capture(key: int): bool = all { itemKeys.contains(key), not(s.captured.contains(key)),
    s' = s.with("captured", s.captured.union(Set(key)))
      .with("scan", s.scan.set(key, s.sourceRows.get(key))) }
  // ACKed pages may still have duplicates in the network. Checking the durable
  // operation-open flag and applying its row are one native transaction.
  action applyScan(key: int): bool = {
    val id = s.scan.get(key)
    all { s.captured.contains(key), not(s.active) or BROKEN == "acceptLateScan",
      s' = s.with("rows", s.rows.set(key, maximum(s.rows.get(key), id)))
        .with("accepted", if (id == -1) s.accepted else s.accepted.union(Set(id)))
        .with("acked", s.acked.union(Set(key)))
        .with("cursor", if (BROKEN == "advanceCursorFromScan" and id > 0)
          if (s.cursor > positions[id]) s.cursor else positions[id] else s.cursor) }
  }
  action finishScan = all { s.acked == itemKeys, not(s.scanDone),
    s' = s.with("scanDone", true) }
  action installNormalHold = all { s.scanDone, not(s.normalHold),
    s' = s.with("normalHold", true) }
  action dropBootstrapHold = all { s.scanDone, s.bootstrapHold,
    s.normalHold or BROKEN == "holdGap", s' = s.with("bootstrapHold", false) }
  action activate = all { s.scanDone, s.normalHold, not(s.active),
    s' = s.with("active", true) }
  action trim = {
    val hold = if (s.bootstrapHold) 10 else if (s.normalHold) s.cursor else 90
    s' = s.with("retained", s.retained.filter(id => positions[id] > hold))
  }
  // Sparse A positions are ordered complete intervals, never dense counters.
  action deliverTarget(id: int): bool = all { Set(2, 3, 4).contains(id),
    s.retained.contains(id), s.cursor == positions[id - 1],
    s' = s.with("cursor", positions[id]).with("accepted", s.accepted.union(Set(id)))
      .with("rows", s.rows.set(eventKeys[id], maximum(s.rows.get(eventKeys[id]), id))) }
  // This isolates scan closure; pair delete-GC qualification is a separate model.
  action reclaim = all { s.active, s.cursor == 90, s.rows.get(0) == 4,
    not(s.reclaimed), s' = s.with("rows", s.rows.set(0, -1)).with("reclaimed", true) }
  action step = {
    nondet id = Set(1, 2, 3, 4).oneOf()
    nondet key = itemKeys.oneOf()
    any { writeOrigin(id), capture(key), applyScan(key), finishScan,
      installNormalHold, dropBootstrapHold, activate, trim, deliverTarget(id), reclaim }
  }
  // Independent accepted-event oracle; physical absence after delete GC is valid.
  val visibleWinners = itemKeys.forall(key => {
    val eligible = s.accepted.filter(id => eventKeys[id] == key)
    val winners = eligible.filter(id => not(eligible.exists(other => stamps[other] > stamps[id])))
    val expected = winners.filter(id => id != 4)
    val row = s.rows.get(key)
    val actual = if (row == -1 or row == 4) Set() else Set(row)
    actual == expected
  })
  // After scan ACKs, every missing winner needs retained post-cursor origin data.
  // This checks recoverability, not synchronous agreement at activation.
  val noLostCoverage = not(s.scanDone) or s.written.forall(id =>
    isCovered(s.rows, id) or (s.reclaimed and eventKeys[id] == 0 and stamps[id] <= 90)
      or (positions[id] > s.cursor and s.retained.contains(id)))
  val inv = visibleWinners and noLostCoverage
  val activeLag = s.active and s.cursor == 10 and s.written.contains(3)
    and not(isCovered(s.rows, 2)) and isCovered(s.rows, 3) and inv
  val converged = s.active and s.cursor == 90 and s.rows.get(1) == 3 and inv
  val duplicateClosed = s.reclaimed and s.rows.get(0) == -1 and inv
  val handoffTrim = s.scanDone and not(s.bootstrapHold) and s.normalHold
    and s.retained.contains(2) and s.cursor == 10 and inv
  run fuzzyActiveLagTest = init.then(capture(0)).then(writeOrigin(2))
    .then(writeOrigin(3)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan).then(installNormalHold)
    .then(activate).expect(activeLag)
  run fuzzyTailConvergesTest = init.then(capture(0)).then(writeOrigin(2))
    .then(writeOrigin(3)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan).then(installNormalHold)
    .then(activate).then(writeOrigin(4)).then(deliverTarget(2)).then(deliverTarget(3))
    .then(deliverTarget(4)).expect(converged)
  run continuousHoldTest = init.then(capture(0)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan).then(writeOrigin(2))
    .then(installNormalHold).then(dropBootstrapHold).then(trim).expect(handoffTrim)
  run closedAfterReclaimTest = init.then(capture(0)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan).then(installNormalHold)
    .then(activate).then(writeOrigin(2)).then(writeOrigin(3)).then(writeOrigin(4))
    .then(deliverTarget(2)).then(deliverTarget(3)).then(deliverTarget(4)).then(reclaim)
    .expect(duplicateClosed)
}
module regional_bootstrap_closure { import regional_bootstrap_closure_model(BROKEN = "none").* }
module regional_bootstrap_closure_acceptLateScan {
  import regional_bootstrap_closure_model(BROKEN = "acceptLateScan").*
  run resurrectionTest = init.then(capture(0)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan).then(installNormalHold)
    .then(activate).then(writeOrigin(2)).then(writeOrigin(3)).then(writeOrigin(4))
    .then(deliverTarget(2)).then(deliverTarget(3)).then(deliverTarget(4)).then(reclaim)
    .then(applyScan(0)).expect(not(inv) and s.rows.get(0) == 0)
}
module regional_bootstrap_closure_advanceCursorFromScan {
  import regional_bootstrap_closure_model(BROKEN = "advanceCursorFromScan").*
  run skippedXTest = init.then(capture(0)).then(writeOrigin(2))
    .then(writeOrigin(3)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan)
    .expect(not(inv) and s.cursor == 30 and s.rows.get(0) == 0)
}
module regional_bootstrap_closure_holdGap {
  import regional_bootstrap_closure_model(BROKEN = "holdGap").*
  run trimmedXTest = init.then(capture(0)).then(capture(1))
    .then(applyScan(0)).then(applyScan(1)).then(finishScan).then(writeOrigin(2))
    .then(dropBootstrapHold).then(trim).then(installNormalHold)
    .expect(not(inv) and not(s.retained.contains(2)))
}
