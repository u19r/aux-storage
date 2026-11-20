/// Small executable characterisation; not a native Postgres/restore proof.
/// One key, two writers, two ordinals; startup excludes all writers.
module postgres_stream_positions {
  type State = { mode: str, phase: int, next: int, first: int, second: int,
    bias: int, floor: int, cursor: int, high: int, entries: Set[int],
    failedXid: int, oldXid: int, restored: bool }
  var s: State
  pure val width = 65536
  pure def initial(mode) = { mode: mode, phase: 0, next: 10, first: 0, second: 0,
    bias: 0, floor: 0, cursor: 0, high: 0, entries: Set(),
    failedXid: 0, oldXid: 0, restored: false }
  action init = s' = initial("late")
  action initEarly = s' = initial("early")
  action initRestore = s' = initial("restore")
  action initClamp = s' = initial("clamp")
  action initUnbiased = s' = initial("unbiased")
  action initMissingRetired = s' = initial("missingRetired")
  action initNoOffset = s' = initial("noOffset")
  action initQuiescent = s' = initial("quiescent")
  action initHole = s' = initial("hole")
  def position(x, ordinal) = (x + s.bias + (if (s.mode == "noOffset") 0 else 1)) * width + ordinal
  val frontier = (if (s.oldXid > 0) s.oldXid else s.next) * width +
    (if (s.mode == "unbiased") 0 else s.bias * width)

  // Failed condition after ordering acquires no xid. It cannot hold xmin back.
  action rejectCondition = all { s.phase == 0,
    s' = s.with("phase", 1).with("failedXid", 0) }
  // Early variant: A gets xid then waits; B locks/commits first. Late variant:
  // A has no xid while waiting, so B gets the first xid after locking the key.
  action waitForItem = all { s.phase == 1,
    s' = s.with("phase", 2).with("first", if (s.mode == "early") 10 else 0)
      .with("next", if (s.mode == "early") 11 else 10) }
  action commitFirstHolder = all { s.phase == 2,
    s' = s.with("phase", 3).with("second", s.next).with("next", s.next + 1) }
  action commitWaitingWriter = all { s.phase == 3,
    s' = s.with("phase", 4).with("first", if (s.first == 0) s.next else s.first)
      .with("next", if (s.first == 0) s.next + 1 else s.next) }

  // Abstract unrelated transactions advance next xid to 100 before capture.
  // A genuinely empty stream can still issue a closed high in an xid hole.
  // The external cursor is deliberately not a durable database authority.
  action captureBeforeRestore = all { s.phase == 4,
    s' = s.with("phase", 5).with("next", 100).with("high", 100 * width - 1)
      .with("cursor", 100 * width - 1)
      .with("floor", if (s.mode == "hole") 0 else 100 * width - 1)
      .with("entries", if (s.mode == "hole" or s.mode == "missingRetired") Set() else Set(100 * width - 1)) }
  action restoreAndOpen = {
    val authority = if (s.mode == "missingRetired" or s.mode == "hole") 0 else s.floor
    val raised = if (authority == 0) 0 else (authority + width - 1) / width - 1
    all { s.phase == 5,
      s' = s.with("phase", 6).with("next", 2).with("oldXid", 1)
        .with("bias", if (s.mode == "clamp") 0 else raised)
        .with("restored", true).with("entries", Set()).with("high", 0) }
  }
  action restoredWriters = {
    val a = if (s.mode == "clamp") s.floor else position(2, 0)
    val b = if (s.mode == "clamp") s.floor else position(3, 0)
    all { s.phase == 6,
      s' = s.with("phase", 7).with("first", a).with("second", b)
        .with("next", 4).with("entries", Set(a, a + 1, b, b + 1)) }
  }
  // An old assigned xid delays visibility, although both writers committed.
  // One additional xid advances next to 5: p includes +1, so next=4 alone
  // leaves the second writer at F and is insufficient for strict visibility.
  action finishOldTransaction = all { s.phase == 7,
    s' = s.with("phase", 8).with("oldXid", 0).with("next", if (s.mode == "noOffset" or s.mode == "quiescent") 4 else 5) }
  action captureRestoredHigh = all { s.phase == 8,
    s' = s.with("phase", 9).with("high", frontier - 1) }
  // Once unrelated transactions consume xids, an initially rejected cursor is
  // accepted again. Range reads still start strictly after that cursor.
  action catchUp = all { s.phase == 9,
    s' = s.with("phase", 10).with("next", 102)
      .with("high", (102 + s.bias) * width - 1) }
  action step = any { rejectCondition, waitForItem, commitFirstHolder,
    commitWaitingWriter, captureBeforeRestore, restoreAndOpen, restoredWriters,
    finishOldTransaction, captureRestoredHigh, catchUp, s' = s }
  val itemOrder = s.phase < 4 or s.restored or s.first > s.second
  val failedHasNoXid = s.failedXid == 0
  val distinct = s.phase < 7 or s.first != s.second
  val aboveStoredAuthority = s.phase < 7 or s.entries.forall(p => p > s.floor)
  val visibleAfterOldEnds = s.phase < 8 or s.entries.forall(p => p < frontier)
  val noCursorSkip = s.phase < 10 or s.entries.forall(p => p > s.cursor)
  val inv = itemOrder and failedHasNoXid and distinct and aboveStoredAuthority and visibleAfterOldEnds
  val lateOrder = s.phase == 4 and itemOrder
  val raisedWriters = s.phase == 7 and s.bias > 0 and distinct and aboveStoredAuthority
  val oldDelays = s.phase == 7 and s.entries.forall(p => p >= frontier)
  val allVisible = s.phase == 9 and visibleAfterOldEnds
  val cursorInitiallyRejected = s.phase == 9 and s.cursor > s.high
  val cursorAcceptedButSkips = s.phase == 10 and s.cursor <= s.high and not(noCursorSkip)
  action throughOrder = rejectCondition.then(waitForItem).then(commitFirstHolder).then(commitWaitingWriter)
  action throughOpen = throughOrder.then(captureBeforeRestore).then(restoreAndOpen)
  action throughRead = throughOpen.then(restoredWriters).then(finishOldTransaction).then(captureRestoredHigh)
  run lateOrderTest = init.then(throughOrder).expect(lateOrder and inv)
  run earlyOrderNegativeTest = initEarly.then(throughOrder).expect(not(itemOrder))
  run raisedRestoreTest = initRestore.then(throughOpen).then(restoredWriters)
    .expect(raisedWriters and oldDelays and inv).then(finishOldTransaction)
    .then(captureRestoredHigh).expect(allVisible and inv)
  run clampNegativeTest = initClamp.then(throughOpen).then(restoredWriters).expect(not(distinct))
  run unbiasedNegativeTest = initUnbiased.then(throughRead).expect(not(visibleAfterOldEnds))
  run retiredFloorNegativeTest = initMissingRetired.then(throughRead).expect(not(aboveStoredAuthority))
  run noOffsetQuiescentTest = initNoOffset.then(throughRead).expect(allVisible and inv)
  run plusOneQuiescentNegativeTest = initQuiescent.then(throughRead).expect(not(visibleAfterOldEnds))
  run holeCursorNegativeTest = initHole.then(throughRead).expect(cursorInitiallyRejected)
    .then(catchUp).expect(cursorAcceptedButSkips)
}
