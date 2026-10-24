/// Design-level protocol model. This checks abstract transitions and assumptions,
/// not production implementation, provider atomicity, or crash durability.
/// P1 source and image lifetime contract. Each transition is one committed
/// native transaction. Allocation and rollback are separate from commit.
/// Native lock/conflict tests must prove that concurrent attempts refine these
/// atomic transitions; this model does not claim that drivers do so already.
module gsi_history_lifetime {
  type Record = { id: int, lineage: int, offset: int, incarnation: int, version: int }
  type History = {
    allocated: int, pending: Set[int], records: List[Record], retained: Set[int],
    tableOwners: Set[int], systemOwners: Set[int], images: Set[(int, int)],
    incarnation: int, split: bool, tails: int -> int,
    phase: str, frontier: Set[int], progress: int -> int,
    holdFrontier: Set[int], holds: int -> int, high: int -> int, highFrontier: Set[int],
    hasHigh: bool, scanComplete: bool, applied: Set[int], owed: Set[int], required: Set[int],
  }
  pure val LINEAGES = Set(0, 1, 2, 3)
  pure val baseline = Map(0 -> 0, 1 -> 0, 2 -> 6, 3 -> 6)
  pure def initial: History = {
    allocated: 0, pending: Set(), records: List(), retained: Set(),
    tableOwners: Set(), systemOwners: Set(), images: Set(), incarnation: 1,
    split: false, tails: baseline, phase: "absent", frontier: Set(),
    progress: baseline, holdFrontier: Set(), holds: baseline, high: baseline, highFrontier: Set(),
    hasHigh: false, scanComplete: false, applied: Set(), owed: Set(), required: Set(),
  }
  var state: History
  action init = state' = initial

  pure def leaves(s: History): Set[int] =
    if (s.split) Set(1, 2, 3) else Set(0, 1)

  pure def allocate(s: History): History = s
    .with("allocated", s.allocated + 1)
    .with("pending", s.pending.union(Set(s.allocated + 1)))

  pure def latest(s: History, incarnation: int): int =
    s.records.foldl(0, (v, r) =>
      if (r.incarnation == incarnation and r.version > v) r.version else v)

  pure def predecessor(s: History, r: Record): int =
    s.records.foldl(0, (v, old) =>
      if (old.incarnation == r.incarnation and old.version < r.version and old.version > v)
        old.version else v)

  pure def commit(s: History, id: int, lineage: int): History = {
    val r = { id: id, lineage: lineage, offset: s.tails.get(lineage) + 1,
      incarnation: s.incarnation, version: latest(s, s.incarnation) + 2 }
    s.with("pending", s.pending.exclude(Set(id)))
      .with("records", s.records.append(r))
      .with("owed", if (s.phase == "absent") s.owed else s.owed.union(Set(id)))
      .with("required", if (s.phase == "absent") s.required else s.required.union(Set(id)))
      .with("retained", s.retained.union(Set(id)))
      .with("tableOwners", s.tableOwners.union(Set(id)))
      .with("systemOwners", s.systemOwners.union(Set(id)))
      .with("images", s.images.union(Set((r.incarnation, r.version))))
      .with("tails", s.tails.set(lineage, r.offset))
  }

  pure def capture(s: History): History = s
    .with("phase", "scanning").with("scanComplete", false).with("frontier", leaves(s))
    .with("progress", s.tails).with("holdFrontier", leaves(s))
    .with("holds", s.tails)

  pure def captureHigh(s: History): History = s
    .with("phase", if (s.phase == "scanning") "catching_up" else s.phase)
    .with("high", s.tails).with("highFrontier", leaves(s)).with("hasHigh", true)

  // A parent hold protects every descendant, even before the worker crosses
  // the seal. The split itself changes no build rows.
  pure def protects(frontier: Set[int], positions: int -> int, r: Record): bool =
    (frontier.contains(r.lineage) and r.offset > positions.get(r.lineage)) or
      (r.lineage >= 2 and frontier.contains(0))

  pure def needed(s: History, r: Record): bool =
    protects(s.frontier, s.progress, r)

  pure def hasImages(s: History, r: Record): bool = all {
    s.images.contains((r.incarnation, r.version)),
    predecessor(s, r) == 0 or s.images.contains((r.incarnation, predecessor(s, r))),
  }

  pure def consume(s: History, lineage: int): History = {
    val offset = s.progress.get(lineage) + 1
    val rows = s.records.select(r => r.lineage == lineage and r.offset == offset)
    if (rows.length() != 1) s.with("phase", "blocked")
    else {
      val r = rows.nth(0)
      if (not(s.retained.contains(r.id)) or not(hasImages(s, r)))
        s.with("phase", "blocked")
      else s.with("progress", s.progress.set(lineage, offset))
        .with("holds", s.holds.set(lineage, offset))
        .with("applied", s.applied.union(Set(r.id)))
        .with("owed", s.owed.exclude(Set(r.id)))
    }
  }

  pure def crossSeal(s: History): History = {
    val frontier = s.frontier.exclude(Set(0)).union(Set(2, 3))
    s.with("frontier", frontier).with("holdFrontier", frontier)
      .with("progress", s.progress.set(2, 6).set(3, 6))
      .with("holds", s.holds.set(2, 6).set(3, 6))
  }

  // Explicit deletion removes the build itself. It is not hold release for a
  // live build. The hold-only mutation below must still fail.
  pure def deleteBuild(s: History): History = s.with("phase", "absent")
    .with("frontier", Set()).with("holdFrontier", Set()).with("hasHigh", false)
    .with("owed", Set()).with("required", Set()).with("applied", Set())

  pure def trimSystem(s: History): History = {
    val keep = s.records.select(r => s.retained.contains(r.id) and
      protects(s.holdFrontier, s.holds, r)).foldl(Set(), (ids, r) => ids.union(Set(r.id)))
    s.with("retained", keep).with("systemOwners", keep)
  }

  pure def trimTable(s: History, id: int): History =
    s.with("tableOwners", s.tableOwners.exclude(Set(id)))

  pure def imageFloor(s: History, incarnation: int): int = {
    val owners = s.tableOwners.union(s.systemOwners)
    s.records.foldl(latest(s, incarnation), (floor, r) => {
      val before = predecessor(s, r)
      val required = if (before == 0) r.version else before
      if (r.incarnation == incarnation and owners.contains(r.id) and required < floor)
        required else floor
    })
  }

  pure def trimImages(s: History): History = s.with("images",
    s.images.filter(image => image._2 >= imageFloor(s, image._1)))

  pure def safe(s: History): bool = all {
    s.phase != "active" or s.scanComplete,
    s.frontier == s.holdFrontier,
    s.frontier.forall(l => s.progress.get(l) == s.holds.get(l)),
    s.frontier.forall(l => s.progress.get(l) <= s.tails.get(l)),
    s.frontier.forall(l => s.progress.get(l) >= baseline.get(l)),
    s.retained == s.systemOwners,
    s.required == s.owed.union(s.applied),
    s.owed.intersect(s.applied).size() == 0,
    s.owed.forall(id => s.retained.contains(id)),
    s.records.indices().forall(i => {
      val r = s.records.nth(i)
      s.owed.contains(r.id) == needed(s, r)
    }),
    s.records.indices().forall(i => {
      val r = s.records.nth(i)
      not(s.tableOwners.contains(r.id) or s.systemOwners.contains(r.id)) or hasImages(s, r)
    }),
    Set(1, 2).forall(i => latest(s, i) == 0 or s.images.contains((i, latest(s, i)))),
  }
  val inv = safe(state)

  action Allocate = all { state.allocated < 8, state' = allocate(state) }
  action CommitAt(id: int, lineage: int): bool = all {
    state.pending.contains(id), leaves(state).contains(lineage),
    state' = commit(state, id, lineage),
  }
  action Commit = {
    nondet id = state.pending.oneOf()
    nondet lineage = leaves(state).oneOf()
    CommitAt(id, lineage)
  }
  action Rollback = {
    nondet id = state.pending.oneOf()
    state' = state.with("pending", state.pending.exclude(Set(id)))
  }
  action Capture = all { state.phase == "absent", state' = capture(state) }
  action FinishScan = all {
    state.phase == "scanning", state' = state.with("scanComplete", true),
  }
  action CaptureHigh = all {
    state.scanComplete,
    Set("scanning", "active").contains(state.phase), not(state.hasHigh),
    state' = captureHigh(state),
  }
  action ConsumeAt(lineage: int): bool = all {
      state.frontier.contains(lineage),
      Set("catching_up", "active").contains(state.phase), state.hasHigh,
      state.progress.get(lineage) < state.high.get(lineage),
      state' = consume(state, lineage),
  }
  action Consume = {
    nondet lineage = state.frontier.oneOf()
    ConsumeAt(lineage)
  }
  action CompletePass = all {
    state.hasHigh, state.phase != "blocked", state.scanComplete,
    state.frontier == state.highFrontier,
    state.frontier.forall(l => state.progress.get(l) == state.high.get(l)),
    state' = state.with("phase", "active").with("hasHigh", false),
  }
  action Split = all { not(state.split), state' = state.with("split", true) }
  action CrossSeal = all {
    state.split, state.frontier.contains(0),
    not(state.hasHigh) or state.highFrontier.contains(2),
    state.progress.get(0) == state.tails.get(0), state' = crossSeal(state),
  }
  action DeleteBuild = all { state.phase != "absent", state' = deleteBuild(state) }
  action TrimSystem = state' = trimSystem(state)
  action TrimTable = {
    nondet id = state.tableOwners.oneOf()
    state' = trimTable(state, id)
  }
  action TrimImages = state' = trimImages(state)
  action Recreate = all {
    state.incarnation == 1,
    state' = deleteBuild(state).with("incarnation", 2),
  }
  action step = any {
    Allocate, Commit, Rollback, Capture, FinishScan, CaptureHigh, Consume, CompletePass,
    Split, CrossSeal, DeleteBuild, TrimSystem, TrimTable, TrimImages, Recreate,
  }

  // These traces use the same transitions as exploration, with deterministic
  // schedules for rare races and explicit bad-design controls.
  pure def heldUpdate = commit(commit(capture(allocate(allocate(initial))), 1, 0), 2, 0)
    .with("scanComplete", true)
  pure def splitHeld = commit(allocate(heldUpdate.with("split", true)), 3, 2)

  run delayedPhysicalAllocationTest = {
    val committed = commit(commit(capture(allocate(allocate(initial))), 2, 0), 1, 0)
      .with("scanComplete", true)
    val drained = consume(consume(captureHigh(committed), 0), 0)
    all {
      assert(safe(drained)), assert(drained.applied == Set(1, 2)),
      assert(committed.records.nth(1).id < committed.records.nth(0).id),
      assert(not(safe(committed.with("progress", committed.tails).with("holds", committed.tails).with("owed", Set())))),
    }
  }
  run independentShardProgressTest = {
    val s = commit(commit(capture(allocate(allocate(initial))), 1, 0), 2, 1)
      .with("scanComplete", true)
    val page = consume(captureHigh(s), 0)
    all { assert(page.progress.get(1) == 0), assert(safe(trimSystem(page))) }
  }
  run splitBaselineAndInheritedHoldTest = {
    val drained = consume(consume(captureHigh(splitHeld), 0), 0)
    val crossed = crossSeal(drained)
    all {
      assert(safe(trimSystem(splitHeld))), assert(crossed.progress.get(2) == 6),
      assert(drained.frontier != drained.highFrontier),
      assert(crossed.progress.get(2) < crossed.high.get(2)),
      assert(consume(crossed, 2).applied.contains(3)),
      assert(safe(trimSystem(crossed))),
      assert(not(safe(crossed.with("progress", crossed.progress.set(2, 0))
        .with("holds", crossed.holds.set(2, 0))))),
    }
  }
  run pointerLifetimesAndPredecessorTest = {
    val consumed = trimSystem(consume(captureHigh(heldUpdate), 0))
    val s = trimImages(trimTable(trimTable(consumed, 1), 2))
    all {
      assert(safe(s)), assert(s.images.contains((1, 2))),
      assert(not(safe(s.with("images", s.images.exclude(Set((1, 2))))))),
      assert(s.systemOwners == Set(2)),
      assert(not(safe(s.with("systemOwners", Set())))),
    }
  }
  run earlyHoldReleaseAndConsumedTrimTest = {
    val consumed = consume(captureHigh(heldUpdate), 0)
    all {
      assert(safe(trimSystem(consumed))),
      assert(not(trimSystem(consumed).retained.contains(1))),
      assert(not(safe(trimSystem(heldUpdate.with("holdFrontier", Set()))))),
    }
  }
  run missingHistoryBlocksProgressTest = {
    val broken = captureHigh(heldUpdate.with("retained", Set(2)))
    val blocked = consume(broken, 0)
    all {
      assert(blocked.phase == "blocked"), assert(blocked.progress.get(0) == 0),
      assert(blocked.holdFrontier == broken.holdFrontier),
    }
  }
  run recreationCannotAliasRetainedImagesTest = {
    val s = commit(allocate(deleteBuild(heldUpdate).with("incarnation", 2)), 3, 1)
    val trimmed = trimImages(s)
    all {
      assert(safe(trimmed)), assert(trimmed.images.contains((1, 2))),
      assert(trimmed.images.contains((2, 2))),
      assert(not(safe(trimmed.with("images", trimmed.images.filter(i => i._1 == 2))))),
    }
  }
  run rollbackDoesNotCreateOffsetGapTest = {
    val allocated = allocate(allocate(capture(initial)))
    val rolledBack = allocated.with("pending", allocated.pending.exclude(Set(1)))
    val committed = commit(rolledBack, 2, 0)
    all { assert(committed.tails.get(0) == 1), assert(safe(consume(captureHigh(committed), 0))) }
  }
  // A deleting statement can use an earlier trim observation only if every
  // later publication is strictly above the observed latest image. New rows
  // above the old bound survive; the old latest is the new record's predecessor.
  pure def trimObserved(s: History, observed: History): History = s.with("images",
    s.images.filter(image => image._2 >= imageFloor(observed, image._1)))

  run monotonicPublicationSurvivesEarlierTrimObservationTest = {
    val observed = trimTable(trimTable(trimSystem(deleteBuild(heldUpdate)), 1), 2)
    val published = commit(allocate(observed), 3, 0)
    val trimmed = trimObserved(published, observed)
    all {
      assert(safe(trimmed)), assert(trimmed.images.contains((1, 4))),
      assert(trimmed.images.contains((1, 6))),
      // An unchecked late reference to the old image is not a permitted writer.
      assert(not(safe(trimmed.with("tableOwners", Set(1, 3))))),
    }
  }

  run splitBeforeCaptureDrainsChildBeforeActivationTest = init
    .then(Capture).then(Allocate).then(CommitAt(1, 0))
    .then(Split).then(Allocate).then(CommitAt(2, 2))
    .then(TrimSystem).expect(state.retained == Set(1, 2))
    .then(FinishScan).then(CaptureHigh).then(ConsumeAt(0))
    .expect(state.frontier != state.highFrontier)
    .then(CrossSeal).expect(state.progress.get(2) == 6)
    .then(ConsumeAt(2)).then(CompletePass)
    .expect(state.phase == "active" and state.owed.size() == 0 and
      state.applied == Set(1, 2) and inv)

}
