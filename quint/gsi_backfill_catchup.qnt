/// Shared GSI operation contract. Each page supplies protected observations;
/// its projection changes, phase, cursor, and retained frontier commit together.
/// Physical history acquisition, lineage splits, and image ownership are proved
/// separately by gsi_history_lifetime and real-provider conformance.
module gsi_backfill_catchup {
  type Row = { version: int, live: bool }
  type Source = { version: int, index: int }
  type Effect = { entity: int, before: int, after: int, version: int }
  type Observation = { key: int, version: int, live: bool }
  type Build = {
    phase: str, cursor: int, high: int, pending: bool, scan: int, hold: int,
    rows: int -> Row, sources: int -> Source, history: List[Effect],
    historyAvailable: bool, currentTarget: bool,
    beforeRows: int -> Row, observations: List[Observation], beforeCursor: int,
    writes: List[Observation], event: str, outcome: str, lastMutation: str,
  }
  pure val ITEMS = Set(0, 1)
  pure val KEYS = Set(0, 1, 2, 3)
  pure val emptyRows = KEYS.mapBy(_ => { version: 0, live: false })
  pure val initial: Build = {
    phase: "absent", cursor: 0, high: 0, pending: false, scan: 0, hold: -1,
    rows: emptyRows, sources: ITEMS.mapBy(_ => { version: 0, index: -1 }), history: List(),
    historyAvailable: true, currentTarget: true,
    beforeRows: emptyRows, observations: List(), beforeCursor: 0,
    writes: List(), event: "init", outcome: "idle", lastMutation: "none",
  }
  var state: Build
  action init = state' = initial

  pure def begin(s: Build, event: str): Build = s
    .with("beforeRows", s.rows).with("beforeCursor", s.cursor)
    .with("observations", List()).with("writes", List())
    .with("event", event).with("outcome", "idle")

  pure def effects(e: Effect): List[Observation] = {
    val removed = if (e.before >= 0 and e.before != e.after)
      List({ key: e.entity * 2 + e.before, version: e.version, live: false }) else List()
    if (e.after >= 0)
      removed.append({ key: e.entity * 2 + e.after, version: e.version, live: true })
    else removed
  }

  pure def page(s: Build, through: int): List[Observation] =
    s.history.slice(s.cursor, through).foldl(List(), (observations, e) => observations.concat(effects(e)))

  pure def conflict(rows: int -> Row, observations: List[Observation]): bool =
    observations.indices().exists(i => {
      val o = observations.nth(i)
      val old = rows.get(o.key)
      old.version > 0 and old.version == o.version and old.live != o.live
    })

  /// Select writes against the protected pre-page snapshot. A missing delete
  /// emits no row unless this same page also inserts that key at an older revision.
  pure def requiredWrites(rows: int -> Row, observations: List[Observation]): List[Observation] = {
    val eligible = observations.select(o => {
      val old = rows.get(o.key)
      val pairedPut = old.version == 0 and observations.indices().exists(i => {
        val inserted = observations.nth(i)
        inserted.key == o.key and inserted.live and inserted.version < o.version
      })
      o.version > old.version and (o.live or old.live or pairedPut)
    })
    eligible.foldl(List(), (writes, o) =>
      if (writes.indices().exists(i => writes.nth(i).key == o.key))
        writes.foldl(List(), (updated, w) => updated.append(if (w.key == o.key and w.version <= o.version) o else w))
      else writes.append(o))
  }

  pure def apply(rows: int -> Row, writes: List[Observation]): int -> Row =
    writes.foldl(rows, (result, w) => result.set(w.key, { version: w.version, live: w.live }))

  pure def observe(s: Build, observations: List[Observation]): Build = {
    val read = s.with("observations", observations)
    if (not(s.historyAvailable)) read.with("phase", "blocked").with("high", 0)
      .with("pending", false).with("scan", 0).with("outcome", "history_blocked")
    else if (conflict(s.rows, observations)) read.with("phase", "blocked").with("high", 0)
      .with("pending", false).with("scan", 0).with("outcome", "projection_blocked")
    else {
      val writes = requiredWrites(s.rows, observations)
      read.with("writes", writes).with("rows", apply(s.rows, writes)).with("outcome", "persist")
    }
  }

  pure def mutate(s: Build, entity: int, index: int): Build = {
    val source = s.sources.get(entity)
    val version = source.version + 2
    begin(s, "source").with("sources", s.sources.set(entity, { version: version, index: index }))
      .with("history", s.history.append({ entity: entity, before: source.index, after: index, version: version }))
      .with("lastMutation", if (index < 0) "delete" else if (source.index >= 0 and source.index != index) "move" else "put")
  }

  pure def capture(s: Build): Build = begin(s, "capture")
    .with("phase", "scanning").with("cursor", s.history.length()).with("hold", s.history.length())
    .with("high", 0).with("scan", 0).with("pending", false)
    .with("rows", emptyRows).with("currentTarget", true).with("historyAvailable", true)
    .with("outcome", "persist")

  pure def scan(s: Build): Build = {
    val source = s.sources.get(s.scan)
    val observations = if (source.index < 0) List() else
      List({ key: s.scan * 2 + source.index, version: source.version, live: true })
    val evaluated = observe(begin(s, "scan"), observations)
    if (evaluated.phase == "blocked") evaluated else evaluated.with("scan", s.scan + 1)
  }

  pure def finishScan(s: Build): Build = {
    val evaluated = observe(begin(s, "finish_scan"), List())
    if (evaluated.phase == "blocked") evaluated else evaluated.with("phase", "catching_up")
      .with("scan", 0).with("high", s.history.length())
  }

  pure def readStream(s: Build, limit: int): Build = {
    val high = if (s.phase == "catching_up" or s.pending) s.high else s.history.length()
    val through = if (s.cursor + limit < high) s.cursor + limit else high
    val evaluated = observe(begin(s, "stream"), page(s, through))
    if (evaluated.phase == "blocked") evaluated else {
      val active = s.phase == "active"
      evaluated.with("cursor", through).with("hold", through).with("high", high)
        .with("pending", active and through < high)
        .with("outcome", if (active and through == s.cursor and not(s.pending)) "idle" else "persist")
    }
  }

  pure def activate(s: Build): Build = {
    val evaluated = observe(begin(s, "activate"), List())
    if (evaluated.phase == "blocked") evaluated
    else if (not(s.currentTarget)) evaluated.with("outcome", "superseded")
    else evaluated.with("phase", "active")
  }

  action ConcurrentPut = {
    nondet entity = ITEMS.oneOf()
    nondet index = Set(0, 1).oneOf()
    all { state.history.length() < 6, state' = mutate(state, entity, index) }
  }
  action ConcurrentDelete = {
    nondet entity = ITEMS.oneOf()
    all { state.history.length() < 6, state.sources.get(entity).index >= 0, state' = mutate(state, entity, -1) }
  }
  action KeyMove = {
    nondet entity = ITEMS.oneOf()
    all { state.history.length() < 6, state.sources.get(entity).index >= 0,
      state' = mutate(state, entity, 1 - state.sources.get(entity).index) }
  }
  action AbsentDelete = {
    nondet entity = ITEMS.oneOf()
    all { state.sources.get(entity).index < 0, state' = begin(state, "absent_delete") }
  }
  action CaptureBoundary = all { state.phase == "absent", state' = capture(state) }
  action ScanObservation = all { state.phase == "scanning", state.scan < 2, state' = scan(state) }
  action MarkScanComplete = all { state.phase == "scanning", state.scan == 2, state' = finishScan(state) }
  action StreamObservation = all {
    state.phase == "active" or (state.phase == "catching_up" and state.cursor < state.high),
    state' = readStream(state, 1),
  }
  action FinalDrain = all {
    state.phase == "active" or (state.phase == "catching_up" and state.cursor < state.high),
    state' = readStream(state, 2),
  }
  action Activate = all { state.phase == "catching_up", state.cursor == state.high, state' = activate(state) }
  action MarkMissingHistory = all {
    state.phase != "absent", state.historyAvailable,
    state' = begin(state, "lose_history").with("historyAvailable", false),
  }
  action TrimRequiredHistory = MarkMissingHistory
  action RetainRequiredHistory = all {
    state.phase != "absent", state.historyAvailable, state' = begin(state, "retain_history"),
  }
  action RecreateTarget = all {
    state.phase != "absent", state.currentTarget,
    state' = begin(state, "recreate_target").with("currentTarget", false),
  }
  // Only deleting the build releases its hold. Activation retains the active cursor.
  action ReleaseBoundary = all {
    state.phase == "active",
    state' = begin(state, "delete_build").with("phase", "absent").with("cursor", 0)
      .with("high", 0).with("pending", false).with("scan", 0).with("hold", -1).with("rows", emptyRows),
  }
  action Restart = state' = begin(state, "restart")

  /// An intact source cannot invent an equal-version conflicting image. Supply
  /// one explicitly to prove the decoded-observation boundary fails closed.
  action ConflictingObservation = {
    nondet key = KEYS.oneOf()
    val old = state.rows.get(key)
    val observations = List({ key: key, version: old.version, live: not(old.live) })
    all { state.phase == "scanning", old.version > 0,
      state' = observe(begin(state, "adversarial"), observations) }
  }

  action step = any {
    ConcurrentPut, ConcurrentDelete, KeyMove, AbsentDelete, CaptureBoundary,
    ScanObservation, MarkScanComplete, StreamObservation, FinalDrain, Activate,
    MarkMissingHistory, RetainRequiredHistory, RecreateTarget, ReleaseBoundary,
    Restart, ConflictingObservation,
  }

  val scanRequiresBoundary = state.phase == "absent" or state.hold >= 0
  val missingHistoryFailsClosed = state.event != "activate" or state.historyAvailable or state.phase == "blocked"
  pure def snapshot(s: Build, through: int): int -> Source = s.history.slice(0, through)
    .foldl(initial.sources, (sources, e) => sources.set(e.entity, { version: e.version, index: e.after }))
  val activationGate = state.event != "activate" or state.phase != "active" or all {
    state.cursor == state.high,
    KEYS.forall(key => state.rows.get(key).live == (snapshot(state, state.high).get(key / 2).index == key % 2)),
    KEYS.forall(key => not(state.rows.get(key).live) or
      state.rows.get(key).version == snapshot(state, state.high).get(key / 2).version),
  }
  val workerCursorRespectsOrderedEffects = all {
    state.cursor <= state.history.length(),
    state.phase != "catching_up" or state.cursor <= state.high,
    state.phase != "active" or not(state.pending) or state.cursor < state.high,
    state.phase == "absent" or state.hold == state.cursor,
  }
  val projectedRowsMatchLatestAppliedVersion = all {
    KEYS.forall(key => state.rows.get(key).version <= state.sources.get(key / 2).version),
    Set("capture", "delete_build").contains(state.event) or
      all {
        state.rows == apply(state.beforeRows, state.writes),
        KEYS.forall(key => state.rows.get(key).version >= state.beforeRows.get(key).version),
      },
    not(conflict(state.beforeRows, state.observations)) or all {
      state.phase == "blocked", state.writes == List(), state.rows == state.beforeRows,
    },
  }
  val physicalRowsMatchLatestAppliedVersion = projectedRowsMatchLatestAppliedVersion
  val absentDeleteIsGuarded = state.event != "absent_delete" or state.writes == List()
  val streamObservationIsGuarded = state.event != "stream" or all {
    state.phase == "blocked" or state.observations == page(state.with("cursor", state.beforeCursor), state.cursor),
    state.beforeCursor <= state.cursor,
  }
  val catchupSafety = all {
    scanRequiresBoundary, missingHistoryFailsClosed, activationGate,
    workerCursorRespectsOrderedEffects, projectedRowsMatchLatestAppliedVersion,
    absentDeleteIsGuarded, streamObservationIsGuarded,
  }
  val inv = catchupSafety

  val boundary_capture_witness = state.event == "capture"
  val required_history_retained_witness = state.event == "retain_history"
  val required_history_trimmed_witness = state.outcome == "history_blocked"
  val missing_stream_history_rejected_witness = required_history_trimmed_witness
  val boundary_release_witness = state.event == "delete_build"
  val activation_witness = state.event == "activate" and state.phase == "active"
  val conflicting_observation_witness = state.outcome == "projection_blocked"
  val scan_stream_race_witness = state.event == "stream" and state.observations.indices().exists(i => {
    val o = state.observations.nth(i)
    o.version < state.beforeRows.get(o.key).version
  })
  val stream_delete_beats_stale_scan_witness = KEYS.exists(k => state.rows.get(k).version > 0 and not(state.rows.get(k).live))
  val gsi_key_move_witness = state.lastMutation == "move" and state.event == "stream" and state.writes.length() == 2
  val queued_worker_drain_witness = state.event == "stream" and state.cursor > state.beforeCursor

  pure val inserted = { entity: 0, before: -1, after: 0, version: 2 }
  pure val removed = { entity: 0, before: 0, after: -1, version: 4 }
  pure val move = { entity: 0, before: 0, after: 1, version: 4 }
  pure val prepared = capture(initial).with("sources", initial.sources.set(0, {version: 4, index: 1}))
    .with("history", List(inserted, move)).with("phase", "catching_up").with("high", 2)
  pure val liveRows = emptyRows.set(0, { version: 2, live: true })

  action unsafeBoundaryStep = state' = capture(initial).with("hold", -1)
  action unsafeCasStep = state' = begin(prepared.with("rows", liveRows), "adversarial")
    .with("observations", List({ key: 0, version: 2, live: false })).with("phase", "active")
  action unsafeAbsentDeleteStep = state' = begin(initial, "absent_delete")
    .with("writes", List({ key: 0, version: 2, live: false }))
  action unsafeAbsentStreamObservationStep = state' = begin(initial, "stream")
    .with("observations", List({ key: 0, version: 2, live: true }))
  action unsafeTombstoneStep = state' = readStream(prepared.with("history", List(inserted, removed))
    .with("sources", initial.sources.set(0, { version: 4, index: -1 })), 2).with("rows", liveRows)
  action unsafeKeyMoveStep = state' = readStream(prepared, 2)
    .with("rows", readStream(prepared, 2).rows.set(0, { version: 2, live: true }))
  action unsafeMissingHistoryStep = state' = begin(prepared, "activate")
    .with("historyAvailable", false).with("phase", "active")
  action unsafeActivationStep = state' = begin(prepared, "activate").with("phase", "active")
  action unsafeQueueStep = state' = begin(prepared, "stream").with("cursor", 2).with("hold", 2)

  run pairedInsertDeleteRetainsVersionTest = {
    val observed = effects(inserted).concat(effects(removed))
    val writes = requiredWrites(emptyRows, observed)
    all { assert(writes == List({ key: 0, version: 4, live: false })) }
  }
  run keyMoveRemovesOldProjectionTest = {
    val result = readStream(prepared, 2)
    all { assert(not(result.rows.get(0).live)), assert(result.rows.get(1).live), assert(result.cursor == 2) }
  }
  run activeHistoryRemainsHeldTest = {
    val result = activate(readStream(prepared, 2))
    all { assert(result.phase == "active"), assert(result.hold == 2) }
  }
  run capturedHighDoesNotChaseLaterWritesTest = {
    val active = activate(readStream(prepared, 2))
    val published = mutate(mutate(active, 1, 0), 1, 1)
    val partial = readStream(published, 1)
    val later = mutate(partial, 0, 0)
    val drained = readStream(later, 2)
    all { assert(partial.pending), assert(drained.cursor == 4), assert(drained.history.length() == 5) }
  }
}
