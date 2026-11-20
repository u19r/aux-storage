/// KV GSI build with a protected backfill (storage layout redesign N3, FoundationDB first;
/// docs/plans/incomplete/fable-w6f-storage-layout-redesign.md sections 4.1, 4.2 and 4.6).
///
/// Optimistic transactions: an attempt reads at its read version; the resolver aborts its
/// commit when a key that it read with conflict was written after that version. A writer
/// reads the item key and the metadata version key with conflict. A backfill page reads its
/// key range with conflict (protected) or as a snapshot (variant). Writers in a projecting
/// phase clear the GSI entry of the old image and put the entry of the new image. The page
/// puts the entries of what it read. No GSI tombstones, no revision guard, no catch-up.
///
/// An item value is its abstract GSI key: ABSENT (no item), NO_ATTR (an item without the
/// GSI attribute), or a positive GSI key. An index entry is (GSI key, table key).
module gsi_protected_backfill {
  type Config = { protectedScan: bool, stampCheck: bool }
  type Binding = { phase: str, metaVer: int }
  type Attempt = { busy: bool, key: int, newVal: int, rv: int, old: int, projecting: bool }
  type State = {
    cfg: Config,
    table: int -> int, lastWrite: int -> int, index: Set[(int, int)],
    version: int, phase: str, metaVer: int,
    cursor: int, pageBusy: bool, pageRv: int, pageSeen: int -> int,
    attempts: int -> Attempt, caches: int -> Binding,
    writes: int, pageAborts: int, stampRetries: int, writerAborts: int,
    writesDuringBuild: int, unprojectedCommits: int,
  }

  pure val KEYS = 1.to(3)
  pure val LAST_KEY = 3
  pure val WRITERS = 1.to(2)
  pure val PAGE = 2
  pure val ABSENT = 0
  pure val NO_ATTR = -1
  pure val VALUES = Set(ABSENT, NO_ATTR, 1, 2)
  pure val MAX_WRITES = 8
  pure val idle: Attempt = { busy: false, key: 0, newVal: 0, rv: 0, old: 0, projecting: false }

  pure def initialState(cfg: Config): State = {
    cfg: cfg,
    table: Map(1 -> 1, 2 -> 2, 3 -> ABSENT), lastWrite: KEYS.mapBy(_ => 0), index: Set(),
    version: 0, phase: "None", metaVer: 0,
    cursor: 0, pageBusy: false, pageRv: 0, pageSeen: Map(),
    attempts: WRITERS.mapBy(_ => idle), caches: WRITERS.mapBy(_ => { phase: "None", metaVer: 0 }),
    writes: 0, pageAborts: 0, stampRetries: 0, writerAborts: 0,
    writesDuringBuild: 0, unprojectedCommits: 0,
  }
  pure val design: Config = { protectedScan: true, stampCheck: true }

  var state: State
  action init = state' = initialState(design)
  action initDesign = state' = initialState(design)
  // Must fail: the page reads a snapshot; a writer that commits between the page read and the
  // page commit does not abort the page, so the page puts the entry of an old image.
  action initSnapshotScan = state' = initialState(design.with("protectedScan", false))
  // Must fail: a writer acts on its cached binding without a stamp check and without a
  // conflict read of the metadata version; it does not see the GSI and writes no entry.
  action initNoStampCheck = state' = initialState(design.with("stampCheck", false))

  pure def projecting(p: str): bool = Set("Filling", "Building", "Active").contains(p)
  pure def entries(m: int -> int): Set[(int, int)] =
    m.keys().filter(k => m.get(k) > 0).map(k => (m.get(k), k))
  pure def projection(s: State): Set[(int, int)] = entries(s.table)
  pure def binding(s: State): Binding = { phase: s.phase, metaVer: s.metaVer }
  pure def pageKeys(c: int): Set[int] = KEYS.filter(k => k >= c and k < c + PAGE)

  // One catalog transaction: write the phase and the metadata version key.
  pure def catalogChange(s: State, p: str): State =
    s.with("phase", p).with("version", s.version + 1).with("metaVer", s.version + 1)
  pure def addGsi(s: State): State = catalogChange(s, "Filling")
  pure def startBuild(s: State): State = catalogChange(s, "Building").with("cursor", 1)
  pure def canActivate(s: State): bool = s.phase == "Building" and s.cursor > LAST_KEY and not(s.pageBusy)
  pure def activate(s: State): State = catalogChange(s, "Active")

  pure def pageBegin(s: State): State = s.with("pageBusy", true).with("pageRv", s.version)
    .with("pageSeen", pageKeys(s.cursor).mapBy(k => s.table.get(k)))
  pure def pageConflict(s: State): bool =
    s.cfg.protectedScan and s.pageSeen.keys().exists(k => s.lastWrite.get(k) > s.pageRv)
  // One write attempt: put the entries of the read images and store the cursor in the job record.
  pure def pageCommit(s: State): State =
    if (pageConflict(s)) s.with("pageBusy", false).with("pageAborts", s.pageAborts + 1)
    else s.with("pageBusy", false).with("index", s.index.union(entries(s.pageSeen)))
      .with("cursor", s.cursor + PAGE).with("version", s.version + 1)

  // With the stamp check, a cached binding that differs from the metadata version read at the
  // attempt's read version is invalidated and the attempt is retried before any decision.
  pure def writerBegin(s: State, w: int, k: int, v: int): State = {
    val c = s.caches.get(w)
    if (s.cfg.stampCheck and c.metaVer != s.metaVer)
      s.with("caches", s.caches.set(w, binding(s))).with("stampRetries", s.stampRetries + 1)
    else s.with("attempts", s.attempts.set(w,
      { busy: true, key: k, newVal: v, rv: s.version, old: s.table.get(k), projecting: projecting(c.phase) }))
  }
  pure def writerConflict(s: State, a: Attempt): bool =
    s.lastWrite.get(a.key) > a.rv or (s.cfg.stampCheck and s.metaVer > a.rv)
  pure def writerCommit(s: State, w: int): State = {
    val a = s.attempts.get(w)
    if (writerConflict(s, a))
      s.with("attempts", s.attempts.set(w, idle)).with("writerAborts", s.writerAborts + 1)
    else {
      val cleared = if (a.projecting and a.old > 0) s.index.exclude(Set((a.old, a.key))) else s.index
      val updated = if (a.projecting and a.newVal > 0) cleared.union(Set((a.newVal, a.key))) else cleared
      s.with("attempts", s.attempts.set(w, idle))
        .with("table", s.table.set(a.key, a.newVal)).with("lastWrite", s.lastWrite.set(a.key, s.version + 1))
        .with("index", updated).with("version", s.version + 1).with("writes", s.writes + 1)
        .with("writesDuringBuild", s.writesDuringBuild + (if (s.phase == "Building") 1 else 0))
        .with("unprojectedCommits", s.unprojectedCommits + (if (projecting(s.phase) and not(a.projecting)) 1 else 0))
    }
  }

  // ------------------------------------------------------------------ invariant
  // At Active and at every later step the index equals the projection of the table.
  val indexMatchesTable = state.phase != "Active" or state.index == projection(state)
  val inv = indexMatchesTable

  // ------------------------------------------------------------------ witnesses
  val page_conflict_witness = state.phase == "Active" and state.pageAborts > 0
  val stamp_retry_witness = state.phase == "Active" and state.stampRetries > 0
  val writes_during_build_witness = state.phase == "Active" and state.writesDuringBuild >= 2
  val writer_abort_witness = state.phase == "Active" and state.writerAborts > 0

  // ------------------------------------------------------------------ actions
  action AddGsi = all { state.phase == "None", state' = addGsi(state) }
  action StartBuild = all { state.phase == "Filling", state' = startBuild(state) }
  action Activate = all { canActivate(state), state' = activate(state) }
  action PageBegin = all { state.phase == "Building", not(state.pageBusy), state.cursor <= LAST_KEY,
    state' = pageBegin(state) }
  action PageCommit = all { state.pageBusy, state' = pageCommit(state) }
  // The job stops after a page read; it restarts from the durable cursor.
  action PageDrop = all { state.pageBusy, state' = state.with("pageBusy", false) }
  action WriterBegin = {
    nondet w = WRITERS.oneOf()
    nondet k = KEYS.oneOf()
    nondet v = VALUES.oneOf()
    all { not(state.attempts.get(w).busy), state.writes < MAX_WRITES, state' = writerBegin(state, w, k, v) }
  }
  action WriterCommit = {
    nondet w = WRITERS.oneOf()
    all { state.attempts.get(w).busy, state' = writerCommit(state, w) }
  }
  // A cache refresh can happen at any time; a stale cache can also stay stale for any time.
  action RefreshCache = {
    nondet w = WRITERS.oneOf()
    state' = state.with("caches", state.caches.set(w, binding(state)))
  }
  action step = any { AddGsi, StartBuild, Activate, PageBegin, PageCommit, PageDrop,
    WriterBegin, WriterCommit, RefreshCache }

  // ------------------------------------------------------------------ tests
  pure val built = startBuild(addGsi(initialState(design)))
  pure def fresh(s: State): State = s.with("caches", WRITERS.mapBy(_ => binding(s)))
  pure def drain(s: State): State = 0.to(3).fold(s, (out, _) =>
    if (out.cursor <= LAST_KEY) pageCommit(pageBegin(out)) else out)

  run quietBuildCopiesTheTableTest = {
    val done = activate(drain(built))
    all { assert(done.phase == "Active"), assert(done.index == Set((1, 1), (2, 2))), assert(done.index == projection(done)) }
  }
  run writerAfterPageReadAbortsTheProtectedPageTest = {
    val read = pageBegin(fresh(built))
    val moved = writerCommit(writerBegin(read, 1, 1, 2), 1)
    val retried = pageCommit(moved)
    val done = activate(drain(retried))
    all { assert(retried.pageAborts == 1), assert(retried.cursor == 1),
      assert(done.index == Set((2, 1), (2, 2))), assert(done.index == projection(done)) }
  }
  run snapshotPagePutsTheOldImageTest = {
    val snap = startBuild(addGsi(initialState(design.with("protectedScan", false))))
    val read = pageBegin(fresh(snap))
    val moved = writerCommit(writerBegin(read, 1, 1, 2), 1)
    val done = activate(drain(pageCommit(moved)))
    all { assert(done.index.contains((1, 1))), assert(not(done.index == projection(done))) }
  }
  run pageCommitBeforeWriterIsReplacedByTheWriterTest = {
    val started = writerBegin(pageBegin(fresh(built)), 1, 1, NO_ATTR)
    val copied = pageCommit(started)
    val done = activate(drain(writerCommit(copied, 1)))
    all { assert(copied.index.contains((1, 1))), assert(done.index == Set((2, 2))), assert(done.index == projection(done)) }
  }
  run writerStartedBeforeTheGsiAbortsTest = {
    val started = writerBegin(initialState(design), 1, 3, 1)
    val aborted = writerCommit(addGsi(started), 1)
    all { assert(aborted.writerAborts == 1), assert(aborted.table.get(3) == ABSENT) }
  }
  run staleBindingWithoutStampMissesTheEntryTest = {
    val unchecked = startBuild(addGsi(initialState(design.with("stampCheck", false))))
    val passed = drain(unchecked)
    val done = activate(writerCommit(writerBegin(passed, 1, 3, 1), 1))
    all { assert(done.table.get(3) == 1), assert(not(done.index.contains((1, 3)))), assert(not(done.index == projection(done))) }
  }
}
