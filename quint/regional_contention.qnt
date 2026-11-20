/// Design-level protocol model: lock order, transaction-id assignment and hot
/// records of regional writes on Postgres (W17 model 2).
/// It checks abstract transitions and assumptions, not production code.
/// `quint run` is bounded random simulation, not a proof.
///
/// Source: docs/plans/incomplete/fable-w17-regional-replication-plan.md,
/// sections 1.5, 1.6, 4.3 (RR1, RR2, RR5, RR6), 5.2 (model 2), 6.3 (design B),
/// 7.1 (W6 A8). Approach for the transaction id: quint/item_revision_order.qnt
/// module `item_stream_order` (R12).
///
/// ============================================================================
/// What the model abstracts
/// ============================================================================
/// - One local region and one emitting peer. Up to 3 concurrent Postgres transactions (slots), at most MAX_TXNS starts.
///   Each runs a fixed program of lock steps and write steps in code order,
///   then commits and releases every lock. One step per action.
/// - Locks: row locks (`FOR SHARE` = S, `FOR UPDATE` or an UPDATE = X) and
///   transaction-scoped advisory locks. A row lock or a write assigns the
///   transaction id (xid) if none is assigned; an advisory lock does not.
///   Snapshot reads take no lock and are omitted. `FOR UPDATE` of an absent
///   row (acceptance inventory of a new event) locks nothing and is omitted.
/// - Postgres deadlock detection: a cycle in the waits-for graph; the action
///   `DeadlockAbort` aborts one member (after `deadlock_timeout`).
/// - Stream order: the stream record of a write has offset = xid (R12). For
///   each item, the commit order (the item order: the item lock is held to
///   commit) must equal the xid order.
/// - RR1 is expressed as `serialized`: at most one regional transaction is in
///   its write phase (every lock acquired, not committed). It HOLDS for the
///   current code (the defect) and must be VIOLATED for design B (parallelism).
///
/// Resource ids: cat(t) = 10+t (catalog stamp row), state(t) = 20+t (table
/// state record), tables(t) = 30+t (`tables` row, TTL config), item(t,k) =
/// 100+10t+k, layout = 200, part(k) = 210+k (journal partition of key k),
/// account = 220 (history byte account), catalog advisory lock = 300,
/// cursor(src,t) = 400+10src+t (design B peer cursor row).
///
/// ============================================================================
/// Programs (code order) and Rust mapping
/// ============================================================================
/// Current origin, `origin/executor.rs` `ItemOperation::execute` (:167-240):
///   cat(t) S       `RequestTableBindings::resolve_table` (:173-176)   [xid]
///   state(t) S     `JournalStore::load_capture_state` (journal/catalog.rs:142-148)
///   tables(t) X    Put only: `read_ttl_config` (:181-185)             (P11)
///   item X         `bound.prepare` (:188)
///   layout X, part X  `capture_origin_mutations` -> journal/origin/allocation.rs
///                  `allocate_partitions` -> journal/partition.rs `observe_partitions`
///                  (`require_layout`, Conflict)                        (RR1)
///   account X      `NativeRecords::store` -> history_capacity.rs
///                  `prepare_history_charge` (:288-342)                 (RR1)
///   write          `apply_accepted` (item, provenance, stream record)
/// Current import, `import/executor.rs` `execute_import`, import/store.rs:
///   state(t) S     `load_configuration` -> `observe_table_states` (:79) [xid]
///   cat(t) S       `resolve_table_in_attempt`
///   layout X, part X, account X  `prepare_events` -> journal/import_originals.rs
///                  `prepare_import_events` -> `prepare_first_seen_batch` ->
///                  journal/partition.rs `allocate_acceptance_positions` -> `store`
///   per event: item X (`load_payload_observation` -> `observe_for_write`),
///                  tables(t) X (`store_winner_effects` -> `read_ttl_config`, :264,
///                  RR5), write (item effects, `flush_dynamo_write`).
/// "a8" origin (W6 A8 on the origin only, journal unchanged): catalog advisory
///   S, item X, layout X, part X, account X, write.
/// "aItemFirst" import (plan 6.2 / step 0.6 with the hot rows kept): catalog
///   advisory S, all page items X in key order, layout X, parts X, account X,
///   writes.
/// Design B (plan 6.3, 6.6 phase 1B; none of these exist yet):
///   origin = ordinary write + stamp (1B.2): catalog advisory S, item X,
///     write (item, provenance, stream record); entry `RegionalDynamo::put_item`.
///   apply (1B.3): catalog advisory S, every page item X in key order, the
///     item writes, cursor(src, t) X (peer cursor row written in the same
///     attempt); entry: the apply/resolve owner called by the 1B.4 delivery job.
/// quint-connect: not a trace replay. Pin the statement order with the W6 A0
/// native event counters and the R12 guard (`pg_current_xact_id_if_assigned()`
/// is null before the item lock); the deadlock and overlap scenarios become
/// Postgres tests with a hold hook (plan 6.6 step 0.3).
module regional_contention {
  type Config = {
    origin: str,     // "current" | "a8" | "b"
    apply: str,      // "current" | "aItemFirst" | "b"
    itemLock: str,   // "row" (FOR UPDATE: assigns the xid) | "advisory" (no xid)
    sortKeys: bool,  // a page locks its items in key order (false: page order)
  }
  const CFG: Config

  type LockStep = { kind: str, res: int, mode: str, xid: bool }
  type Txn = { phase: str, kind: str, prog: List[LockStep], pc: int, xid: int, writes: List[int] }
  type Hold = { res: int, txn: int, mode: str }
  type Target = { t: int, k: int }
  type State = {
    txns: int -> Txn, held: Set[Hold], nextXid: int, started: int,
    itemLog: int -> List[int], aborts: int, maxParallel: int, lastAction: str,
  }

  pure val SLOTS = 1.to(3)
  pure val MAX_TXNS = 6
  pure val TABLES = 1.to(2)
  pure val KEYS = 1.to(2)
  pure val ITEMS = TABLES.map(t => KEYS.map(k => 100 + 10 * t + k)).flatten()
  pure val PAGES: Set[List[Target]] = Set(
    List({ t: 1, k: 1 }), List({ t: 1, k: 2 }), List({ t: 2, k: 1 }),
    List({ t: 1, k: 1 }, { t: 1, k: 2 }), List({ t: 1, k: 2 }, { t: 1, k: 1 }))
  pure val idleTxn: Txn = { phase: "idle", kind: "", prog: List(), pc: 0, xid: 0, writes: List() }

  pure def itemRes(x: Target): int = 100 + 10 * x.t + x.k
  pure def lk(res: int, mode: str, xid: bool): LockStep = { kind: "lock", res: res, mode: mode, xid: xid }
  pure def row(res: int, mode: str): LockStep = lk(res, mode, true)
  pure def adv(res: int, mode: str): LockStep = lk(res, mode, false)
  pure def wr(res: int): LockStep = { kind: "write", res: res, mode: "", xid: true }
  pure def itemStep(x: Target): LockStep = lk(itemRes(x), "X", CFG.itemLock == "row")
  pure val hot: List[LockStep] = List(row(200, "X"))
  pure def concatAll(ls: List[List[LockStep]]): List[LockStep] = ls.foldl(List(), (a, l) => a.concat(l))
  pure def distinctTables(page: List[Target]): List[int] =
    range(1, 3).foldl(List(), (a, t) => if (page.indices().exists(i => page[i].t == t)) a.append(t) else a)
  pure def distinctKeys(page: List[Target]): List[int] =
    range(1, 3).foldl(List(), (a, k) => if (page.indices().exists(i => page[i].k == k)) a.append(k) else a)
  /// Key order of a page (canonical) or page order.
  pure def lockOrder(page: List[Target]): List[Target] =
    if (not(CFG.sortKeys)) page
    else range(1, 3).foldl(List(), (a, k) => page.foldl(a, (b, x) => if (x.k == k) b.append(x) else b))
  pure def hotRows(ks: List[int]): List[LockStep] =
    List(row(200, "X")).concat(ks.foldl(List(), (a, k) => a.append(row(210 + k, "X")))).append(row(220, "X"))

  pure def originProg(isPut: bool, x: Target): List[LockStep] =
    if (CFG.origin == "current") concatAll(List(
      List(row(10 + x.t, "S"), row(20 + x.t, "S")),
      if (isPut) List(row(30 + x.t, "X")) else List(),
      List(itemStep(x)), hotRows(List(x.k)), List(wr(itemRes(x)))))
    else if (CFG.origin == "a8") concatAll(List(
      List(adv(300, "S"), itemStep(x)), hotRows(List(x.k)), List(wr(itemRes(x)))))
    else List(adv(300, "S"), itemStep(x), wr(itemRes(x)))

  pure def applyProg(src: int, page: List[Target]): List[LockStep] = {
    val tabs = distinctTables(page)
    if (CFG.apply == "current") concatAll(List(
      tabs.foldl(List(), (a, t) => a.append(row(20 + t, "S"))),
      tabs.foldl(List(), (a, t) => a.append(row(10 + t, "S"))),
      hotRows(distinctKeys(page)),
      page.foldl(List(), (a, x) => a.concat(List(itemStep(x), row(30 + x.t, "X"), wr(itemRes(x)))))))
    else if (CFG.apply == "aItemFirst") concatAll(List(
      List(adv(300, "S")),
      lockOrder(page).foldl(List(), (a, x) => a.append(itemStep(x))),
      hotRows(distinctKeys(page)),
      page.foldl(List(), (a, x) => a.append(wr(itemRes(x))))))
    else concatAll(List(
      List(adv(300, "S")),
      lockOrder(page).foldl(List(), (a, x) => a.append(itemStep(x))),
      page.foldl(List(), (a, x) => a.append(wr(itemRes(x)))),
      tabs.foldl(List(), (a, t) => a.append(row(400 + 10 * src + t, "X")))))
  }

  pure val initial: State = {
    txns: SLOTS.mapBy(_ => idleTxn), held: Set(), nextXid: 0, started: 0,
    itemLog: ITEMS.mapBy(_ => List()), aborts: 0, maxParallel: 0, lastAction: "init",
  }
  var s: State
  action init = s' = initial

  // ---------------------------------------------------------------- locks
  pure def holders(st: State, res: int): Set[Hold] = st.held.filter(h => h.res == res)
  pure def holds(st: State, w: int, res: int, mode: str): bool =
    st.held.exists(h => h.res == res and h.txn == w and (h.mode == "X" or mode == "S"))
  pure def conflicting(st: State, w: int, res: int, mode: str): Set[int] =
    holders(st, res).filter(h => h.txn != w and (mode == "X" or h.mode == "X")).map(h => h.txn)
  pure def current(st: State, w: int): LockStep = st.txns.get(w).prog[st.txns.get(w).pc]
  pure def running(st: State, w: int): bool =
    st.txns.get(w).phase == "run" and st.txns.get(w).pc < st.txns.get(w).prog.length()
  pure def waitsFor(st: State, w: int): Set[int] =
    if (not(running(st, w))) Set()
    else {
      val stp = current(st, w)
      if (stp.kind != "lock" or holds(st, w, stp.res, stp.mode)) Set()
      else conflicting(st, w, stp.res, stp.mode)
    }
  pure def edge(st: State, a: int, b: int): bool = waitsFor(st, a).contains(b)
  pure def inCycle(st: State, a: int): bool = SLOTS.exists(b => b != a and (
    (edge(st, a, b) and edge(st, b, a)) or
    SLOTS.exists(c => c != a and c != b and edge(st, a, b) and edge(st, b, c) and edge(st, c, a))))

  pure def lastLock(p: List[LockStep]): int = p.indices().fold(-1, (m, i) => if (p[i].kind == "lock" and i > m) i else m)
  pure def inWritePhase(st: State, w: int): bool =
    st.txns.get(w).phase == "run" and st.txns.get(w).pc > lastLock(st.txns.get(w).prog)
  pure def parallel(st: State): int = SLOTS.filter(w => inWritePhase(st, w)).size()

  // ---------------------------------------------------------------- transitions
  pure def start(st: State, w: int, kind: str, prog: List[LockStep]): State =
    st.with("txns", st.txns.set(w, { ...idleTxn, phase: "run", kind: kind, prog: prog }))
      .with("started", st.started + 1).with("lastAction", "start")
  pure def canStep(st: State, w: int): bool =
    running(st, w) and {
      val stp = current(st, w)
      stp.kind == "write" or holds(st, w, stp.res, stp.mode) or conflicting(st, w, stp.res, stp.mode).size() == 0
    }
  pure def doStep(st: State, w: int): State = {
    val t = st.txns.get(w)
    val stp = current(st, w)
    val assignXid = stp.xid and t.xid == 0
    val xid = if (assignXid) st.nextXid + 1 else t.xid
    val held = if (stp.kind == "lock" and not(holds(st, w, stp.res, stp.mode)))
      st.held.union(Set({ res: stp.res, txn: w, mode: stp.mode })) else st.held
    val t2 = { ...t, pc: t.pc + 1, xid: xid, writes: if (stp.kind == "write") t.writes.append(stp.res) else t.writes }
    val st2 = st.with("txns", st.txns.set(w, t2)).with("held", held)
      .with("nextXid", if (assignXid) st.nextXid + 1 else st.nextXid)
      .with("lastAction", stp.kind)
    st2.with("maxParallel", if (parallel(st2) > st.maxParallel) parallel(st2) else st.maxParallel)
  }
  pure def canCommit(st: State, w: int): bool =
    st.txns.get(w).phase == "run" and st.txns.get(w).pc == st.txns.get(w).prog.length()
  pure def commit(st: State, w: int): State = {
    val t = st.txns.get(w)
    st.with("itemLog", ITEMS.mapBy(i =>
        if (t.writes.indices().exists(j => t.writes[j] == i)) st.itemLog.get(i).append(t.xid) else st.itemLog.get(i)))
      .with("held", st.held.filter(h => h.txn != w))
      .with("txns", st.txns.set(w, idleTxn))
      .with("lastAction", "commit")
  }
  pure def abort(st: State, w: int): State =
    st.with("held", st.held.filter(h => h.txn != w))
      .with("txns", st.txns.set(w, idleTxn))
      .with("aborts", st.aborts + 1)
      .with("lastAction", "deadlock_abort")

  action StartOrigin = {
    nondet w = SLOTS.oneOf()
    nondet t = TABLES.oneOf()
    nondet k = KEYS.oneOf()
    nondet isPut = Set(true, false).oneOf()
    all { st_idle(s, w), s' = start(s, w, if (isPut) "origin_put" else "origin_delete", originProg(isPut, { t: t, k: k })) }
  }
  action StartApply = {
    nondet w = SLOTS.oneOf()
    nondet src = Set(1).oneOf()
    nondet page = PAGES.oneOf()
    all { st_idle(s, w), s' = start(s, w, if (src == 1) "apply1" else "apply2", applyProg(src, page)) }
  }
  pure def st_idle(st: State, w: int): bool = st.txns.get(w).phase == "idle" and st.started < MAX_TXNS
  action RunStep = { nondet w = SLOTS.oneOf() all { canStep(s, w), s' = doStep(s, w) } }
  action Commit = { nondet w = SLOTS.oneOf() all { canCommit(s, w), s' = commit(s, w) } }
  action DeadlockAbort = { nondet w = SLOTS.oneOf() all { inCycle(s, w), s' = abort(s, w) } }
  action step = any { StartOrigin, StartApply, RunStep, RunStep, RunStep, Commit, DeadlockAbort }

  // ---------------------------------------------------------------- invariants
  /// RR2: no waits-for cycle.
  val noDeadlock = SLOTS.forall(w => not(inCycle(s, w)))
  /// I7 / RR6 / R12: per item, commit order equals xid (stream offset) order.
  val streamOrder = ITEMS.forall(i => {
    val l = s.itemLog.get(i)
    l.indices().forall(j => j == 0 or l[j - 1] < l[j])
  })
  /// RR1 statement: at most one regional transaction is in its write phase.
  val serialized = parallel(s) <= 1
  val inv = all { noDeadlock, streamOrder }

  val parallelWitness = parallel(s) >= 2
  val contentionWitness = ITEMS.exists(i => s.itemLog.get(i).length() >= 2)
  val overlapApplyWitness = SLOTS.exists(a => SLOTS.exists(b => a != b and
    s.txns.get(a).kind == "apply1" and s.txns.get(b).kind == "apply2" and
    s.txns.get(a).prog.indices().exists(i => s.txns.get(b).prog.indices().exists(j =>
      s.txns.get(a).prog[i].kind == "write" and s.txns.get(a).prog[i] == s.txns.get(b).prog[j]))))
  val originApplyWitness = SLOTS.exists(a => SLOTS.exists(b => a != b and
    s.txns.get(a).kind == "origin_put" and s.txns.get(b).kind == "apply1"))

  // ---------------------------------------------------------------- scenarios
  /// RR2 + RR5 on different keys of one table: origin Put of (1,1) holds
  /// tables(1) and item(1,1) and waits for layout; an import of (1,2) holds
  /// layout, part, account and item(1,2) and waits for tables(1).
  pure def stepN(st: State, w: int, n: int): State = range(0, n).foldl(st, (a, _) => doStep(a, w))
  pure val rr2Scenario: State = {
    val o = stepN(start(initial, 1, "origin_put", originProg(true, { t: 1, k: 1 })), 1, 4)
    val i = start(o, 2, "apply1", applyProg(1, List({ t: 1, k: 2 })))
    val n = i.txns.get(2).prog.indices().filter(j => i.txns.get(2).prog[j].res == 30 + 1).fold(99, (m, j) => if (j < m) j else m)
    stepN(i, 2, n)
  }
  run rr2Test = all {
    assert(CFG.origin != "current" or CFG.apply != "current" or inCycle(rr2Scenario, 1)),
  }
  /// R12: X takes its xid at the stamp, stalls; Y writes (1,1) and commits;
  /// X then writes (1,1) with the lower xid.
  pure val r12Scenario: State = {
    val x = stepN(start(initial, 1, "origin_delete", originProg(false, { t: 1, k: 1 })), 1, 1)
    val yp = originProg(false, { t: 1, k: 1 })
    val y = commit(stepN(start(x, 2, "origin_delete", yp), 2, yp.length()), 2)
    val xp = x.txns.get(1).prog
    commit(stepN(y, 1, xp.length() - 1), 1)
  }
  run r12Test = all {
    assert(CFG.origin != "current" or r12Scenario.itemLog.get(111) == List(2, 1)),
  }
}

// ============================================================================
// Variants
// ============================================================================
/// Current code (RR1, RR2, RR5, RR6).
module contention_current {
  import regional_contention(CFG = { origin: "current", apply: "current", itemLock: "row", sortKeys: false }).*
}
/// W6 A8 on the origin only; the import still takes hot rows before item locks.
module contention_a8_origin_only {
  import regional_contention(CFG = { origin: "a8", apply: "current", itemLock: "advisory", sortKeys: false }).*
}
/// Design A order fix (items first everywhere) with the region-wide hot rows kept.
module contention_items_first_hot {
  import regional_contention(CFG = { origin: "a8", apply: "aItemFirst", itemLock: "advisory", sortKeys: true }).*
}
/// Design B: ordinary write + stamp; apply locks page keys in key order.
module contention_b {
  import regional_contention(CFG = { origin: "b", apply: "b", itemLock: "advisory", sortKeys: true }).*
}
/// Design B, page keys locked in page order.
module contention_b_unsorted {
  import regional_contention(CFG = { origin: "b", apply: "b", itemLock: "advisory", sortKeys: false }).*
}
/// Design B, item locks are FOR UPDATE row locks (assignXid the xid at the first lock).
module contention_b_row_item_lock {
  import regional_contention(CFG = { origin: "b", apply: "b", itemLock: "row", sortKeys: true }).*
}
