/// Design-level protocol model of the W2/W12 SQS claim design
/// (docs/plans/incomplete/fable-w2-sqs-claim-design.md, D1-D11 and section 3).
/// It checks abstract transitions and the stated transaction assumptions, not
/// a provider, a driver, or crash durability.
///
/// Shape: two queues (1 = source with a redrive policy to 2, 2 = dead-letter
/// target), three message identities, three concurrent operation slots on two
/// API nodes, one shared store. Message operations read the catalog record
/// without a lock (snapshot) and then act on that incarnation. Catalog changes
/// are atomic, exclusive among themselves, and Durable. Two claim engines:
///   "lock"       (Postgres): select at a statement snapshot, then lock rows
///                with SKIP LOCKED and re-check the WHERE clause on the latest
///                committed version (EvalPlanQual), then commit.
///   "optimistic" (FoundationDB): snapshot window, aligned slot, conflict read
///                of the picked message keys and the dead-letter target key;
///                a changed key aborts; after BUDGET attempts return EMPTY (D4).
/// Crash: roll the store back to any committed prefix that still contains
/// every Durable commit (D11). Responses that clients already hold remain.
///
/// Time: one true clock `now`. Node 2 may run DELTA ahead (cfg.skewed); each
/// operation captures its node clock once, before its retries (fact 4).
module queue_claim {
  pure val IDS = 1.to(3)
  pure val ID_LIST = [1, 2, 3]
  pure val OPS = 1.to(3)
  pure val NODES = 1.to(2)
  pure val SOURCE = 1
  pure val TARGET = 2
  pure val QUEUES = Set(SOURCE, TARGET)
  pure val LIMIT = 1
  pure val WINDOW = 2
  pure val VIS = 2
  pure val SOURCE_RETENTION = 9
  pure val TARGET_RETENTION = 6
  pure val MAX_RECEIVE_COUNT = 1
  pure val BUDGET = 2
  pure val DELTA = 1
  pure val HORIZON = 14
  pure val MAX_INCARNATION = 6
  pure val NEVER = 1000
  pure val MALFORMED = -1
  /// Sampling bounds per trace, so that message races dominate the traces.
  pure val CATALOG_CHANGES = 2
  pure val CRASHES = 2

  type Row = { q: int, inc: int, id: int, due: int, receipt: int, count: int, created: int }
  /// Where an accepted message must be found, and the commit index at which
  /// the operation that put it there read the catalog.
  type Home = { q: int, inc: int, readAt: int }
  /// `home` and `gone` are ghost columns that roll back with the rows.
  type Store = { cur: int -> int, rows: Set[Row], home: int -> Home, gone: int -> str }
  type Op = {
    kind: str, phase: str, node: int, q: int, inc: int, readAt: int, captured: int, capturedTrue: int,
    attempt: int, snap: Set[Row], cand: List[int], picked: Set[int], receipt: int, arg: int,
    tInc: int, tReadAt: int, id: int,
  }
  /// One receive response entry. `until` is the true time at which the
  /// holder's visibility window ends (captured true time + timeout).
  type Response = { q: int, id: int, receipt: int, node: int, start: int, until: int, commitIdx: int }
  /// One visibility window that a holder was promised, in true time.
  type Lease = { id: int, receipt: int, from: int, until: int, idx: int }
  type Config = {
    engine: str, recheck: bool, cachedCatalog: bool, skewed: bool, leaseGuard: bool,
    snapshotResponse: bool, crashes: bool,
  }
  type Checkpoint = { store: Store, idx: int }
  type Lock = { inc: int, id: int, op: int }
  type State = {
    cfg: Config, now: int, cidx: int, store: Store, history: List[Checkpoint],
    retiredAt: int -> int, born: int -> int, nextInc: int, nextId: int, nextReceipt: int,
    acked: Set[int], sendCommitAt: int -> int, inflight: int -> Op, locks: Set[Lock],
    responses: Set[Response], leases: Set[Lease], rolledBack: Set[int], visLog: Set[{ receipt: int, idx: int }],
    cache: int -> (int -> int), outcomes: Set[str], violations: Set[str],
    catalogLeft: int, crashLeft: int,
  }
  type ClaimAcc = { store: Store, receipt: int, resp: Set[Response], tags: Set[str], violations: Set[str] }

  pure val idleOp: Op = {
    kind: "idle", phase: "none", node: 0, q: 0, inc: 0, readAt: 0, captured: 0, capturedTrue: 0,
    attempt: 0, snap: Set(), cand: [], picked: Set(), receipt: 0, arg: 0, tInc: 0, tReadAt: 0, id: 0,
  }
  pure val initialStore: Store = {
    cur: Map(SOURCE -> 1, TARGET -> 2), rows: Set(),
    home: IDS.mapBy(_ => { q: 0, inc: 0, readAt: 0 }), gone: IDS.mapBy(_ => "none"),
  }
  pure def initialState(cfg: Config): State = {
    cfg: cfg, now: 0, cidx: 0, store: initialStore, history: [{ store: initialStore, idx: 0 }],
    retiredAt: 1.to(MAX_INCARNATION).mapBy(_ => NEVER), born: Map(SOURCE -> 0, TARGET -> 0), nextInc: 3, nextId: 1, nextReceipt: 1,
    acked: Set(), sendCommitAt: IDS.mapBy(_ => 0), inflight: OPS.mapBy(_ => idleOp), locks: Set(),
    responses: Set(), leases: Set(), rolledBack: Set(), visLog: Set(),
    cache: NODES.mapBy(_ => initialStore.cur), outcomes: Set(), violations: Set(),
    catalogLeft: CATALOG_CHANGES, crashLeft: CRASHES,
  }
  pure val designLock: Config = {
    engine: "lock", recheck: true, cachedCatalog: false, skewed: false, leaseGuard: false,
    snapshotResponse: false, crashes: true,
  }
  pure val designOptimistic: Config = designLock.with("engine", "optimistic")

  var s: State

  // Design as written, no clock skew.
  action init = s' = initialState(designLock)
  action initLock = s' = initialState(designLock)
  action initOptimistic = s' = initialState(designOptimistic)
  // Bounded clock skew between nodes: the design as written (expected to FAIL I2).
  action initLockSkew = s' = initialState(designLock.with("skewed", true))
  action initOptimisticSkew = s' = initialState(designOptimistic.with("skewed", true))
  // Corrected rule: a row that carries a receipt is due only DELTA after its
  // visibility time (lease guard). Expected to hold with skew.
  action initLockSkewGuard = s' = initialState(designLock.with("skewed", true).with("leaseGuard", true))
  action initOptimisticSkewGuard = s' = initialState(designOptimistic.with("skewed", true).with("leaseGuard", true))
  // Mutation controls (expected to FAIL).
  action initLockNoRecheck = s' = initialState(designLock.with("recheck", false))
  action initCachedCatalog = s' = initialState(designOptimistic.with("cachedCatalog", true))
  action initSnapshotResponse = s' = initialState(designOptimistic.with("snapshotResponse", true))

  // ---------------------------------------------------------------- helpers
  pure def minInt(a: int, b: int): int = if (a < b) a else b
  pure def clock(st: State, n: int): int = st.now + (if (st.cfg.skewed and n == 2) DELTA else 0)
  pure def catalogView(st: State, n: int, q: int): int =
    if (st.cfg.cachedCatalog) st.cache.get(n).get(q) else st.store.cur.get(q)
  pure def rowAt(rows: Set[Row], inc: int, id: int): Set[Row] = rows.filter(r => r.inc == inc and r.id == id)
  pure def without(rows: Set[Row], inc: int, id: int): Set[Row] = rows.filter(r => not(r.inc == inc and r.id == id))
  pure def retention(q: int): int = if (q == SOURCE) SOURCE_RETENTION else TARGET_RETENTION
  pure def expired(r: Row, at: int): bool = r.created + retention(r.q) <= at
  pure def dueFor(cfg: Config, r: Row, captured: int): bool =
    r.due <= captured and (not(cfg.leaseGuard) or r.receipt == 0 or r.due + DELTA <= captured)
  pure def dueIds(st: State, inc: int, captured: int): List[int] =
    ID_LIST.select(i => st.store.rows.exists(r => r.inc == inc and r.id == i and dueFor(st.cfg, r, captured)))
  pure def lockedByOther(st: State, inc: int, id: int, op: int): bool =
    st.locks.exists(l => l.inc == inc and l.id == id and l.op != op)
  pure def isCurrent(st: State, inc: int): bool = inc != 0 and QUEUES.exists(q => st.store.cur.get(q) == inc)
  pure def isRetired(st: State, inc: int): bool = st.retiredAt.get(inc) < NEVER
  /// I5 at a write: the incarnation was retired at or before the operation's catalog read.
  pure def fence(st: State, inc: int, readAt: int): Set[str] =
    if (st.retiredAt.get(inc) <= readAt) Set("I5") else Set()
  pure def commit(st: State, store: Store, durable: bool): State = {
    val idx = st.cidx + 1
    val point = { store: store, idx: idx }
    st.with("cidx", idx).with("store", store)
      .with("history", if (durable) [point] else st.history.append(point))
  }
  pure def release(st: State, oid: int): State =
    st.with("inflight", st.inflight.set(oid, idleOp)).with("locks", st.locks.filter(l => l.op != oid))
  pure def tag(st: State, t: str): State = st.with("outcomes", st.outcomes.union(Set(t)))

  // ------------------------------------------------------------ claim apply
  pure def deliver(st: State, op: Op, idx: int, acc: ClaimAcc, r: Row): ClaimAcc = {
    val claimed = r.with("receipt", acc.receipt).with("count", r.count + 1).with("due", op.captured + VIS)
    val entry = { q: op.q, id: r.id, receipt: acc.receipt, node: op.node, start: st.now,
      until: op.capturedTrue + VIS, commitIdx: idx }
    acc.with("store", acc.store.with("rows", without(acc.store.rows, r.inc, r.id).union(Set(claimed))))
      .with("receipt", acc.receipt + 1)
      .with("resp", acc.resp.union(Set(entry)))
  }

  /// D10 at claim time, in the claim transaction.
  pure def claimOne(st: State, op: Op, idx: int, acc: ClaimAcc, r: Row): ClaimAcc = {
    val store = acc.store
    if (expired(r, op.captured)) {
      acc.with("store", store.with("rows", without(store.rows, r.inc, r.id)).with("gone", store.gone.set(r.id, "expired")))
        .with("tags", acc.tags.union(Set("expired_on_claim")))
    } else if (op.q == SOURCE and r.count >= MAX_RECEIVE_COUNT) {
      val targetHolds = op.tInc != 0 and store.rows.exists(x => x.inc == op.tInc and x.id == r.id)
      if (op.tInc == 0 or targetHolds) {
        deliver(st, op, idx, acc.with("tags", acc.tags.union(Set("redrive_skipped"))), r)
      } else if (r.created + TARGET_RETENTION <= op.captured) {
        acc.with("store", store.with("rows", without(store.rows, r.inc, r.id)).with("gone", store.gone.set(r.id, "expired")))
          .with("tags", acc.tags.union(Set("redrive_expired")))
      } else {
        // AWS keeps the receive count on a move (W12, design section 6).
        val moved = { q: TARGET, inc: op.tInc, id: r.id, due: op.captured, receipt: 0, count: r.count, created: r.created }
        val raceTags = (if (isRetired(st, op.tInc) and st.retiredAt.get(op.tInc) > op.tReadAt) Set("move_into_retired_target") else Set())
          .union(if (isRetired(st, op.inc)) Set("purged_message_moved") else Set())
        acc.with("store", store.with("rows", without(store.rows, r.inc, r.id).union(Set(moved)))
            .with("home", store.home.set(r.id, { q: TARGET, inc: op.tInc, readAt: op.tReadAt })))
          .with("tags", acc.tags.union(Set("moved")).union(raceTags))
          .with("violations", acc.violations.union(fence(st, op.tInc, op.tReadAt)))
      }
    } else {
      deliver(st, op, idx, acc, r)
    }
  }

  /// One Relaxed claim commit over the rows the engine validated.
  pure def applyClaim(st: State, oid: int, op: Op, rows: Set[Row]): State = {
    val idx = st.cidx + 1
    val ordered = ID_LIST.select(i => rows.exists(r => r.id == i))
    val acc0: ClaimAcc = { store: st.store, receipt: st.nextReceipt, resp: Set(), tags: Set(), violations: Set() }
    val acc = ordered.foldl(acc0, (a, i) => claimOne(st, op, idx, a, rows.filter(r => r.id == i).getOnlyElement()))
    val written = acc.store.rows.filter(r => r.receipt >= st.nextReceipt).map(r => (r.id, r.receipt))
    val answered = acc.resp.map(x => (x.id, x.receipt))
    val i3 = if (written == answered) Set() else Set("I3")
    val committed = release(commit(st, acc.store, false), oid)
    committed.with("nextReceipt", acc.receipt)
      .with("responses", st.responses.union(acc.resp))
      .with("leases", st.leases.union(acc.resp.map(x => { id: x.id, receipt: x.receipt, from: x.start, until: x.until, idx: x.commitIdx })))
      .with("outcomes", st.outcomes.union(acc.tags))
      .with("violations", st.violations.union(acc.violations).union(fence(st, op.inc, op.readAt)).union(i3))
  }

  // ------------------------------------------------- lock-style claim (L)
  pure def canClaimSelect(st: State, oid: int, n: int, q: int): bool = all {
    st.cfg.engine == "lock", st.inflight.get(oid).kind == "idle", catalogView(st, n, q) != 0,
  }
  pure def claimSelect(st: State, oid: int, n: int, q: int): State = {
    val inc = catalogView(st, n, q)
    val captured = clock(st, n)
    val op = idleOp.with("kind", "claim").with("phase", "selected").with("node", n).with("q", q)
      .with("inc", inc).with("readAt", st.cidx).with("captured", captured).with("capturedTrue", st.now)
      .with("cand", dueIds(st, inc, captured)).with("snap", st.store.rows.filter(r => r.inc == inc))
    st.with("inflight", st.inflight.set(oid, op))
  }
  pure def canClaimLock(st: State, oid: int): bool =
    st.inflight.get(oid).kind == "claim" and st.inflight.get(oid).phase == "selected"
  /// FOR UPDATE SKIP LOCKED with the READ COMMITTED re-check; the target
  /// catalog statement runs after the lock, before commit.
  pure def claimLock(st: State, oid: int): State = {
    val op = st.inflight.get(oid)
    val skipped = op.cand.select(i => lockedByOther(st, op.inc, i, oid)).length() > 0
    val taken = op.cand.foldl(Set(), (acc, i) =>
      if (acc.size() >= LIMIT or lockedByOther(st, op.inc, i, oid)) acc
      else {
        val current = rowAt(st.store.rows, op.inc, i)
        val ok = if (st.cfg.recheck) current.exists(r => dueFor(st.cfg, r, op.captured)) else current.size() > 0
        if (ok) acc.union(Set(i)) else acc
      })
    val lockedOp = op.with("phase", "locked").with("picked", taken)
      .with("tInc", catalogView(st, op.node, TARGET)).with("tReadAt", st.cidx)
    val locked = st.with("locks", st.locks.union(taken.map(i => { inc: op.inc, id: i, op: oid })))
      .with("inflight", st.inflight.set(oid, lockedOp))
    if (skipped and taken.size() > 0) tag(locked, "skip_locked") else locked
  }
  pure def canLockClaimCommit(st: State, oid: int): bool =
    st.cfg.engine == "lock" and st.inflight.get(oid).kind == "claim" and st.inflight.get(oid).phase == "locked"
  pure def lockClaimCommit(st: State, oid: int): State = {
    val op = st.inflight.get(oid)
    val rows = if (st.cfg.recheck) st.store.rows.filter(r => r.inc == op.inc and op.picked.contains(r.id))
      else op.snap.filter(r => op.picked.contains(r.id))
    if (op.picked.size() == 0) release(st, oid) else applyClaim(st, oid, op, rows)
  }

  // ------------------------------------------- optimistic claim (O, D3/D4)
  pure def canOptClaimBegin(st: State, oid: int, n: int, q: int): bool = {
    val prior = st.inflight.get(oid)
    val fresh = prior.kind == "idle"
    val node = if (fresh) n else prior.node
    val queue = if (fresh) q else prior.q
    all {
      st.cfg.engine == "optimistic",
      fresh or (prior.kind == "claim" and prior.phase == "retry"),
      catalogView(st, node, queue) != 0,
    }
  }
  pure def optClaimBegin(st: State, oid: int, n: int, q: int, rotation: int): State = {
    val prior = st.inflight.get(oid)
    val fresh = prior.kind == "idle"
    val node = if (fresh) n else prior.node
    val queue = if (fresh) q else prior.q
    val captured = if (fresh) clock(st, node) else prior.captured
    val capturedTrue = if (fresh) st.now else prior.capturedTrue
    val inc = catalogView(st, node, queue)
    val window = dueIds(st, inc, captured).slice(0, minInt(WINDOW, dueIds(st, inc, captured).length()))
    val slots = (window.length() + LIMIT - 1) / LIMIT
    // W2-F6: a first attempt takes slot 0 only below two full slots (and an
    // unfilled window); here WINDOW = 2 * LIMIT, so both bounds agree.
    val slot = if (slots == 0) 0 else if (fresh and window.length() < 2 * LIMIT and window.length() < WINDOW) 0 else rotation % slots
    val pickedList = window.slice(minInt(slot * LIMIT, window.length()), minInt(slot * LIMIT + LIMIT, window.length()))
    val picked = pickedList.foldl(Set(), (acc, i) => acc.union(Set(i)))
    val tInc = catalogView(st, node, TARGET)
    val snap = st.store.rows.filter(r => picked.contains(r.id) and (r.inc == inc or (r.inc == tInc and tInc != 0)))
    val op = idleOp.with("kind", "claim").with("phase", "read").with("node", node).with("q", queue)
      .with("inc", inc).with("readAt", st.cidx).with("captured", captured).with("capturedTrue", capturedTrue)
      .with("attempt", if (fresh) 1 else prior.attempt).with("picked", picked).with("snap", snap)
      .with("tInc", tInc).with("tReadAt", st.cidx)
    st.with("inflight", st.inflight.set(oid, op))
  }
  pure def canOptClaimCommit(st: State, oid: int): bool =
    st.cfg.engine == "optimistic" and st.inflight.get(oid).kind == "claim" and st.inflight.get(oid).phase == "read"
  pure def optClaimCommit(st: State, oid: int): State = {
    val op = st.inflight.get(oid)
    val changed = op.picked.exists(i =>
      rowAt(st.store.rows, op.inc, i) != rowAt(op.snap, op.inc, i)
        or (op.tInc != 0 and op.q == SOURCE and rowAt(st.store.rows, op.tInc, i) != rowAt(op.snap, op.tInc, i)))
    if (changed and op.attempt < BUDGET) {
      st.with("inflight", st.inflight.set(oid, op.with("phase", "retry").with("attempt", op.attempt + 1)))
    } else if (changed) {
      // D4: a conflict proves no commit; the receive is empty.
      val dueLeft = st.store.rows.exists(r => isCurrent(st, r.inc) and r.q == op.q and dueFor(st.cfg, r, op.captured))
      val base = tag(release(st, oid), if (dueLeft) "contention_empty_with_due" else "contention_empty")
      if (st.cfg.snapshotResponse) {
        // Mutation: answer from the aborted snapshot.
        val bogus = op.snap.filter(r => r.inc == op.inc).map(r => { q: op.q, id: r.id, receipt: st.nextReceipt,
          node: op.node, start: st.now, until: op.capturedTrue + VIS, commitIdx: st.cidx })
        base.with("responses", st.responses.union(bogus)).with("nextReceipt", st.nextReceipt + 1)
          .with("violations", st.violations.union(if (bogus.size() > 0) Set("I3") else Set()))
      } else base
    } else if (op.picked.size() == 0) {
      release(st, oid)
    } else {
      applyClaim(st, oid, op, st.store.rows.filter(r => r.inc == op.inc and op.picked.contains(r.id)))
    }
  }

  // ------------------------------------------------------- send (Durable)
  pure def canSendBegin(st: State, oid: int, n: int, q: int): bool = all {
    st.inflight.get(oid).kind == "idle", st.nextId <= 3, catalogView(st, n, q) != 0,
  }
  pure def sendBegin(st: State, oid: int, n: int, q: int, delay: int): State = {
    val op = idleOp.with("kind", "send").with("phase", "read").with("node", n).with("q", q)
      .with("inc", catalogView(st, n, q)).with("readAt", st.cidx).with("captured", clock(st, n))
      .with("id", st.nextId).with("arg", delay)
    st.with("inflight", st.inflight.set(oid, op)).with("nextId", st.nextId + 1)
  }
  pure def canSendCommit(st: State, oid: int): bool = st.inflight.get(oid).kind == "send"
  pure def sendCommit(st: State, oid: int): State = {
    val op = st.inflight.get(oid)
    val row = { q: op.q, inc: op.inc, id: op.id, due: op.captured + op.arg, receipt: 0, count: 0, created: op.captured }
    val store = st.store.with("rows", st.store.rows.union(Set(row)))
      .with("home", st.store.home.set(op.id, { q: op.q, inc: op.inc, readAt: op.readAt }))
    val c = release(commit(st, store, true), oid)
    val overlap = isRetired(st, op.inc) and st.retiredAt.get(op.inc) > op.readAt
    c.with("acked", st.acked.union(Set(op.id)))
      .with("sendCommitAt", st.sendCommitAt.set(op.id, c.cidx))
      .with("violations", st.violations.union(fence(st, op.inc, op.readAt)))
      .with("outcomes", st.outcomes.union(if (overlap) Set("overlap_send_retired") else Set()))
  }

  // ------------------------------- delete / change visibility by receipt (D7)
  pure def receiptSnapshot(st: State, inc: int, receipt: int): Set[Row] =
    st.store.rows.filter(r => r.inc == inc and r.receipt == receipt)
  pure def canReceiptBegin(st: State, oid: int): bool = st.inflight.get(oid).kind == "idle"
  pure def receiptBegin(st: State, oid: int, n: int, kind: str, q: int, receipt: int, v: int): State =
    if (receipt == MALFORMED) tag(st, "malformed_rejected")
    else {
      val inc = catalogView(st, n, q)
      val op = idleOp.with("kind", kind).with("phase", "read").with("node", n).with("q", q).with("inc", inc)
        .with("readAt", st.cidx).with("captured", clock(st, n)).with("capturedTrue", st.now)
        .with("receipt", receipt).with("arg", v).with("attempt", 1).with("snap", receiptSnapshot(st, inc, receipt))
      st.with("inflight", st.inflight.set(oid, op))
    }
  pure def canReceiptCommit(st: State, oid: int): bool = {
    val op = st.inflight.get(oid)
    all {
      op.kind == "delete" or op.kind == "visibility",
      // Postgres: FOR UPDATE waits for a claim that holds the row lock.
      st.cfg.engine != "lock" or not(op.snap.exists(r => lockedByOther(st, r.inc, r.id, oid))),
    }
  }
  pure def receiptCommit(st: State, oid: int): State = {
    val op = st.inflight.get(oid)
    val conflict = st.cfg.engine == "optimistic"
      and op.snap.exists(r => rowAt(st.store.rows, r.inc, r.id) != Set(r))
    if (conflict and op.attempt < BUDGET) {
      val inc = catalogView(st, op.node, op.q)
      st.with("inflight", st.inflight.set(oid, op.with("inc", inc).with("readAt", st.cidx)
        .with("attempt", op.attempt + 1).with("snap", receiptSnapshot(st, inc, op.receipt))))
    } else if (conflict) {
      tag(release(st, oid), "write_retry_exhausted_503")
    } else {
      // Lock style re-checks the receipt on the locked latest version.
      val current = op.snap.fold(Set(), (acc, r) => acc.union(rowAt(st.store.rows, r.inc, r.id)))
        .filter(r => r.receipt == op.receipt)
      if (current.size() == 0) {
        val stale = st.responses.filter(x => x.receipt == op.receipt)
        val redelivered = stale.exists(a => st.responses.exists(b => b.id == a.id and b.receipt != a.receipt and b.commitIdx > a.commitIdx))
        val t = if (op.kind == "delete") (if (redelivered) "stale_delete_after_redelivery" else "stale_delete")
          else "stale_visibility_error"
        tag(tag(release(st, oid), t), if (op.kind == "delete") "stale_delete" else t)
      } else {
        val r = current.getOnlyElement()
        val mine = st.responses.filter(x => x.receipt == op.receipt)
        val revived = mine.exists(a => st.responses.exists(b => b.id == a.id and b.commitIdx > a.commitIdx))
        val applied = if (revived) tag(st, "revived_receipt_used") else st
        if (op.kind == "delete") {
          val store = st.store.with("rows", without(st.store.rows, r.inc, r.id)).with("gone", st.store.gone.set(r.id, "deleted"))
          val c = release(commit(applied, store, false), oid)
          tag(c.with("violations", st.violations.union(fence(st, r.inc, op.readAt))), "deleted")
        } else {
          val store = st.store.with("rows", without(st.store.rows, r.inc, r.id).union(Set(r.with("due", op.captured + op.arg))))
          val c = release(commit(applied, store, false), oid)
          // The holder's window is now [now, captured + timeout); earlier windows of this receipt end now.
          val leases = st.leases.map(x => if (x.receipt == op.receipt and x.until > st.now) x.with("until", st.now) else x)
            .union(Set({ id: r.id, receipt: op.receipt, from: st.now, until: op.capturedTrue + op.arg, idx: c.cidx }))
          tag(c.with("leases", leases).with("visLog", st.visLog.union(Set({ receipt: op.receipt, idx: c.cidx })))
            .with("violations", st.violations.union(fence(st, r.inc, op.readAt))),
            if (op.arg == 0) "visibility_zero" else "visibility_extended")
        }
      }
    }
  }

  // ------------------------------------------- cleanup and catalog (Durable)
  pure def cleanupVictims(st: State): Set[Row] = st.store.rows.filter(r =>
    st.locks.forall(l => not(l.inc == r.inc and l.id == r.id)) and (not(isCurrent(st, r.inc)) or expired(r, clock(st, 1))))
  pure def cleanup(st: State): State = {
    val victims = cleanupVictims(st)
    val gone = victims.filter(r => isCurrent(st, r.inc)).fold(st.store.gone, (g, r) => g.set(r.id, "expired"))
    tag(commit(st, st.store.with("rows", st.store.rows.exclude(victims)).with("gone", gone), true), "cleanup")
  }
  pure def retire(st: State, q: int, nextIncarnation: int, label: str): State = {
    val old = st.store.cur.get(q)
    val c = commit(st, st.store.with("cur", st.store.cur.set(q, nextIncarnation)), true)
    tag(c.with("retiredAt", if (old != 0) st.retiredAt.set(old, c.cidx) else st.retiredAt)
      .with("catalogLeft", st.catalogLeft - 1)
      .with("nextInc", if (nextIncarnation != 0) st.nextInc + 1 else st.nextInc), label)
  }
  pure def canPurge(st: State, q: int): bool = st.catalogLeft > 0 and st.store.cur.get(q) != 0 and st.nextInc <= MAX_INCARNATION
  pure def purge(st: State, q: int): State = retire(st, q, st.nextInc, "purge")
  pure def canDeleteQueue(st: State, q: int): bool = st.catalogLeft > 0 and st.store.cur.get(q) != 0
  pure def deleteQueue(st: State, q: int): State = retire(st, q, 0, "delete_queue")
  pure def canCreateQueue(st: State, q: int): bool = st.store.cur.get(q) == 0 and st.nextInc <= MAX_INCARNATION
  /// A re-created queue has a new identity: `born` is the commit that created it, and a
  /// receipt handle issued before that commit names a queue that no longer exists.
  pure def createQueue(st: State, q: int): State = {
    val c = retire(st, q, st.nextInc, "create_queue")
    c.with("born", c.born.set(q, c.cidx))
  }

  /// D11: restore a committed prefix that contains every Durable commit.
  pure def crash(st: State, k: int): State = {
    val point = st.history.nth(k)
    val undone = st.responses.filter(r => r.commitIdx > point.idx).map(r => r.receipt)
      .union(st.visLog.filter(v => v.idx > point.idx).map(v => v.receipt))
    val base = st.with("store", point.store).with("history", st.history.slice(0, k + 1))
      .with("inflight", OPS.mapBy(_ => idleOp)).with("locks", Set())
      .with("rolledBack", st.rolledBack.union(undone)).with("crashLeft", st.crashLeft - 1)
    if (undone.size() > 0) tag(base, "crash_undid_relaxed") else base
  }

  // ---------------------------------------------------------------- actions
  action ClaimSelect = {
    nondet oid = OPS.oneOf()
    nondet node = NODES.oneOf()
    nondet queue = QUEUES.oneOf()
    all { canClaimSelect(s, oid, node, queue), s' = claimSelect(s, oid, node, queue) }
  }
  action ClaimLock = {
    nondet oid = OPS.oneOf()
    all { canClaimLock(s, oid), s' = claimLock(s, oid) }
  }
  action LockClaimCommit = {
    nondet oid = OPS.oneOf()
    all { canLockClaimCommit(s, oid), s' = lockClaimCommit(s, oid) }
  }
  action OptClaimBegin = {
    nondet oid = OPS.oneOf()
    nondet node = NODES.oneOf()
    nondet queue = QUEUES.oneOf()
    nondet rotation = 0.to(1).oneOf()
    all { canOptClaimBegin(s, oid, node, queue), s' = optClaimBegin(s, oid, node, queue, rotation) }
  }
  action OptClaimCommit = {
    nondet oid = OPS.oneOf()
    all { canOptClaimCommit(s, oid), s' = optClaimCommit(s, oid) }
  }
  action SendBegin = {
    nondet oid = OPS.oneOf()
    nondet node = NODES.oneOf()
    nondet queue = QUEUES.oneOf()
    nondet delay = 0.to(1).oneOf()
    all { canSendBegin(s, oid, node, queue), s' = sendBegin(s, oid, node, queue, delay) }
  }
  action SendCommit = {
    nondet oid = OPS.oneOf()
    all { canSendCommit(s, oid), s' = sendCommit(s, oid) }
  }
  action ReceiptBegin = {
    nondet oid = OPS.oneOf()
    nondet node = NODES.oneOf()
    nondet kind = Set("delete", "visibility").oneOf()
    nondet held = s.responses.map(r => (r.q, r.receipt)).union(Set((SOURCE, MALFORMED))).oneOf()
    nondet timeout = Set(0, VIS).oneOf()
    all { canReceiptBegin(s, oid), s' = receiptBegin(s, oid, node, kind, held._1, held._2, timeout) }
  }
  action ReceiptCommit = {
    nondet oid = OPS.oneOf()
    all { canReceiptCommit(s, oid), s' = receiptCommit(s, oid) }
  }
  action Cleanup = all { cleanupVictims(s).size() > 0, s' = cleanup(s) }
  action Purge = {
    nondet queue = QUEUES.oneOf()
    all { canPurge(s, queue), s' = purge(s, queue) }
  }
  action DeleteQueue = {
    nondet queue = QUEUES.oneOf()
    all { canDeleteQueue(s, queue), s' = deleteQueue(s, queue) }
  }
  action CreateQueue = {
    nondet queue = QUEUES.oneOf()
    all { canCreateQueue(s, queue), s' = createQueue(s, queue) }
  }
  action Crash = {
    nondet k = 0.to(s.history.length() - 1).oneOf()
    all { s.cfg.crashes, s.crashLeft > 0, s.history.length() > 1, s' = crash(s, k) }
  }
  /// Only meaningful for the cached-catalog mutation.
  action RefreshCache = {
    nondet node = NODES.oneOf()
    all { s.cfg.cachedCatalog, s' = s.with("cache", s.cache.set(node, s.store.cur)) }
  }
  action Tick = all { s.now < HORIZON, s' = s.with("now", s.now + 1) }

  /// Message operations are listed twice to weight the random scheduler
  /// towards interleavings of message transactions.
  action step = any {
    ClaimSelect, ClaimLock, LockClaimCommit, OptClaimBegin, OptClaimCommit,
    ClaimSelect, ClaimLock, LockClaimCommit, OptClaimBegin, OptClaimCommit,
    SendBegin, SendCommit, ReceiptBegin, ReceiptCommit, SendBegin, SendCommit,
    Cleanup, Purge, DeleteQueue, CreateQueue, Crash, RefreshCache, Tick, Tick,
  }

  // ------------------------------------------------------------- invariants
  /// I1 for one accepted message. Allowed absences: deleted with its current
  /// receipt, expired, or its home incarnation was retired by a catalog change
  /// that committed AFTER the catalog read of the operation that put it there
  /// (a later purge/delete, or the D1 overlap). A retirement at or before that
  /// read is a loss by a non-overlapping operation.
  pure def accounted(st: State, m: int): bool = {
    val h = st.store.home.get(m)
    val presentHome = st.store.cur.get(h.q) == h.inc and st.store.rows.exists(r => r.inc == h.inc and r.id == m)
    val retiredAfterRead = st.store.cur.get(h.q) != h.inc and st.retiredAt.get(h.inc) > h.readAt
    presentHome or st.store.gone.get(m) != "none" or retiredAfterRead
  }
  val i1NoLoss = s.acked.forall(m => accounted(s, m))
  /// I2: two deliveries of one message overlap in true time only when the
  /// earlier lease was undone by a crash (a Relaxed commit was lost).
  val i2NoDoubleDelivery = s.leases.forall(a => s.leases.forall(b =>
    (a.id == b.id and a.receipt != b.receipt and a.idx < b.idx and b.from < a.until)
      implies s.rolledBack.contains(a.receipt)))
  /// I3: checked at each claim commit: response entries equal the receipts
  /// that the committed claim wrote; an aborted claim answers nothing.
  val i3ClaimAtomic = not(s.violations.contains("I3"))
  /// I4: at most one live copy of a message id across current incarnations.
  val i4SingleCopy = IDS.forall(m => s.store.rows.filter(r => r.id == m and isCurrent(s, r.inc)).size() <= 1)
  /// I5: checked at each write: no write lands in an incarnation retired at
  /// or before the writer's catalog read.
  val i5Fence = not(s.violations.contains("I5"))
  val inv = all { i1NoLoss, i2NoDoubleDelivery, i3ClaimAtomic, i4SingleCopy, i5Fence }

  // -------------------------------------------------------------- witnesses
  val contended_empty_witness = s.outcomes.contains("contention_empty_with_due")
  val stale_delete_after_redelivery_witness = s.outcomes.contains("stale_delete_after_redelivery")
  val overlap_send_witness = s.outcomes.contains("overlap_send_retired")
  val dead_letter_move_witness = s.outcomes.contains("moved")
  val move_races_target_purge_witness = s.outcomes.contains("move_into_retired_target")
  val purged_message_moved_witness = s.outcomes.contains("purged_message_moved")
  val skip_locked_witness = s.outcomes.contains("skip_locked")
  val redrive_skipped_witness = s.outcomes.contains("redrive_skipped")
  val crash_undo_claim_witness = s.leases.exists(a => s.rolledBack.contains(a.receipt)
    and s.leases.exists(b => b.id == a.id and b.receipt != a.receipt and b.idx > a.idx and b.from < a.until))
  /// A crash that undid a newer claim made an older receipt current again,
  /// and its holder then used it successfully.
  val revived_receipt_witness = s.outcomes.contains("revived_receipt_used")

  // ------------------------------------------------------- deterministic runs
  pure val L0 = initialState(designLock)
  pure val O0 = initialState(designOptimistic)
  pure def ok(st: State): bool = all {
    st.acked.forall(m => accounted(st, m)),
    IDS.forall(m => st.store.rows.filter(r => r.id == m and isCurrent(st, r.inc)).size() <= 1),
    st.violations == Set(),
  }
  pure def tickN(st: State, k: int): State = 1.to(k).fold(st, (acc, _) => acc.with("now", acc.now + 1))

  run sendOverlappingPurgeIsTheAcceptedAnomalyTest = {
    val begun = sendBegin(L0, 1, 1, SOURCE, 0)
    val purged = purge(begun, SOURCE)
    val sent = sendCommit(purged, 1)
    all { assert(sent.outcomes.contains("overlap_send_retired")), assert(ok(sent)),
      assert(sent.store.rows.forall(r => not(isCurrent(sent, r.inc)))) }
  }
  run sendAfterPurgeUsesNewIncarnationTest = {
    val sent = sendCommit(sendBegin(purge(L0, SOURCE), 1, 1, SOURCE, 0), 1)
    all { assert(sent.store.rows.forall(r => isCurrent(sent, r.inc))), assert(ok(sent)) }
  }
  run cachedCatalogLosesNonOverlappingSendTest = {
    val c0 = initialState(designOptimistic.with("cachedCatalog", true))
    val sent = sendCommit(sendBegin(purge(c0, SOURCE), 1, 1, SOURCE, 0), 1)
    all { assert(sent.violations.contains("I5")), assert(not(sent.acked.forall(m => accounted(sent, m)))) }
  }
  run skipLockedGivesDisjointClaimsTest = {
    val two = sendCommit(sendBegin(sendCommit(sendBegin(L0, 1, 1, SOURCE, 0), 1), 1, 1, SOURCE, 0), 1)
    val a = claimLock(claimSelect(two, 1, 1, SOURCE), 1)
    val b = claimLock(claimSelect(a, 2, 2, SOURCE), 2)
    val done = lockClaimCommit(lockClaimCommit(b, 1), 2)
    all { assert(done.responses.map(r => r.id) == Set(1, 2)), assert(done.outcomes.contains("skip_locked")), assert(ok(done)) }
  }
  run recheckRejectsRowClaimedAfterSnapshotTest = {
    val one = sendCommit(sendBegin(L0, 1, 1, SOURCE, 0), 1)
    val a = claimSelect(one, 1, 1, SOURCE)
    val b = lockClaimCommit(claimLock(claimSelect(a, 2, 2, SOURCE), 2), 2)
    val done = lockClaimCommit(claimLock(b, 1), 1)
    all { assert(done.responses.size() == 1), assert(ok(done)) }
  }
  run optimisticConflictAbortsAndReturnsEmptyTest = {
    val one = sendCommit(sendBegin(O0, 1, 1, SOURCE, 0), 1)
    val a = optClaimBegin(one, 1, 1, SOURCE, 0)
    val b = optClaimCommit(optClaimBegin(a, 2, 2, SOURCE, 0), 2)
    val retry = optClaimCommit(b, 1)
    val sent2 = sendCommit(sendBegin(retry, 3, 1, SOURCE, 0), 3)
    // The retry reads again; force a second conflict on the new message.
    val r2 = optClaimBegin(sent2, 1, 1, SOURCE, 0)
    val stolen = optClaimCommit(optClaimBegin(r2, 2, 2, SOURCE, 0), 2)
    val empty = optClaimCommit(stolen, 1)
    all { assert(empty.outcomes.contains("contention_empty")), assert(empty.inflight.get(1).kind == "idle"), assert(ok(empty)) }
  }
  run staleReceiptDeleteSucceedsWithoutDeletingTest = {
    val one = sendCommit(sendBegin(L0, 1, 1, TARGET, 0), 1)
    val first = lockClaimCommit(claimLock(claimSelect(one, 1, 1, TARGET), 1), 1)
    val second = lockClaimCommit(claimLock(claimSelect(tickN(first, VIS), 1, 1, TARGET), 1), 1)
    val deleted = receiptCommit(receiptBegin(second, 2, 1, "delete", TARGET, 1, 0), 2)
    all { assert(deleted.outcomes.contains("stale_delete_after_redelivery")),
      assert(deleted.store.rows.exists(r => r.id == 1)), assert(ok(deleted)) }
  }
  run deadLetterMoveIsAtomicTest = {
    val one = sendCommit(sendBegin(L0, 1, 1, SOURCE, 0), 1)
    val first = lockClaimCommit(claimLock(claimSelect(one, 1, 1, SOURCE), 1), 1)
    val moved = lockClaimCommit(claimLock(claimSelect(tickN(first, VIS), 1, 1, SOURCE), 1), 1)
    all { assert(moved.outcomes.contains("moved")), assert(moved.store.rows.map(r => r.q) == Set(TARGET)),
      assert(moved.store.home.get(1).q == TARGET), assert(ok(moved)) }
  }
  run moveRacingTargetPurgeLandsInRetiredTargetTest = {
    val one = sendCommit(sendBegin(L0, 1, 1, SOURCE, 0), 1)
    val first = lockClaimCommit(claimLock(claimSelect(one, 1, 1, SOURCE), 1), 1)
    val locked = claimLock(claimSelect(tickN(first, VIS), 1, 1, SOURCE), 1)
    val moved = lockClaimCommit(purge(locked, TARGET), 1)
    all { assert(moved.outcomes.contains("move_into_retired_target")), assert(ok(moved)),
      assert(moved.store.rows.forall(r => not(isCurrent(moved, r.inc)))) }
  }
  run crashUndoesClaimAndOnlyRedeliversTest = {
    val one = sendCommit(sendBegin(L0, 1, 1, TARGET, 0), 1)
    val first = lockClaimCommit(claimLock(claimSelect(one, 1, 1, TARGET), 1), 1)
    val crashed = crash(first, 0)
    val again = lockClaimCommit(claimLock(claimSelect(crashed, 2, 2, TARGET), 2), 2)
    all { assert(crashed.store.rows == one.store.rows), assert(again.responses.size() == 2),
      assert(again.rolledBack == Set(1)), assert(ok(again)) }
  }
  run skewedNodeReclaimsInsideLeaseTest = {
    val k0 = initialState(designLock.with("skewed", true))
    val one = sendCommit(sendBegin(k0, 1, 1, TARGET, 0), 1)
    val first = lockClaimCommit(claimLock(claimSelect(one, 1, 1, TARGET), 1), 1)
    val early = lockClaimCommit(claimLock(claimSelect(tickN(first, VIS - DELTA), 2, 2, TARGET), 2), 2)
    val guarded0 = initialState(designLock.with("skewed", true).with("leaseGuard", true))
    val g1 = sendCommit(sendBegin(guarded0, 1, 1, TARGET, 0), 1)
    val gFirst = lockClaimCommit(claimLock(claimSelect(g1, 1, 1, TARGET), 1), 1)
    val gEarly = lockClaimCommit(claimLock(claimSelect(tickN(gFirst, VIS - DELTA), 2, 2, TARGET), 2), 2)
    all { assert(early.responses.size() == 2), assert(gEarly.responses.size() == 1) }
  }
}
