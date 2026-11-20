/// Model-based-test boundary of quint/queue_claim.qnt for the Rust driver
/// crates/queue/src/quint_queue_claim_tests.rs (spec OWN-C-a, OWN-C-b).
///
/// Every customer operation is one atomic step on one shared store, so no two
/// native attempts overlap. Interleavings inside attempts, crashes, the cached
/// catalog, the snapshot-response mutation and cleanup stay in the design model.
///
/// `mbtInitHead`: SQL stores take the head of (due, id).
/// `mbtInitSlot`: ordered-key stores take position `rotation % eligible`
/// (`ClaimSlot::positions` with LIMIT 1 and an unfilled window).
/// A zero visibility timeout sets the row back by DELTA (`visibility_end`).
module queue_claim_mbt {
  import queue_claim.* from "queue_claim"

  type Answer = { kind: str, ids: Set[int] }

  var mbtState: State
  var mbtAnswer: Answer

  pure def answerOf(kind: str): Answer = { kind: kind, ids: Set() }

  pure def mbtConfig(engine: str): Config =
    designLock.with("engine", engine).with("skewed", true).with("leaseGuard", true).with("crashes", false)

  /// The visibility time of message `id` in incarnation `inc` (NEVER when absent).
  pure def dueOf(ms: State, inc: int, id: int): int =
    ms.store.rows.filter(r => r.inc == inc and r.id == id).fold(NEVER, (acc, r) => r.due)

  pure def dueBefore(ms: State, inc: int, a: int, b: int): bool = {
    val da = dueOf(ms, inc, a)
    val db = dueOf(ms, inc, b)
    da < db or (da == db and a < b)
  }

  /// The due ids of `inc` in (due, id) order: the SQL index order and the KV due-key order.
  pure def dueOrder(ms: State, inc: int, captured: int): List[int] =
    dueIds(ms, inc, captured).foldl([], (acc, i) =>
      acc.select(j => dueBefore(ms, inc, j, i)).append(i).concat(acc.select(j => dueBefore(ms, inc, i, j))))

  /// Ids of the response entries that `after` added to `before`.
  pure def newResponses(before: State, after: State): Set[int] =
    after.responses.filter(r => r.receipt >= before.nextReceipt).map(r => r.id)

  /// One receive of at most LIMIT = 1 message by node `n` on queue `q`, in one attempt.
  pure def claimAtomic(ms: State, n: int, q: int, rotation: int): State = {
    val inc = ms.store.cur.get(q)
    val captured = clock(ms, n)
    val order = dueOrder(ms, inc, captured)
    val slot = if (ms.cfg.engine == "lock" or order.length() < 2) 0 else rotation % order.length()
    val picked = if (order.length() == 0) Set() else Set(order.nth(slot))
    val op = idleOp.with("kind", "claim").with("phase", "locked").with("node", n).with("q", q)
      .with("inc", inc).with("readAt", ms.cidx).with("captured", captured).with("capturedTrue", ms.now)
      .with("attempt", 1).with("picked", picked)
      .with("tInc", ms.store.cur.get(TARGET)).with("tReadAt", ms.cidx)
    if (picked.size() == 0) ms
    else applyClaim(ms, 1, op, ms.store.rows.filter(r => r.inc == inc and picked.contains(r.id)))
  }

  /// Delete or change visibility by receipt, in one attempt; a zero timeout
  /// sets the row back by the skew margin, as `visibility_end` does.
  pure def receiptAtomic(ms: State, n: int, kind: str, q: int, receipt: int, timeout: int): State = {
    val begun = receiptBegin(ms, 1, n, kind, q, receipt, timeout)
    val op = begun.inflight.get(1)
    val done = receiptCommit(begun, 1)
    if (kind == "visibility" and timeout == 0)
      done.with("store", done.store.with("rows", done.store.rows.map(r =>
        if (r.inc == op.inc and r.receipt == receipt and r.due == op.captured) r.with("due", op.captured - DELTA) else r)))
    else done
  }

  action mbtInitHead = all { mbtState' = initialState(mbtConfig("lock")), mbtAnswer' = answerOf("none") }
  action mbtInitSlot = all { mbtState' = initialState(mbtConfig("optimistic")), mbtAnswer' = answerOf("none") }

  action MbtSend = {
    nondet node = NODES.oneOf()
    nondet queue = QUEUES.oneOf()
    nondet delay = 0.to(1).oneOf()
    val missing = mbtState.store.cur.get(queue) == 0
    all {
      mbtState.nextId <= 3,
      mbtState' = if (missing) mbtState else sendCommit(sendBegin(mbtState, 1, node, queue, delay), 1),
      mbtAnswer' = if (missing) answerOf("missing") else { kind: "sent", ids: Set(mbtState.nextId) },
    }
  }

  action MbtClaim = {
    nondet node = NODES.oneOf()
    nondet queue = QUEUES.oneOf()
    nondet rotation = 0.to(1).oneOf()
    val missing = mbtState.store.cur.get(queue) == 0
    val after = if (missing) mbtState else claimAtomic(mbtState, node, queue, rotation)
    all {
      mbtState' = after,
      mbtAnswer' = if (missing) answerOf("missing") else { kind: "received", ids: newResponses(mbtState, after) },
    }
  }

  action MbtReceipt = {
    nondet node = NODES.oneOf()
    nondet kind = Set("delete", "visibility").oneOf()
    nondet held = mbtState.responses.map(r => (r.q, r.receipt)).union(Set((SOURCE, 0))).oneOf()
    nondet timeout = Set(0, VIS).oneOf()
    val queue = held._1
    val missing = mbtState.store.cur.get(queue) == 0
    // A handle of an earlier creation of the queue finds no queue (the receipt names its queue id).
    val earlier = mbtState.responses.exists(r => r.receipt == held._2 and r.commitIdx < mbtState.born.get(queue))
    val holder = mbtState.store.rows.exists(r => r.inc == mbtState.store.cur.get(queue) and r.receipt == held._2)
    all {
      held._2 > 0,
      mbtState' = if (missing or earlier) mbtState else receiptAtomic(mbtState, node, kind, queue, held._2, timeout),
      mbtAnswer' = answerOf(if (missing or earlier) "missing" else if (kind == "visibility" and not(holder)) "stale_error" else "ok"),
    }
  }

  action MbtPurge = {
    nondet queue = QUEUES.oneOf()
    val missing = mbtState.store.cur.get(queue) == 0
    all {
      mbtState.catalogLeft > 0,
      mbtState.nextInc <= MAX_INCARNATION,
      mbtState' = if (missing) mbtState else purge(mbtState, queue),
      mbtAnswer' = answerOf(if (missing) "missing" else "ok"),
    }
  }

  action MbtDeleteQueue = {
    nondet queue = QUEUES.oneOf()
    val missing = mbtState.store.cur.get(queue) == 0
    all {
      mbtState.catalogLeft > 0,
      mbtState' = if (missing) mbtState else deleteQueue(mbtState, queue),
      mbtAnswer' = answerOf(if (missing) "missing" else "ok"),
    }
  }

  action MbtCreateQueue = {
    nondet queue = QUEUES.oneOf()
    val noTarget = queue == SOURCE and mbtState.store.cur.get(TARGET) == 0
    all {
      mbtState.catalogLeft > 0,
      mbtState.store.cur.get(queue) == 0,
      mbtState.nextInc <= MAX_INCARNATION,
      mbtState' = if (noTarget) mbtState else createQueue(mbtState, queue),
      mbtAnswer' = answerOf(if (noTarget) "invalid" else "ok"),
    }
  }

  action MbtTick = {
    nondet k = Set(1, 3).oneOf()
    all {
      mbtState.now + k <= HORIZON,
      mbtState' = mbtState.with("now", mbtState.now + k),
      mbtAnswer' = answerOf("none"),
    }
  }

  /// Claims and receipts are listed more than once to weight the scheduler.
  action mbtStep = any {
    MbtSend, MbtSend, MbtClaim, MbtClaim, MbtClaim, MbtReceipt, MbtReceipt,
    MbtPurge, MbtDeleteQueue, MbtCreateQueue, MbtTick, MbtTick, MbtTick,
  }

  /// I3, I4 and I5 of the design model, restated over the boundary state.
  val mbtInv = all {
    not(mbtState.violations.contains("I3")),
    not(mbtState.violations.contains("I5")),
    IDS.forall(m => mbtState.store.rows.filter(r => r.id == m and isCurrent(mbtState, r.inc)).size() <= 1),
  }

  run mbtHeadFollowsDueTimeNotIdTest = {
    val h0 = initialState(mbtConfig("lock"))
    val late = sendCommit(sendBegin(h0, 1, 1, TARGET, 1), 1)
    val early = sendCommit(sendBegin(late, 1, 1, TARGET, 0), 1)
    val got = claimAtomic(early.with("now", 1), 1, TARGET, 0)
    assert(newResponses(early, got) == Set(2))
  }

  run mbtSlotTakesRotationAmongTwoDueTest = {
    val k0 = initialState(mbtConfig("optimistic"))
    val two = sendCommit(sendBegin(sendCommit(sendBegin(k0, 1, 1, TARGET, 0), 1), 1, 1, TARGET, 0), 1)
    val slotPick = claimAtomic(two, 1, TARGET, 1)
    val headPick = claimAtomic(two.with("cfg", two.cfg.with("engine", "lock")), 1, TARGET, 1)
    all { assert(newResponses(two, slotPick) == Set(2)), assert(newResponses(two, headPick) == Set(1)) }
  }

  run mbtZeroVisibilityRedeliversAtOnceTest = {
    val h0 = initialState(mbtConfig("lock"))
    val sent = sendCommit(sendBegin(h0, 1, 1, TARGET, 0), 1)
    val first = claimAtomic(sent, 1, TARGET, 0)
    val zeroed = receiptAtomic(first, 1, "visibility", TARGET, 1, 0)
    val again = claimAtomic(zeroed, 1, TARGET, 0)
    all { assert(newResponses(zeroed, again) == Set(1)), assert(again.store.rows.map(r => r.count) == Set(2)) }
  }
}
