/// Abstract stream ordering only; native allocation is covered by provider position models.
module item_stream_order {
  type Config = {
    // "begin": the id is assigned before any item lock (R12 today).
    // "firstWrite": the id is assigned by the first write.
    idAt: str,
    // "allFirst": one statement takes every item lock (sorted) before any write.
    // "incremental": lock item, write item, then lock the next item.
    lockMode: str,
  }
  const CFG: Config

  type StreamRec = { item: int, offset: int, prev: int, image: int }
  type Writer = {
    phase: str, items: List[int], id: int, locked: Set[int], written: Set[int], pending: Set[StreamRec],
  }
  type State = {
    nextId: int, images: int -> int, locks: int -> int, writers: int -> Writer,
    committed: Set[StreamRec], delivered: List[StreamRec], cursor: int, consumerImages: int -> int,
    diverged: bool, lastAction: str,
  }

  pure val WRITERS = 1.to(3)
  pure val ITEMS = 1.to(2)
  pure val ITEM_SETS = Set(List(1), List(2), List(1, 2))
  pure val NO_FRONTIER = 1000
  pure val idleWriter: Writer = {
    phase: "idle", items: List(), id: 0, locked: Set(), written: Set(), pending: Set(),
  }
  pure val initial: State = {
    nextId: 0, images: ITEMS.mapBy(_ => 0), locks: ITEMS.mapBy(_ => 0),
    writers: WRITERS.mapBy(_ => idleWriter),
    committed: Set(), delivered: List(), cursor: 0, consumerImages: ITEMS.mapBy(_ => 0),
    diverged: false, lastAction: "init",
  }
  var s: State
  action init = s' = initial

  pure def setW(st: State, w: int, wr: Writer): State = st.with("writers", st.writers.set(w, wr))
  pure def indexOf(l: List[int], x: int): int =
    l.indices().fold(-1, (acc, i) => if (l[i] == x) i else acc)

  pure def canBegin(st: State, w: int): bool = st.writers.get(w).phase == "idle"
  pure def begin(st: State, w: int, items: List[int]): State = {
    val early = CFG.idAt == "begin"
    setW(st, w, { ...idleWriter, phase: "active", items: items,
      id: if (early) st.nextId + 1 else 0 })
      .with("nextId", if (early) st.nextId + 1 else st.nextId)
      .with("lastAction", "begin")
  }

  /// The items this writer may lock in this step.
  pure def lockable(st: State, w: int): Set[int] = {
    val wr = st.writers.get(w)
    val missing = wr.items.indices().filter(i => not(wr.locked.contains(wr.items[i])))
    if (wr.phase != "active" or missing.size() == 0) Set()
    else if (CFG.lockMode == "allFirst") {
      val wanted = wr.items.indices().map(i => wr.items[i])
      if (wanted.forall(i => st.locks.get(i) == 0 or st.locks.get(i) == w)) wanted else Set()
    } else {
      val nextKey = wr.items[missing.fold(99, (m, i) => if (i < m) i else m)]
      if (st.locks.get(nextKey) == 0 and wr.locked == wr.written) Set(nextKey) else Set()
    }
  }
  pure def canLock(st: State, w: int): bool = lockable(st, w).size() > 0
  pure def lock(st: State, w: int): State = {
    val wr = st.writers.get(w)
    val got = lockable(st, w)
    setW(st, w, { ...wr, locked: wr.locked.union(got) })
      .with("locks", ITEMS.mapBy(i => if (got.contains(i)) w else st.locks.get(i)))
      .with("lastAction", "lock")
  }

  pure def canWrite(st: State, w: int, key: int): bool = {
    val wr = st.writers.get(w)
    wr.phase == "active" and wr.locked.contains(key) and not(wr.written.contains(key))
  }
  /// Read the current image under the item lock, write a new image and stage
  /// a stream record whose offset derives from the id.
  pure def write(st: State, w: int, key: int): State = {
    val wr = st.writers.get(w)
    val id = if (wr.id == 0) st.nextId + 1 else wr.id
    val rec = { item: key, offset: id * 10 + indexOf(wr.items, key), prev: st.images.get(key), image: w * 10 + id }
    setW(st, w, { ...wr, id: id, written: wr.written.union(Set(key)),
      pending: wr.pending.union(Set(rec)) })
      .with("nextId", if (wr.id == 0) st.nextId + 1 else st.nextId)
      .with("images", st.images.set(key, rec.image))
      .with("lastAction", "write")
  }

  pure def canCommit(st: State, w: int): bool = {
    val wr = st.writers.get(w)
    wr.phase == "active" and wr.items.length() == wr.written.size()
  }
  pure def commit(st: State, w: int): State = {
    val wr = st.writers.get(w)
    setW(st, w, { ...wr, phase: "done", pending: Set() })
      .with("committed", st.committed.union(wr.pending))
      .with("locks", ITEMS.mapBy(i => if (st.locks.get(i) == w) 0 else st.locks.get(i)))
      .with("lastAction", "commit")
  }

  /// Reader frontier: only offsets below the oldest in-flight id are readable.
  pure def frontier(st: State): int = WRITERS.fold(NO_FRONTIER, (m, w) => {
    val wr = st.writers.get(w)
    if (wr.phase == "active" and wr.id > 0 and wr.id * 10 < m) wr.id * 10 else m
  })
  pure def nextReadable(st: State): Set[StreamRec] = {
    val after = st.committed.filter(r => r.offset > st.cursor and r.offset < frontier(st))
    after.filter(r => after.forall(o => o.offset >= r.offset))
  }
  pure def consume(st: State, r: StreamRec): State = st
    .with("delivered", st.delivered.append(r))
    .with("cursor", r.offset)
    .with("consumerImages", st.consumerImages.set(r.item, r.image))
    .with("diverged", st.diverged or r.prev != st.consumerImages.get(r.item))
    .with("lastAction", "consume")

  action Begin = {
    nondet w = WRITERS.oneOf()
    nondet items = ITEM_SETS.oneOf()
    all { canBegin(s, w), s' = begin(s, w, items) }
  }
  action Lock = { nondet w = WRITERS.oneOf() all { canLock(s, w), s' = lock(s, w) } }
  action Write = {
    nondet w = WRITERS.oneOf()
    nondet key = ITEMS.oneOf()
    all { canWrite(s, w, key), s' = write(s, w, key) }
  }
  action Commit = { nondet w = WRITERS.oneOf() all { canCommit(s, w), s' = commit(s, w) } }
  action Consume = all {
    nextReadable(s).size() > 0,
    nondet r = nextReadable(s).oneOf()
    s' = consume(s, r),
  }
  action step = any { Begin, Lock, Write, Commit, Consume }

  /// For each item, ordering its committed records by offset gives the order
  /// in which the writes were applied: each record's prev image is the image
  /// of the record immediately before it (or the initial image).
  pure def streamChainAt(st: State): bool = st.committed.forall(r => {
    val before = st.committed.filter(o => o.item == r.item and o.offset < r.offset)
    val pred = before.fold({ item: r.item, offset: -1, prev: 0, image: 0 },
      (m, o) => if (o.offset > m.offset) o else m)
    r.prev == pred.image
  })
  val streamChain = streamChainAt(s)
  /// The frontier guarantee: no record commits below a delivered offset.
  val noSkip = s.committed.forall(r => r.offset > s.cursor or s.delivered.indices().exists(i => s.delivered[i] == r))
  /// A consumer that applies delivered records in order never sees a record
  /// whose prev image differs from the image it holds.
  val consumerChain = not(s.diverged)
  val inv = all { streamChain, noSkip, consumerChain }

  val contention_witness = ITEMS.exists(i => s.committed.filter(r => r.item == i).size() >= 2)
  val multi_item_witness = WRITERS.exists(w => s.writers.get(w).phase == "done" and
    s.writers.get(w).items.length() == 2)
  val consumer_witness = s.delivered.length() >= 2

  /// The R12 example: X begins (id 1), Y begins (id 2), Y locks, writes and
  /// commits, then X locks, reads Y's image and commits with the lower offset.
  pure val r12Scenario: State = {
    val x = begin(initial, 1, List(1))
    val y = commit(write(lock(begin(x, 2, List(1)), 2), 2, 1), 2)
    commit(write(lock(y, 1), 1, 1), 1)
  }
  run r12Test = all {
    assert(CFG.idAt != "begin" or not(streamChainAt(r12Scenario))),
    assert(CFG.idAt != "firstWrite" or streamChainAt(r12Scenario)),
  }
  /// Two-item writer that locks incrementally: X writes item 1 (id 1), Y
  /// writes and commits item 2 (id 2), X then writes item 2 with offset 11.
  pure val incrementalScenario: State = {
    val x = write(lock(begin(initial, 1, List(1, 2)), 1), 1, 1)
    val y = commit(write(lock(begin(x, 2, List(2)), 2), 2, 2), 2)
    commit(write(lock(y, 1), 1, 2), 1)
  }
  run incrementalLockTest = all {
    assert(CFG.lockMode != "incremental" or not(streamChainAt(incrementalScenario))),
  }
}

module item_stream_order_early {
  import item_stream_order(CFG = { idAt: "begin", lockMode: "allFirst" }).*
}
module item_stream_order_late {
  import item_stream_order(CFG = { idAt: "firstWrite", lockMode: "allFirst" }).*
}
module item_stream_order_late_incremental {
  import item_stream_order(CFG = { idAt: "firstWrite", lockMode: "incremental" }).*
}

