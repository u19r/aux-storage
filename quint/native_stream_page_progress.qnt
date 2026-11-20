// Limit-one pages spend one read even on an empty lineage. Native global clocks
// advance idle lineages too; their existing checkpoint is the scheduling state.
module native_stream_page_progress {
  pure val width = 65536
  pure def key(version, batch, event) = version * width * width + batch * width + event
  pure val end = key(0, 65535, 65535)
  pure def initial(policy, native) = {
    policy: policy, native: native, lanes: 0, format: "", phase: "new",
    clock: 0, record: 0, positions: 0.to(3).mapBy(_ => 0),
    highs: 0.to(3).mapBy(_ => 0), reads: 0, returned: 0,
    lastLane: -1, lastAfter: 0, lastThrough: 0, lastReturned: 0,
    previous: 0.to(3).mapBy(_ => 0),
  }
  var s: { policy: str, native: bool, lanes: int, format: str, phase: str,
    clock: int, record: int, positions: int -> int, highs: int -> int,
    reads: int, returned: int, lastLane: int, lastAfter: int, lastThrough: int,
    lastReturned: int, previous: int -> int }
  action init = s' = initial("least", true)
  action initCanonical = s' = initial("canonical", true)
  action initCounter = s' = initial("canonical", false)
  // Real setup actions first create lineages, then append a record to the last.
  action createLineages = {
    nondet count = 2.to(4).oneOf()
    nondet format = Set("numeric", "tuple").oneOf()
    all { s.phase == "new", s' = s.with("lanes", count).with("format", format).with("phase", "empty") }
  }
  action appendLater = {
    nondet ordinal = Set(0, 65535).oneOf()
    val record = if (s.format == "numeric") 2 else key(2, 65535, ordinal)
    val high = if (s.format == "numeric") 2 else key(2, 0, 0) + end
    all {
      s.phase == "empty",
      s' = s.with("phase", "reading").with("clock", 2).with("record", record)
        .with("highs", 0.to(3).mapBy(i => if (s.native) high else if (i == 0) 100 else if (i == s.lanes - 1) record else 0)),
    }
  }
  // Every read includes a preceding unrelated write. Counter bounds do not move.
  // Tuple scalar is exactly unsigned big-endian lexicographic (version,batch,event)
  // order, including both ordinal extremes; numeric providers use ordinary order.
  action readPage = {
    val clock = s.clock + 1
    val high = if (s.format == "numeric") clock else key(clock, 0, 0) + end
    val highs = if (s.native) 0.to(3).mapBy(_ => high) else s.highs
    val pending = 0.to(s.lanes - 1).filter(i => s.positions.get(i) < highs.get(i))
    val candidates = if (s.policy == "canonical") pending else pending.filter(i =>
      pending.forall(j => s.positions.get(i) <= s.positions.get(j)))
    nondet lane = candidates.oneOf()
    val after = s.positions.get(lane)
    val through = highs.get(lane)
    val hasRecord = lane == s.lanes - 1 and after < s.record and s.record <= through
    val position = if (hasRecord) s.record else through
    all {
      s.phase == "reading", pending.size() > 0,
      // Canonical tie order is unchanged. There are no ancestors in this model.
      candidates.forall(i => lane <= i),
      s' = s.with("clock", clock).with("highs", highs).with("previous", s.positions)
        .with("positions", s.positions.set(lane, position)).with("reads", s.reads + 1)
        .with("returned", s.returned + (if (hasRecord) 1 else 0))
        .with("lastLane", lane).with("lastAfter", after).with("lastThrough", through)
        .with("lastReturned", if (hasRecord) s.record else 0),
    }
  }
  action step = any { createLineages, appendLater, readPage }
  val boundedProgress = s.phase != "reading" or s.reads < s.lanes or s.returned == 1
  val exclusiveReads = s.returned <= 1 and (s.lastReturned == 0 or
    (s.lastAfter < s.lastReturned and s.lastReturned <= s.lastThrough))
  val laneProgress = 0.to(3).forall(i => s.positions.get(i) >= s.previous.get(i))
  val oneReadBudget = s.reads == 0 or 0.to(3).filter(i => s.positions.get(i) != s.previous.get(i)).size() == 1
  val perLaneBound = 0.to(s.lanes - 1).forall(i => s.positions.get(i) <= s.highs.get(i))
  val inv = boundedProgress and exclusiveReads and laneProgress and oneReadBudget and perLaneBound
  val idleFirst = s.reads == 1 and s.lastLane == 0 and s.returned == 0
  val laterPopulated = s.returned == 1 and s.lastLane == s.lanes - 1
  val tupleLow = s.format == "tuple" and s.record == key(2, 65535, 0) and s.returned == 1
  val tupleHigh = s.format == "tuple" and s.record == key(2, 65535, 65535) and s.returned == 1
  val numericProgress = s.format == "numeric" and s.returned == 1
  val fourLanesProgress = s.lanes == 4 and s.returned == 1 and s.reads == 4
  val counterUnchanged = not(s.native) and s.returned == 1 and s.reads == 2 and s.positions.get(0) == 100
  action fourReads = readPage.then(readPage).then(readPage).then(readPage)
  run leastAdvancedReachesLaterTest = init.then(createLineages).then(appendLater)
    .then(fourReads).expect(s.returned == 1 and inv)
  run canonicalStarvesLaterTest = initCanonical.then(createLineages).then(appendLater)
    .then(fourReads).expect(s.returned == 0 and not(boundedProgress) and exclusiveReads)
  run counterKeepsCanonicalOrderTest = initCounter.then(createLineages).then(appendLater)
    .then(readPage).expect(idleFirst and s.positions.get(0) == 100)
    .then(readPage).expect(counterUnchanged and inv)
}

