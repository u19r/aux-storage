/// Two nodes, one key and at most three writes. Local commit observes the
/// protected current winner; each action is one atomic native operation.
/// This isolates ordering from delivery-prefix/GC and bootstrap protocols.
module regional_item_order_model {
  const BROKEN: str
  type Event = { clock: int, region: int, live: bool }
  type State = { events: List[Event], rows: int -> int, known: int -> Set[int],
    pending: int -> int, regressed: bool }
  var s: State
  pure val nodes = Set(0, 1)
  pure def clock(events: List[Event], id: int): int = if (id == -1) 0 else events[id].clock
  pure def greater(a: Event, b: Event): bool = a.clock > b.clock or
    (a.clock == b.clock and a.region > b.region)
  pure def choose(events: List[Event], row: int, id: int): int =
    if (row == -1 or (if (BROKEN == "noTieBreak") events[id].clock > events[row].clock
      else greater(events[id], events[row]))) id else row
  action init = s' = { events: List(), rows: Map(0 -> -1, 1 -> -1),
    known: Map(0 -> Set(), 1 -> Set()), pending: Map(0 -> -2, 1 -> -2), regressed: false }
  action begin(node: int): bool = all { s.pending.get(node) == -2,
    s' = s.with("pending", s.pending.set(node, s.rows.get(node))) }
  action commit(node: int, live: bool): bool = {
    val current = s.rows.get(node)
    val observed = if (BROKEN == "stalePredecessor") s.pending.get(node) else current
    val event = { clock: clock(s.events, observed) + 1, region: node, live: live }
    val id = s.events.length()
    all { id < 3, s.pending.get(node) != -2,
      s' = s.with("events", s.events.append(event))
        .with("rows", s.rows.set(node, if (not(live) and BROKEN == "noTombstone") -1 else id))
        .with("known", s.known.set(node, s.known.get(node).union(Set(id))))
        .with("pending", s.pending.set(node, -2))
        .with("regressed", s.regressed or (current != -1 and not(greater(event, s.events[current])))) }
  }
  action deliver(node: int, id: int): bool = all { s.events.indices().contains(id),
    s' = s.with("rows", s.rows.set(node, choose(s.events, s.rows.get(node), id)))
      .with("known", s.known.set(node, s.known.get(node).union(Set(id)))) }
  action step = { nondet node = nodes.oneOf(); nondet live = Set(true, false).oneOf()
    nondet id = Set(0, 1, 2).oneOf()
    any { begin(node), commit(node, live), deliver(node, id) } }
  // Independent lexicographic maximum, including tombstones and empty deletes.
  val visibleWinners = nodes.forall(node => {
    val history = s.known.get(node)
    val winners = history.filter(id => not(history.exists(other => greater(s.events[other], s.events[id]))))
    val expected = winners.filter(id => s.events[id].live)
    val row = s.rows.get(node)
    val actual = if (row == -1 or not(s.events[row].live)) Set() else Set(row)
    expected == actual
  })
  val inv = visibleWinners and not(s.regressed)
  val tiedConverged = s.events.length() == 2 and s.events[0].clock == s.events[1].clock
    and s.rows.get(0) == 1 and s.rows.get(1) == 1 and inv
  val absentDeleteCaptured = s.events.length() == 1 and not(s.events[0].live)
    and s.rows.get(0) == 0 and inv
  val freshPredecessor = s.events.length() == 3 and s.events[2].clock == 3 and inv
  run equalClockTieTest = init.then(begin(0)).then(begin(1)).then(commit(0, true))
    .then(commit(1, true)).then(deliver(0, 1)).then(deliver(1, 0)).expect(tiedConverged)
  run absentDeleteTest = init.then(begin(0)).then(commit(0, false)).expect(absentDeleteCaptured)
  run stalePreparedWriteTest = init.then(begin(0)).then(commit(0, true)).then(begin(0))
    .then(deliver(1, 0)).then(begin(1)).then(commit(1, true)).then(deliver(0, 1))
    .then(commit(0, true)).expect(freshPredecessor)
}
module regional_item_order { import regional_item_order_model(BROKEN = "none").* }
module regional_item_order_noTieBreak { import regional_item_order_model(BROKEN = "noTieBreak").* }
module regional_item_order_stalePredecessor { import regional_item_order_model(BROKEN = "stalePredecessor").* }
module regional_item_order_noTombstone { import regional_item_order_model(BROKEN = "noTombstone").* }
