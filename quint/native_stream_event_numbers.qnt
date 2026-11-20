/// Root-owned event numbers. Native snapshots and atomic commit are premises.
/// The accepted event list is independent of physical last-write-wins keys.
module native_stream_event_numbers_model {
  const BROKEN: str
  const ROCKS: bool
  const LIMIT: int
  type Event = { id: int, table: int, route: int, ordinal: int }
  // Phase: idle=0, open=1, committed=2, aborted=3, rejected=4.
  type Attempt = { phase: int, next: int, events: List[Event], base: int, retry: bool }
  type Accepted = { event: Event, base: int, order: int, index: int }
  // One fixed source scope; route identifies the shard/lineage pair. Table is absent.
  type Key = (int, int, int, int)
  var attempts: int -> Attempt
  var inventory: List[Accepted]
  var feed: Key -> int
  var entries: (int, Key) -> int
  var nextId: int
  var commits: int
  var sequence: int
  var views: int
  var flushes: int
  var routes: int
  var aborts: int
  var retryCommits: int
  var rejectedEvents: Set[int]
  var rejections: int
  pure def empty(base) = { phase: 0, next: 0, events: List(), base: base, retry: false }
  action init = all {
    attempts' = Map(0 -> empty(0), 1 -> empty(0)), inventory' = List(),
    feed' = Map(), entries' = Map(), nextId' = 0, commits' = 0, sequence' = 0,
    views' = 0, flushes' = 0, routes' = 0, aborts' = 0, rejections' = 0, retryCommits' = 0, rejectedEvents' = Set(),
  }
  action begin(id) = all {
    Set(0, 3).contains(attempts.get(id).phase),
    not(ROCKS) or attempts.keys().forall(k => not(Set(1, 4).contains(attempts.get(k).phase))),
    attempts' = attempts.set(id, empty(sequence).with("phase", 1).with("retry", attempts.get(id).phase == 3)),
    inventory' = inventory, feed' = feed, entries' = entries, nextId' = nextId, commits' = commits, sequence' =
    sequence, views' = views, flushes' = flushes, routes' = routes, aborts' = aborts, rejections' = rejections,
    retryCommits' = retryCommits, rejectedEvents' = rejectedEvents,
  }
  action emit(id, table, route) = {
    val a = attempts.get(id)
    val allowed = a.next <= LIMIT or BROKEN == "wrap"
    val ordinal = if (BROKEN == "wrap") a.next % (LIMIT + 1) else a.next
    all { a.phase == 1, a.events.length() < 5,
      attempts' = if (allowed) attempts.set(id, a.with("next", a.next + 1)
        .with("events", a.events.append({ id: nextId, table: table, route: route, ordinal: ordinal }))) else attempts.set(id, a.with("phase", 4)),
      nextId' = nextId + (if (allowed) 1 else 0),
      rejections' = rejections + (if (allowed) 0 else 1),
      rejectedEvents' = if (allowed) rejectedEvents else rejectedEvents.union(a.events.indices().map(i => a.events[i].id)),
      inventory' = inventory, feed' = feed, entries' = entries, commits' = commits, sequence' = sequence, views' =
      views, flushes' = flushes, routes' = routes, aborts' = aborts, retryCommits' = retryCommits,
    }
  }
  // A new joined view, flush, and route install/reload all keep the root counter.
  action changeView(id, kind) = all {
    attempts.get(id).phase == 1,
    attempts' = if (BROKEN == kind) attempts.set(id, attempts.get(id).with("next", 0)) else attempts,
    views' = views + (if (kind == "view") 1 else 0),
    flushes' = flushes + (if (kind == "flush") 1 else 0),
    routes' = routes + (if (kind == "route") 1 else 0),
    inventory' = inventory, feed' = feed, entries' = entries, nextId' = nextId, commits' = commits, sequence' =
    sequence, aborts' = aborts, rejections' = rejections, retryCommits' = retryCommits, rejectedEvents' = rejectedEvents,
  }
  pure def physicalKey(e: Event, base: int, order: int): Key = if (ROCKS)
    (e.route, base + 1 + e.ordinal, 0, 0)
    else (e.route, 1, if (BROKEN == "order") 0 else order, e.ordinal)
  action commit(id) = {
    val a = attempts.get(id)
    val accepted = a.events.foldl(List(), (rows, e) => rows.append({
      event: e, base: a.base, order: commits, index: rows.length() }))
    all { a.phase == 1,
      inventory' = inventory.concat(accepted),
      feed' = a.events.foldl(feed, (m, e) => m.put(physicalKey(e, a.base, commits), e.id)),
      entries' = a.events.foldl(entries, (m, e) => m.put((e.table, physicalKey(e, a.base, commits)), e.id)),
      attempts' = attempts.set(id, a.with("phase", 2)), commits' = commits + 1, retryCommits' = retryCommits + (if (a.retry) 1 else 0),
      // Serialized Rocks admission supplies a fresh native sequence after commit.
      sequence' = sequence + a.events.length() + 1, rejectedEvents' = rejectedEvents,
      nextId' = nextId, views' = views, flushes' = flushes, routes' = routes, aborts' = aborts, rejections' = rejections,
    }
  }
  action abort(id) = all {
    Set(1, 4).contains(attempts.get(id).phase),
    attempts' = attempts.set(id, empty(sequence).with("phase", 3)), aborts' = aborts + 1,
    inventory' = inventory, feed' = feed, entries' = entries, nextId' = nextId, commits' = commits, sequence' =
    sequence, views' = views, flushes' = flushes, routes' = routes, rejections' = rejections, retryCommits' =
    retryCommits, rejectedEvents' = rejectedEvents,
  }
  action step = {
    nondet id = Set(0, 1).oneOf()
    nondet table = Set(0, 1).oneOf()
    nondet route = Set(0, 1).oneOf()
    nondet kind = Set("view", "flush", "route").oneOf()
    any { begin(id), emit(id, table, route), changeView(id, kind), commit(id), abort(id) }
  }
  val inv = all {
    inventory.indices().forall(i => not(rejectedEvents.contains(inventory[i].event.id))),
    inventory.indices().forall(i => {
      val c = inventory[i]
      val key = physicalKey(c.event, c.base, c.order)
      all { c.event.ordinal == c.index, c.event.ordinal <= LIMIT,
        feed.keys().contains(key), feed.get(key) == c.event.id,
        entries.keys().contains((c.event.table, key)), entries.get((c.event.table, key)) == c.event.id }
    }),
    feed.keys().size() == inventory.length(), entries.keys().size() == inventory.length(),
    feed.keys().forall(k => inventory.indices().exists(i => inventory[i].event.id == feed.get(k))),
    entries.keys().forall(k => inventory.indices().exists(i => inventory[i].event.id == entries.get(k))),
  }
  val joinedViews = views > 0 and inventory.length() >= 2
  val flushedViews = flushes > 0 and views > 0 and inventory.length() >= 2
  val reinstalledRoutes = routes > 0 and inventory.length() >= 2
  val bothTablesRoutes = inventory.indices().exists(i => inventory[i].event.table == 0 and inventory[i].event.route == 0) and
    inventory.indices().exists(i => inventory[i].event.table == 1 and inventory[i].event.route == 1)
  val nativeOrders = commits == 2 and inventory.indices().exists(i => inventory[i].order == 0) and inventory.indices().exists(i => inventory[i].order == 1)
  val freshRetry = retryCommits > 0 and nextId > inventory.length()
  val exhausted = rejections > 0 and rejectedEvents.size() >= LIMIT + 1
  action joinedPair = begin(0).then(emit(0, 0, 0)).then(changeView(0, "view"))
    .then(emit(0, 1, 0)).then(commit(0))
  run givenJoinedViewsWhenTablesShareLineageThenBothFeedRecordsSurviveTest =
    init.then(joinedPair).expect(inv and inventory.length() == 2)
  run givenFlushAndReloadWhenNewViewWritesThenOrderContinuesTest =
    init.then(begin(0)).then(emit(0, 0, 0)).then(changeView(0, "flush"))
      .then(changeView(0, "view")).then(changeView(0, "route"))
      .then(emit(0, 1, 0)).then(emit(0, 0, 1)).then(commit(0)).expect(inv and inventory.length() == 3)
  run givenAbortedEventsWhenFreshRetryCommitsThenAbortedEventsAreAbsentTest =
    init.then(begin(0)).then(emit(0, 0, 0)).then(abort(0)).then(begin(0))
      .then(emit(0, 1, 1)).then(commit(0)).expect(inv and freshRetry and inventory.length() == 1)
  run givenFullCounterWhenAnotherEventRequestedThenAttemptRejectsAndFreshRetryRestartsTest =
    init.then(begin(0)).then(emit(0, 0, 0)).then(emit(0, 1, 0)).then(emit(0, 0, 1))
      .then(emit(0, 1, 1)).expect(inv and exhausted and attempts.get(0).phase == 4 and
        attempts.get(0).next == 3 and attempts.get(0).events.length() == 3 and
        attempts.get(0).events.indices().forall(i => attempts.get(0).events[i].ordinal == i) and
        inventory.length() == 0 and feed.keys().size() == 0 and entries.keys().size() == 0)
      .then(abort(0)).then(begin(0)).expect(attempts.get(0).next == 0)
      .then(emit(0, 1, 1)).then(commit(0)).expect(inv and freshRetry and inventory.length() == 1)
}
module native_stream_event_numbers {
  import native_stream_event_numbers_model(BROKEN = "none", ROCKS = false, LIMIT = 2).*
  run givenConcurrentNativeTransactionsWhenSameVersionCommitsThenNativeOrderSeparatesKeysTest =
    init.then(begin(0)).then(begin(1)).then(emit(0, 0, 0)).then(emit(1, 1, 0))
      .then(commit(1)).then(commit(0)).expect(inv and nativeOrders and inventory.length() == 2)
}
module native_stream_event_numbers_rocks { import native_stream_event_numbers_model(BROKEN = "none", ROCKS = true, LIMIT = 2).* }
module native_stream_event_numbers_view { import native_stream_event_numbers_model(BROKEN = "view", ROCKS = false, LIMIT = 2).* }
module native_stream_event_numbers_flush { import native_stream_event_numbers_model(BROKEN = "flush", ROCKS = false, LIMIT = 2).* }
module native_stream_event_numbers_route { import native_stream_event_numbers_model(BROKEN = "route", ROCKS = false, LIMIT = 2).* }
module native_stream_event_numbers_wrap { import native_stream_event_numbers_model(BROKEN = "wrap", ROCKS = false, LIMIT = 2).* }
module native_stream_event_numbers_order { import native_stream_event_numbers_model(BROKEN = "order", ROCKS = false, LIMIT = 2).* }
