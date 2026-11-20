/// Two-region echo cleanup (design B, R1 and R2), one key. Regions 0 (A) and 1 (B);
/// region id is the order rank. Origins keep a marker only for their own deletes; a
/// receiver that applies a delete stores nothing and appends an echo in the same step.
/// Delivery is a contiguous prefix per peer stream; loss is a cursor that does not
/// move, duplicates below the cursor are ignored (not modelled as state changes).
/// Ghost: every local operation records the ops its region had incorporated (past).
/// JOIN != "off": B starts hidden (no local writes), A also sends stale scan pages;
/// B keeps received markers while Joining; activation only closes the scan, and a
/// later sweep removes each kept marker. "sweep": a receiver that keeps a marker
/// echoes only in the step that removes it. "asWritten": echo at receive (R2 text).
/// Not covered: lineage split, native positions, unknown commit outcomes.
module regional_pair_echo_model {
  const BROKEN: str
  const JOIN: str
  pure val N = 3
  pure val MAX_OPS = 4
  pure val MAX_PAGES = 2
  pure val REGIONS = Set(0, 1)
  type Cell = { kind: str, n: int, r: int, op: int }
  type Op = { region: int, kind: str, n: int, past: Set[int], overKept: bool }
  var st: int -> Cell
  var streams: int -> List[Cell]
  var got: int -> Set[int]
  var ops: List[Op]
  var seen: int -> Set[int]
  var joining: bool
  var pages: Set[Cell]

  pure val ABSENT: Cell = { kind: "absent", n: 0, r: 0, op: -1 }
  pure val NONE: Cell = { kind: "none", n: 0, r: 0, op: -1 }
  pure def gt(n1: int, r1: int, n2: int, r2: int): bool = n1 > n2 or (n1 == n2 and r1 > r2)
  pure def echoOf(c: Cell): Cell = { kind: "echo", n: c.n, r: c.r, op: c.op }

  action init = all {
    st' = Map(0 -> ABSENT, 1 -> ABSENT),
    streams' = Map(0 -> List(), 1 -> List()),
    got' = Map(0 -> Set(), 1 -> Set()),
    ops' = List(),
    seen' = Map(0 -> Set(), 1 -> Set()),
    joining' = JOIN != "off",
    pages' = Set(),
  }

  // Local put or delete: any number above the stored winner's number, any number if absent.
  action local(r: int, kind: str, n: int): bool = {
    val c = st.get(r)
    val id = ops.length()
    val silent = kind == "del" and c.kind == "absent" and BROKEN == "noAbsentMarker"
    val after = if (silent) c else
      { kind: if (kind == "put") "item" else "marker", n: n, r: r, op: id }
    all {
      id < MAX_OPS, 1 <= n, n <= N, c.kind == "absent" or n > c.n, r == 0 or not(joining),
      ops' = ops.append({ region: r, kind: kind, n: n, past: seen.get(r),
        overKept: r == 1 and c.kind == "marker" and c.r == 0 }),
      seen' = seen.set(r, seen.get(r).union(Set(id))),
      st' = st.set(r, after),
      streams' = if (silent) streams
        else streams.set(r, streams.get(r).append({ kind: kind, n: n, r: r, op: id })),
      got' = got, joining' = joining, pages' = pages,
    }
  }

  // Region r applies position j of the peer stream.
  action receive(r: int, j: int): bool = {
    val p = 1 - r
    val c = st.get(r)
    val e = if (j < streams.get(p).length()) streams.get(p)[j] else NONE
    val beaten = c.kind != "absent" and gt(c.n, c.r, e.n, e.r)
    val putWins = e.kind == "put" and (c.kind == "absent" or gt(e.n, e.r, c.n, c.r))
    val delApplies = e.kind == "del" and not(beaten)
    val keepMarker = r == 1 and joining and BROKEN != "joinNoMarker"
    val echoes = delApplies and not(keepMarker and JOIN == "sweep")
    val echoClears = e.kind == "echo" and c.kind == "marker" and BROKEN != "ackCleanup" and
      (BROKEN == "anyOrderEcho" or (c.n == e.n and c.r == e.r))
    val after = if (putWins) { kind: "item", n: e.n, r: e.r, op: e.op }
      else if (delApplies and keepMarker) { kind: "marker", n: e.n, r: e.r, op: e.op }
      else if (delApplies or echoClears) ABSENT else c
    all {
      e.kind != "none", not(got.get(r).contains(j)),
      BROKEN == "reorder" or j == got.get(r).size(),
      got' = got.set(r, got.get(r).union(Set(j))),
      st' = st.set(r, after),
      streams' = if (echoes) streams.set(r, streams.get(r).append(echoOf(e))) else streams,
      seen' = if (e.kind == "echo") seen
        else seen.set(r, seen.get(r).union(ops[e.op].past).union(Set(e.op))),
      ops' = ops, joining' = joining, pages' = pages,
    }
  }

  // ackCleanup variant: drop the marker once the peer cursor passed the Delete entry.
  action ackClean(r: int): bool = {
    val c = st.get(r)
    all {
      BROKEN == "ackCleanup", c.kind == "marker",
      got.get(1 - r).exists(j => streams.get(r)[j].kind == "del" and
        streams.get(r)[j].n == c.n and streams.get(r)[j].r == c.r),
      st' = st.set(r, ABSENT), streams' = streams, got' = got, ops' = ops, seen' = seen,
      joining' = joining, pages' = pages,
    }
  }

  // Joining: A captures its current item image; delivery to B may be arbitrarily late.
  action scan = all {
    joining, st.get(0).kind == "item", pages.size() < MAX_PAGES,
    pages' = pages.union(Set(st.get(0))),
    st' = st, streams' = streams, got' = got, ops' = ops, seen' = seen, joining' = joining,
  }
  action deliverPage(pg: Cell): bool = {
    val c = st.get(1)
    val wins = joining and (c.kind == "absent" or gt(pg.n, pg.r, c.n, c.r))
    all {
      pages.contains(pg), pages' = pages.exclude(Set(pg)),
      st' = if (wins) st.set(1, pg) else st,
      seen' = if (wins) seen.set(1, seen.get(1).union(ops[pg.op].past).union(Set(pg.op)))
        else seen,
      streams' = streams, got' = got, ops' = ops, joining' = joining,
    }
  }
  // Activation closes the scan (later pages are rejected) and allows B local writes.
  action activate = all { joining, joining' = false,
    st' = st, streams' = streams, got' = got, ops' = ops, seen' = seen, pages' = pages }
  // A later bounded sweep removes a kept marker (order region A) and, under "sweep",
  // appends its echo in the same step.
  action sweep = {
    val c = st.get(1)
    all {
      JOIN != "off", not(joining), c.kind == "marker", c.r == 0,
      st' = st.set(1, ABSENT),
      streams' = if (JOIN == "sweep" and BROKEN != "sweepNoEcho")
        streams.set(1, streams.get(1).append(echoOf(c))) else streams,
      got' = got, ops' = ops, seen' = seen, joining' = joining, pages' = pages,
    }
  }

  action idle = all { st' = st, streams' = streams, got' = got, ops' = ops, seen' = seen,
    joining' = joining, pages' = pages }

  action step = {
    nondet r = REGIONS.oneOf()
    nondet n = 1.to(N).oneOf()
    nondet j = 0.to(2 * MAX_OPS).oneOf()
    any { local(r, "put", n), local(r, "del", n), receive(r, j), ackClean(r), scan,
      activate, sweep, idle, all { pages != Set(), nondet pg = pages.oneOf() deliverPage(pg) } }
  }

  // Properties, evaluated at quiescence: both cursors at the end of the peer stream,
  // B active, no scan page in flight and no kept marker (order region A) at B.
  val quiescent = not(joining) and pages == Set() and
    not(st.get(1).kind == "marker" and st.get(1).r == 0) and
    REGIONS.forall(r => got.get(r).size() == streams.get(1 - r).length())
  def visible(r: int): int = if (st.get(r).kind == "item") st.get(r).op else -1
  val maximal = ops.indices().filter(i => not(ops.indices().exists(k => ops[k].past.contains(i))))
  // Causally maximal ops are pairwise concurrent (at most one per region); LWW picks the
  // greatest order among them. A delete winner means absent, a put winner means its item.
  val winners = maximal.filter(i => maximal.forall(k => k == i or
    gt(ops[i].n, ops[i].region, ops[k].n, ops[k].region)))
  val expected = winners.fold(-1, (acc, i) => if (ops[i].kind == "put") i else acc)
  val converged = quiescent implies visible(0) == visible(1)
  val causalWinner = quiescent implies REGIONS.forall(r => visible(r) == expected)
  val noMarkerLeak = quiescent implies REGIONS.forall(r => st.get(r).kind != "marker")
  val inv = converged and causalWinner and noMarkerLeak

  // Witnesses; each negation must be violated by TLC, which proves reachability.
  pure def concurrent(a: Op, ia: int, b: Op, ib: int): bool =
    not(a.past.contains(ib)) and not(b.past.contains(ia))
  val noFloorWitness = quiescent and inv and visible(0) >= 0 and
    ops.indices().forall(k => ops[k].region == 0) and
    ops.indices().exists(d => ops[d].kind == "del" and d < visible(0) and
      ops[d].n > ops[visible(0)].n)
  val concurrentDeleteWitness = quiescent and inv and ops.length() == 2 and
    ops.indices().exists(d => ops.indices().exists(q =>
      ops[d].region == 0 and ops[d].kind == "del" and ops[q].region == 1 and
      ops[q].kind == "put" and concurrent(ops[d], d, ops[q], q) and
      gt(ops[d].n, 0, ops[q].n, 1)))
  val ttlWitness = quiescent and inv and ops.length() == 2 and ops[0].kind == "del" and
    ops[1].kind == "del" and ops[0].region != ops[1].region and ops[0].n == ops[1].n and
    concurrent(ops[0], 0, ops[1], 1)
  val noFloorUnreached = not(noFloorWitness)
  val concurrentDeleteUnreached = not(concurrentDeleteWitness)
  val ttlUnreached = not(ttlWitness)
  // Joining: a stale page arrives after the delete it predates and is rejected.
  val stalePageUnreached = not(quiescent and inv and ops.length() == 2 and
    ops[0].kind == "put" and ops[1].kind == "del" and ops[0].region == 0 and
    ops[1].region == 0 and seen.get(1).contains(1))
  // Joining: B writes after activation over a kept marker, before any sweep.
  val activeWriteUnreached = not(quiescent and inv and
    ops.indices().exists(k => ops[k].overKept))

  run noFloorTest = init.then(local(0, "del", 2)).then(receive(1, 0)).then(receive(0, 0))
    .then(local(0, "put", 1)).then(receive(1, 1))
    .expect(quiescent and visible(0) == 1 and visible(1) == 1 and inv and noFloorWitness)
  run concurrentDeleteTest = init.then(local(0, "del", 2)).then(local(1, "put", 1))
    .then(receive(0, 0)).expect(st.get(0).kind == "marker")
    .then(receive(1, 0)).then(receive(0, 1))
    .expect(quiescent and visible(0) == -1 and visible(1) == -1 and concurrentDeleteWitness)
  run ttlTest = init.then(local(0, "del", 2)).then(local(1, "del", 2)).then(receive(0, 0))
    .then(receive(1, 0)).expect(st.get(1).kind == "marker").then(receive(1, 1))
    .expect(quiescent and ttlWitness)
  run higherPutBeatsMarkerTest = init.then(local(1, "put", 2)).then(local(0, "del", 1))
    .then(receive(0, 0)).then(receive(1, 0))
    .expect(quiescent and visible(0) == 0 and visible(1) == 0 and inv)
  // Joining trace: put, scan, delete; B gets the tail, then the stale page, then activates.
  run joinStalePage = init.then(local(0, "put", 1)).then(scan).then(local(0, "del", 2))
    .then(receive(1, 0)).then(receive(1, 1)).then(deliverPage(streams.get(0)[0]
      .with("kind", "item"))).then(activate)
  // Joining trace: delete echoed, origin rewrites lower, B still holds the marker.
  run joinLowerRewrite = init.then(local(0, "del", 2)).then(receive(1, 0))
    .then(activate).then(sweep).then(receive(0, 0)).then(local(0, "put", 1))
    .then(receive(1, 1))
  // Joining trace: B writes over the kept marker after activation; no sweep, no echo.
  run joinActiveWrite = init.then(local(0, "del", 2)).then(receive(1, 0))
    .then(activate).then(local(1, "put", 3)).then(receive(0, 0))
}
module regional_pair_echo { import regional_pair_echo_model(BROKEN = "none", JOIN = "off").* }
module regional_pair_echo_ackCleanup {
  import regional_pair_echo_model(BROKEN = "ackCleanup", JOIN = "off").*
  run ackCleanupFails = init.then(local(0, "del", 2)).then(local(1, "put", 1))
    .then(receive(1, 0)).then(ackClean(0)).then(receive(0, 0)).then(receive(0, 1))
    .expect(quiescent and not(converged))
}
module regional_pair_echo_anyOrderEcho {
  import regional_pair_echo_model(BROKEN = "anyOrderEcho", JOIN = "off").*
  run anyOrderEchoFails = init.then(local(0, "del", 1)).then(receive(1, 0))
    .then(local(0, "del", 2)).then(receive(0, 0)).then(local(1, "put", 1))
    .then(receive(1, 1)).then(receive(0, 1)).then(receive(0, 2))
    .expect(quiescent and not(converged) and not(causalWinner))
}
module regional_pair_echo_noAbsentMarker {
  import regional_pair_echo_model(BROKEN = "noAbsentMarker", JOIN = "off").*
  run noAbsentMarkerFails = init.then(local(1, "put", 1)).then(local(0, "del", 2))
    .then(receive(0, 0)).expect(quiescent and converged and not(causalWinner))
}
module regional_pair_echo_reorder {
  import regional_pair_echo_model(BROKEN = "reorder", JOIN = "off").*
  run reorderFails = init.then(local(0, "put", 1)).then(local(0, "del", 2))
    .then(receive(1, 1)).then(receive(1, 0)).then(receive(0, 0))
    .expect(quiescent and not(converged))
}
module regional_pair_echo_join {
  import regional_pair_echo_model(BROKEN = "none", JOIN = "sweep").*
  run joinStalePageTest = joinStalePage.expect(not(quiescent)).then(sweep)
    .then(receive(0, 0))
    .expect(quiescent and visible(1) == -1 and inv and not(stalePageUnreached))
  run joinLowerRewriteTest = joinLowerRewrite.expect(quiescent and visible(1) == 1 and inv)
  run joinActiveWriteTest = joinActiveWrite
    .expect(quiescent and visible(0) == 1 and visible(1) == 1 and not(activeWriteUnreached))
}
module regional_pair_echo_joinAsWritten {
  import regional_pair_echo_model(BROKEN = "none", JOIN = "asWritten").*
  // Echo at receive lets A clear its marker and rewrite lower while B still keeps the
  // received marker: B rejects the causally later put; the sweep then drops the marker.
  run joinAsWrittenFails = init.then(local(0, "del", 2)).then(receive(1, 0))
    .then(receive(0, 0)).then(local(0, "put", 1)).then(receive(1, 1)).then(activate)
    .then(sweep).expect(quiescent and visible(0) == 1 and visible(1) == -1 and not(converged))
}
module regional_pair_echo_joinNoMarker {
  import regional_pair_echo_model(BROKEN = "joinNoMarker", JOIN = "sweep").*
  run joinNoMarkerFails = joinStalePage.then(receive(0, 0))
    .expect(quiescent and visible(1) == 0 and not(converged))
}
module regional_pair_echo_sweepNoEcho {
  import regional_pair_echo_model(BROKEN = "sweepNoEcho", JOIN = "sweep").*
  run sweepNoEchoFails = init.then(local(0, "del", 2)).then(receive(1, 0)).then(activate)
    .then(sweep).expect(quiescent and st.get(0).kind == "marker" and not(noMarkerLeak))
}
