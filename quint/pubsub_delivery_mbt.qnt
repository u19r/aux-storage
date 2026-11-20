/// Model-based-test boundary of quint/pubsub_delivery.qnt for the Rust driver
/// crates/pubsub/src/quint_pubsub_delivery_tests.rs (spec OWN-C-c, OWN-C-d).
///
/// Time replaces the attempt count: one unit is 20 minutes, the HTTP retry
/// lifetime is 3 units, a lease 1 unit, a retry delay below 1 unit. A claim
/// takes every due record of one destination. "q" is a queue subscription on
/// the same store; "h" an HTTPS subscription with a dead-letter queue.
/// A publish snapshots the topic's subscriptions; materialization writes
/// records from that snapshot, so a subscription deleted after the publish
/// still gets its record (OWN-C-c Change 2).
/// Crashes and the failing variants stay in the design model.
module pubsub_delivery_mbt {
  import pubsub_delivery.* from "pubsub_delivery"

  pure val HTTP_LIFE = 3
  pure val LEASE = 1
  pure val D_HORIZON = 6
  pure val D_CATALOG = 2

  type DRec = { present: bool, gen: int, owner: int, nonce: int, due: int, attempts: int }
  type DFlight = { n: int, k: Key, gen: int, nonce: int, result: str }
  type DState = {
    now: int, published: int -> int, pending: int, snapshot: Set[str], matches: Key -> bool,
    subExists: str -> bool, dlqExists: str -> bool, recs: Key -> DRec, flights: Set[DFlight],
    queue: Key -> int, dlq: Key -> int, endpoint: Key -> int,
    dropped: Set[{ k: Key, reason: str }], created: Set[Key], nextNonce: int, catalogLeft: int,
  }
  type DAnswer = { kind: str, keys: Set[Key] }

  var dState: DState
  var dAnswer: DAnswer

  pure val noRecord: DRec = { present: false, gen: 0, owner: 0, nonce: 0, due: 0, attempts: 0 }
  pure val noFlight: DFlight = { n: 0, k: { m: 0, s: "" }, gen: 0, nonce: 0, result: "" }
  pure def dAns(kind: str): DAnswer = { kind: kind, keys: Set() }

  pure def dInitial(matches: Key -> bool): DState = {
    now: 0, published: MSGS.mapBy(_ => -1), pending: 0, snapshot: Set(), matches: matches,
    subExists: SUBS.mapBy(_ => true), dlqExists: SUBS.mapBy(_ => true),
    recs: KEYS.mapBy(_ => noRecord), flights: Set(),
    queue: KEYS.mapBy(_ => 0), dlq: KEYS.mapBy(_ => 0), endpoint: KEYS.mapBy(_ => 0),
    dropped: Set(), created: Set(), nextNonce: 1, catalogLeft: D_CATALOG,
  }

  /// The settle fence (record, generation, nonce), W3S R4.
  pure def holds(ds: DState, f: DFlight): bool = {
    val r = ds.recs.get(f.k)
    r.present and r.gen == f.gen and r.nonce == f.nonce
  }
  pure def gone(ds: DState, k: Key): DState = ds.with("recs", ds.recs.set(k, noRecord))
  pure def dropKey(ds: DState, k: Key, reason: str): DState =
    gone(ds, k).with("dropped", ds.dropped.union(Set({ k: k, reason: reason })))

  pure def dueKeys(ds: DState, sub: str): Set[Key] =
    KEYS.filter(k => k.s == sub and ds.recs.get(k).present and ds.recs.get(k).due <= ds.now)
  /// Nonces are given in message order within one claim.
  pure def claimNonce(ds: DState, due: Set[Key], k: Key): int =
    if (k.m == 2 and due.contains({ m: 1, s: k.s })) ds.nextNonce + 1 else ds.nextNonce

  /// One claim of every due record of destination `sub` by node `n`.
  pure def dClaim(ds: DState, n: int, sub: str): DState = {
    val due = dueKeys(ds, sub)
    val taken = due.map(k => { n: n, k: k, gen: ds.recs.get(k).gen + 1, nonce: claimNonce(ds, due, k), result: "" })
    ds.with("recs", KEYS.mapBy(k => if (due.contains(k))
        ds.recs.get(k).with("gen", ds.recs.get(k).gen + 1).with("owner", n)
          .with("nonce", claimNonce(ds, due, k)).with("due", ds.now + LEASE)
      else ds.recs.get(k)))
      .with("flights", ds.flights.union(taken))
      .with("nextNonce", ds.nextNonce + due.size())
  }

  /// `DeliveryState::decide` after the fenced settle read.
  pure def settleKind(ds: DState, f: DFlight): str =
    if (not(holds(ds, f))) "claim_lost"
    else if (f.result == "delivered") "delivered"
    else if (not(ds.subExists.get(f.k.s))) "dropped_subscription_deleted"
    else if (f.result == "transient" and ds.now < ds.published.get(f.k.m) + HTTP_LIFE) "retried"
    else if (ds.dlqExists.get(f.k.s)) "dead_lettered"
    else "dropped_dead_letter_missing"

  pure def dSettle(ds: DState, f: DFlight): DState = {
    val rest = ds.with("flights", ds.flights.exclude(Set(f)))
    val kind = settleKind(ds, f)
    val r = ds.recs.get(f.k)
    if (kind == "claim_lost") rest
    else if (kind == "delivered") gone(rest, f.k).with("endpoint", bump(ds.endpoint, f.k))
    else if (kind == "retried")
      rest.with("recs", ds.recs.set(f.k, r.with("attempts", r.attempts + 1).with("owner", 0).with("nonce", 0).with("due", ds.now + 1)))
    else if (kind == "dead_lettered") gone(rest, f.k).with("dlq", bump(ds.dlq, f.k))
    else if (kind == "dropped_subscription_deleted") dropKey(rest, f.k, "subscription_deleted")
    else dropKey(rest, f.k, "dead_letter_missing")
  }

  action dInit = {
    nondet chosen = KEYS.powerset().oneOf()
    all { dState' = dInitial(KEYS.mapBy(k => chosen.contains(k))), dAnswer' = dAns("none") }
  }

  /// dInit without catalog changes: no DUnsubscribe or DDeleteDlq, so the
  /// dead-letter path stays reachable in sampled traces.
  action dInitNoCatalog = {
    nondet chosen = KEYS.powerset().oneOf()
    all {
      dState' = dInitial(KEYS.mapBy(k => chosen.contains(k))).with("catalogLeft", 0),
      dAnswer' = dAns("none"),
    }
  }

  action DPublish = {
    nondet m = MSGS.oneOf()
    all {
      dState.published.get(m) < 0,
      dState.pending == 0,
      dState' = dState.with("published", dState.published.set(m, dState.now)).with("pending", m)
        .with("snapshot", SUBS.filter(s => dState.subExists.get(s))),
      dAnswer' = dAns("published"),
    }
  }

  action DMaterialize = {
    val m = dState.pending
    val made = KEYS.filter(k => k.m == m and dState.matches.get(k) and dState.snapshot.contains(k.s))
    all {
      m != 0,
      dState' = dState.with("pending", 0).with("snapshot", Set()).with("created", dState.created.union(made))
        .with("recs", KEYS.mapBy(k => if (made.contains(k))
          { present: true, gen: 0, owner: 0, nonce: 0, due: dState.published.get(m), attempts: 0 }
          else dState.recs.get(k))),
      dAnswer' = { kind: "materialized", keys: made },
    }
  }

  action DClaim = {
    nondet node = NODES.oneOf()
    nondet sub = SUBS.oneOf()
    all {
      dState' = dClaim(dState, node, sub),
      dAnswer' = { kind: "claimed", keys: dueKeys(dState, sub) },
    }
  }

  /// Fence, enqueue into the local queue and delete the record, in one attempt.
  action DDeliverLocal = {
    nondet f = dState.flights.union(Set(noFlight)).oneOf()
    val rest = dState.with("flights", dState.flights.exclude(Set(f)))
    all {
      f.nonce > 0,
      f.k.s == "q",
      dState' = if (holds(dState, f)) gone(rest, f.k).with("queue", bump(dState.queue, f.k)) else rest,
      dAnswer' = { kind: if (holds(dState, f)) "delivered" else "claim_lost", keys: Set(f.k) },
    }
  }

  /// The HTTP request: no store change; its result waits for the settle.
  action DHttpSend = {
    nondet f = dState.flights.union(Set(noFlight)).oneOf()
    nondet result = RESULTS.oneOf()
    all {
      f.nonce > 0,
      f.k.s == "h",
      f.result == "",
      dState' = dState.with("flights", dState.flights.exclude(Set(f)).union(Set(f.with("result", result)))),
      dAnswer' = dAns("none"),
    }
  }

  action DSettle = {
    nondet f = dState.flights.union(Set(noFlight)).oneOf()
    all {
      f.nonce > 0,
      f.result != "",
      dState' = dSettle(dState, f),
      dAnswer' = { kind: settleKind(dState, f), keys: Set(f.k) },
    }
  }

  /// Local delivery and claims do not read the subscription: a record of a
  /// deleted "q" subscription is still enqueued; only a failed HTTP settle
  /// reads it (`dropped_subscription_deleted`).
  action DUnsubscribe = {
    nondet sub = SUBS.oneOf()
    all {
      dState.catalogLeft > 0,
      dState.subExists.get(sub),
      dState' = dState.with("subExists", dState.subExists.set(sub, false)).with("catalogLeft", dState.catalogLeft - 1),
      dAnswer' = dAns("ok"),
    }
  }

  action DDeleteDlq = all {
    dState.catalogLeft > 0,
    dState.dlqExists.get("h"),
    dState' = dState.with("dlqExists", dState.dlqExists.set("h", false)).with("catalogLeft", dState.catalogLeft - 1),
    dAnswer' = dAns("ok"),
  }

  action DTick = all {
    dState.now < D_HORIZON,
    dState' = dState.with("now", dState.now + 1),
    dAnswer' = dAns("none"),
  }

  action dStep = any {
    DPublish, DMaterialize, DClaim, DClaim, DDeliverLocal, DDeliverLocal, DHttpSend, DHttpSend,
    DSettle, DSettle, DUnsubscribe, DDeleteDlq, DTick, DTick,
  }

  /// NeverZero, NoFilteredRecord and OneEnqueuePerClaim of the design, over the boundary state.
  val dInv = all {
    dState.created.forall(k => dState.recs.get(k).present or dState.queue.get(k) > 0
      or dState.endpoint.get(k) > 0 or dState.dlq.get(k) > 0 or dState.dropped.exists(d => d.k == k)),
    KEYS.forall(k => dState.recs.get(k).present implies dState.matches.get(k)),
    KEYS.forall(k => dState.queue.get(k) <= 1),
  }

  pure val oneHttpRecord: DState = {
    val d0 = dInitial(KEYS.mapBy(_ => true)).with("published", MSGS.mapBy(m => if (m == 1) 0 else -1))
    d0.with("recs", d0.recs.set({ m: 1, s: "h" }, { present: true, gen: 0, owner: 0, nonce: 0, due: 0, attempts: 0 }))
      .with("created", Set({ m: 1, s: "h" }))
  }

  run dTakeoverLosesOldClaimTest = {
    val c1 = dClaim(oneHttpRecord, 1, "h")
    val c2 = dClaim(c1.with("now", 1), 2, "h")
    val old = c1.flights.filter(f => f.n == 1).getOnlyElement().with("result", "delivered")
    all { assert(settleKind(c2, old) == "claim_lost"), assert(c2.recs.get({ m: 1, s: "h" }).gen == 2) }
  }

  run dDeadlineEndsRetriesTest = {
    val c1 = dClaim(oneHttpRecord, 1, "h")
    val f1 = c1.flights.getOnlyElement().with("result", "transient")
    all {
      assert(settleKind(c1, f1) == "retried"),
      assert(settleKind(c1.with("now", 3), f1) == "dead_lettered"),
      assert(settleKind(c1.with("now", 3).with("dlqExists", c1.dlqExists.set("h", false)), f1) == "dropped_dead_letter_missing"),
    }
  }
}
