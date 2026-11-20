/// Design-level model of SNS delivery safety (W3 design, section 9.3:
/// docs/plans/incomplete/fable-w3-sns-design.md). It checks abstract
/// transitions and the stated transaction assumptions, not a provider.
///
/// Shape: two messages, two subscriptions ("q" delivers to a local queue on
/// the same store, "h" delivers to an HTTP endpoint and has a dead-letter
/// queue), two nodes, one store. Publish is Durable. Materialize, claim,
/// settle, the local enqueue and the dead-letter move are Relaxed: a crash
/// rolls the store back to any state after the last Durable commit.
///
/// Bookkeeping that the invariants read is part of the store and rolls back
/// with it (review C9): the drop log, the acknowledged HTTP deliveries and
/// the enqueues of each claim. The HTTP request itself is outside the store;
/// only the settle that records it counts.
///
/// Two crash kinds: `crash` (the store rolls back and node memory is lost)
/// and `storeCrash` (the store rolls back and the API nodes keep their
/// in-flight attempts). After a store crash a lost claim can be issued again
/// with the same generation and owner; the per-claim nonce tells them apart.
///
/// Variants that must FAIL:
///   `initTwoTxnLocal`: delete the record in one transaction, enqueue in a
///      second one; a crash between them loses the notification (`NeverZero`).
///   `initAttemptLimit`: the old behaviour: after the attempt limit the
///      record is marked failed and never delivered (`NeverZero`).
///   `initNoNonce`: the fence compares generation and owner only; a lost
///      claim issued again lets the old attempt enqueue as well
///      (`OneEnqueuePerClaim`).
module pubsub_delivery {
  pure val MSGS = Set(1, 2)
  pure val SUBS = Set("q", "h")
  pure val NODES = Set(1, 2)
  /// Failed attempts before the retry deadline (a stand-in for time).
  pure val DEADLINE_ATTEMPTS = 2
  pure val CRASHES = 2
  pure val RESULTS = Set("delivered", "transient", "permanent")
  pure val DROP_REASONS = Set(
    "dead_letter_missing", "subscription_deleted", "permanent_no_dlq", "deadline_no_dlq",
  )

  type Key = { m: int, s: str }
  type Delivery = { gen: int, owner: int, nonce: int, leased: bool, attempts: int }
  type Claim = { k: Key, gen: int }
  /// The part of the state that the store holds and a crash rolls back.
  type Store = {
    accepted: Set[int], materialized: Set[int], records: Key -> Delivery, present: Set[Key],
    queue: Key -> int, dlq: Key -> int, endpoint: Key -> int,
    dropped: Set[{ k: Key, reason: str }], enqueues: Claim -> int,
    subExists: str -> bool, dlqExists: str -> bool,
    // Ghost: an attempt changed a record whose claim it does not hold.
    staleEffect: bool,
  }
  type Flight = { n: int, k: Key, gen: int, nonce: int }
  type Config = { twoTxn: bool, attemptLimit: bool, nonce: bool }
  type State = {
    cfg: Config, store: Store, history: List[Store], matches: Key -> bool,
    inFlight: Set[Flight], settling: Flight -> str, deleted: Set[Flight],
    nextNonce: int, crashLeft: int, tags: Set[str],
  }

  pure val KEYS = tuples(MSGS, SUBS).map(((m, s)) => { m: m, s: s })
  pure val noRec: Delivery = { gen: 0, owner: 0, nonce: 0, leased: false, attempts: 0 }
  pure val emptyStore: Store = {
    accepted: Set(), materialized: Set(), records: KEYS.mapBy(_ => noRec), present: Set(),
    queue: KEYS.mapBy(_ => 0), dlq: KEYS.mapBy(_ => 0), endpoint: KEYS.mapBy(_ => 0),
    dropped: Set(), enqueues: Map(), subExists: SUBS.mapBy(_ => true),
    dlqExists: SUBS.mapBy(_ => true), staleEffect: false,
  }
  pure val design: Config = { twoTxn: false, attemptLimit: false, nonce: true }

  var st: State

  pure def initial(cfg: Config, matches: Key -> bool): State = {
    cfg: cfg, store: emptyStore, history: [emptyStore], matches: matches,
    inFlight: Set(), settling: Map(), deleted: Set(), nextNonce: 1,
    crashLeft: CRASHES, tags: Set(),
  }

  action initWith(cfg: Config): bool = {
    nondet chosen = KEYS.powerset().oneOf()
    st' = initial(cfg, KEYS.mapBy(k => chosen.contains(k)))
  }
  action init = initWith(design)
  action initTwoTxnLocal = initWith(design.with("twoTxn", true))
  action initAttemptLimit = initWith(design.with("attemptLimit", true))
  action initNoNonce = initWith(design.with("nonce", false))

  // ------------------------------------------------------------ commits
  /// A Durable commit is on disk with everything before it.
  pure def durable(s: State, after: Store): State =
    s.with("store", after).with("history", [after])
  /// A Relaxed commit can be lost by a crash, with every later commit.
  pure def relaxed(s: State, after: Store): State =
    s.with("store", after).with("history", s.history.append(after))

  /// The fence of the design is `(record, generation, nonce)` (W3S R4): the
  /// owner is not compared, because a nonce is unique for each claim. The
  /// `initNoNonce` variant compares the generation and the owner instead.
  pure def current(s: State, f: Flight): bool =
    val rec = s.store.records.get(f.k)
    s.store.present.contains(f.k) and rec.gen == f.gen
      and (if (s.cfg.nonce) rec.nonce == f.nonce else rec.owner == f.n)
  /// The fence passed for an attempt of another claim (possible only
  /// without the nonce, after a lost claim was issued again).
  pure def isStale(s: State, f: Flight): bool = s.store.records.get(f.k).nonce != f.nonce
  pure def mark(s: State, f: Flight): State =
    if (isStale(s, f)) s.with("store", s.store.with("staleEffect", true)) else s
  pure def remove(store: Store, k: Key): Store =
    store.with("present", store.present.exclude(Set(k)))
  pure def bump(counts: Key -> int, k: Key): Key -> int = counts.set(k, counts.get(k) + 1)
  pure def drop(store: Store, k: Key, reason: str): Store =
    remove(store, k).with("dropped", store.dropped.union(Set({ k: k, reason: reason })))
  pure def countEnqueue(store: Store, f: Flight): Store =
    val c = { k: f.k, gen: f.gen }
    val n = if (store.enqueues.keys().contains(c)) store.enqueues.get(c) else 0
    store.with("enqueues", store.enqueues.put(c, n + 1))

  // ------------------------------------------------------------ actions
  action publish(m: int): bool = all {
    not(st.store.accepted.contains(m)),
    st' = durable(st, st.store.with("accepted", st.store.accepted.union(Set(m)))),
  }

  /// One chunk: the records of every matching subscription and the advance
  /// of the intent commit together (filter at materialization).
  action materialize(m: int): bool = all {
    st.store.accepted.contains(m),
    not(st.store.materialized.contains(m)),
    val made = KEYS.filter(k => k.m == m and st.matches.get(k))
    val after = st.store
      .with("materialized", st.store.materialized.union(Set(m)))
      .with("present", st.store.present.union(made))
      .with("records", st.store.records.keys().mapBy(k =>
        if (made.contains(k)) noRec else st.store.records.get(k)))
    st' = relaxed(st, after),
  }

  action claim(n: int, k: Key): bool = all {
    st.store.present.contains(k),
    not(st.store.records.get(k).leased),
    val rec = st.store.records.get(k)
    val claimed = rec.with("gen", rec.gen + 1).with("owner", n).with("leased", true)
      .with("nonce", st.nextNonce)
    val after = st.store.with("records", st.store.records.set(k, claimed))
    st' = relaxed(st, after)
      .with("inFlight", st.inFlight.union(Set({ n: n, k: k, gen: rec.gen + 1, nonce: st.nextNonce })))
      .with("nextNonce", st.nextNonce + 1),
  }

  /// The lease ends while the holder still works (a slow node).
  action expireLease(k: Key): bool = all {
    st.store.present.contains(k),
    st.store.records.get(k).leased,
    val rec = st.store.records.get(k)
    st' = st.with("store", st.store.with("records", st.store.records.set(k, rec.with("leased", false)))),
  }

  /// The settle decision for a failed attempt, in the attempt that checked
  /// the fence.
  pure def failSettle(s: State, f: Flight, permanent: bool): State =
    val rec = s.store.records.get(f.k)
    val attempts = rec.attempts + 1
    if (not(s.store.subExists.get(f.k.s)))
      relaxed(s, drop(s.store, f.k, "subscription_deleted"))
    else if (not(permanent) and attempts < DEADLINE_ATTEMPTS)
      relaxed(s, s.store.with("records", s.store.records.set(f.k,
        rec.with("leased", false).with("attempts", attempts)))).with("tags", s.tags.union(Set("retry")))
    else if (s.cfg.attemptLimit)
      // Old behaviour: the record is marked failed; nothing counts it.
      relaxed(s, remove(s.store, f.k)).with("tags", s.tags.union(Set("marked_failed")))
    else if (f.k.s == "h" and s.store.dlqExists.get(f.k.s))
      relaxed(s, remove(s.store, f.k).with("dlq", bump(s.store.dlq, f.k))).with("tags", s.tags.union(Set("dead_lettered")))
    else
      val reason = if (f.k.s == "h") "dead_letter_missing" else if (permanent) "permanent_no_dlq" else "deadline_no_dlq"
      relaxed(s, drop(s.store, f.k, reason))

  /// One transaction: fence, enqueue into the local queue, delete the record.
  action deliverLocal(f: Flight): bool = all {
    not(st.cfg.twoTxn),
    st.inFlight.contains(f),
    f.k.s == "q",
    val rest = st.with("inFlight", st.inFlight.exclude(Set(f)))
    if (current(st, f))
      st' = relaxed(rest, countEnqueue(remove(st.store, f.k).with("queue", bump(st.store.queue, f.k)), f)
        .with("staleEffect", st.store.staleEffect or isStale(st, f)))
        .with("tags", st.tags.union(Set("local_delivered")))
    else
      st' = rest.with("tags", st.tags.union(Set("claim_lost"))),
  }

  /// Variant: the record is deleted in one transaction ...
  action deleteFirst(f: Flight): bool = all {
    st.cfg.twoTxn,
    st.inFlight.contains(f),
    f.k.s == "q",
    current(st, f),
    st' = relaxed(st, remove(st.store, f.k))
      .with("inFlight", st.inFlight.exclude(Set(f)))
      .with("deleted", st.deleted.union(Set(f))),
  }

  /// ... and the message is enqueued in a second one.
  action enqueueLater(f: Flight): bool = all {
    st.deleted.contains(f),
    st' = relaxed(st, countEnqueue(st.store.with("queue", bump(st.store.queue, f.k)), f))
      .with("deleted", st.deleted.exclude(Set(f))),
  }

  /// The HTTP request. Its effect counts only when the settle commits.
  action httpSend(f: Flight): bool = all {
    st.inFlight.contains(f),
    f.k.s == "h",
    nondet result = RESULTS.oneOf()
    st' = st
      .with("inFlight", st.inFlight.exclude(Set(f)))
      .with("settling", st.settling.put(f, result)),
  }

  action settle(f: Flight): bool = all {
    st.settling.keys().contains(f),
    val result = st.settling.get(f)
    val rest = st.with("settling", st.settling.keys().exclude(Set(f)).mapBy(g => st.settling.get(g)))
    if (not(current(st, f)))
      st' = rest.with("tags", st.tags.union(Set("claim_lost")))
    else if (result == "delivered")
      st' = relaxed(mark(rest, f), remove(mark(rest, f).store, f.k).with("endpoint", bump(st.store.endpoint, f.k)))
    else
      st' = failSettle(mark(rest, f), f, result == "permanent"),
  }

  action unsubscribe(s: str): bool = all {
    st.store.subExists.get(s),
    st' = durable(st, st.store.with("subExists", st.store.subExists.set(s, false))),
  }

  action deleteDlq(s: str): bool = all {
    st.store.dlqExists.get(s),
    st' = durable(st, st.store.with("dlqExists", st.store.dlqExists.set(s, false))),
  }

  /// Keep any prefix of the Relaxed commits after the last Durable one.
  pure def rollback(s: State, index: int): State =
    val kept = s.history.slice(0, index + 1)
    s.with("store", kept[index]).with("history", kept).with("crashLeft", s.crashLeft - 1)
      .with("tags", if (index < s.history.length() - 1) s.tags.union(Set("crash_lost_commit")) else s.tags)

  /// The store and the nodes crash: node memory is lost.
  action crash: bool = all {
    st.crashLeft > 0,
    nondet index = 0.to(st.history.length() - 1).oneOf()
    st' = rollback(st, index).with("inFlight", Set()).with("settling", Map()).with("deleted", Set()),
  }

  /// Only the store crashes: the API nodes keep their in-flight attempts,
  /// so a lost claim can be issued again while its attempt still runs.
  action storeCrash: bool = all {
    st.crashLeft > 0,
    nondet index = 0.to(st.history.length() - 1).oneOf()
    st' = rollback(st, index).with("tags", st.tags.union(Set("store_crash"))),
  }

  action step = any {
    nondet m = MSGS.oneOf()
    any { publish(m), materialize(m) },
    nondet k = KEYS.oneOf()
    nondet n = NODES.oneOf()
    any { claim(n, k), expireLease(k) },
    nondet f = st.inFlight.union(st.deleted).union(st.settling.keys()).oneOf()
    any { deliverLocal(f), deleteFirst(f), enqueueLater(f), httpSend(f), settle(f) },
    nondet s = SUBS.oneOf()
    any { unsubscribe(s), deleteDlq(s) },
    crash,
    storeCrash,
  }

  // ------------------------------------------------------------ properties
  /// No accepted notification for a matching subscription is ever lost
  /// without a committed, counted drop.
  val NeverZero = st.store.accepted.forall(m => SUBS.forall(s =>
    val k = { m: m, s: s }
    not(st.matches.get(k))
      or not(st.store.materialized.contains(m))
      or st.store.present.contains(k)
      or st.store.queue.get(k) > 0
      or st.store.endpoint.get(k) > 0
      or st.store.dlq.get(k) > 0
      or st.store.dropped.exists(d => d.k == k and DROP_REASONS.contains(d.reason))))
  /// No record exists for a pair that the filter excluded.
  val NoFilteredRecord = st.store.present.forall(k => st.matches.get(k))
  /// One claim generation enqueues at most once.
  val OneEnqueuePerClaim = st.store.enqueues.keys().forall(c => st.store.enqueues.get(c) <= 1)
  /// Every committed settle or enqueue came from the attempt of the claim
  /// that the record held (generation, owner and nonce).
  val FencedEffects = not(st.store.staleEffect)
  val inv = NeverZero and NoFilteredRecord and OneEnqueuePerClaim and FencedEffects

  val dead_letter_witness = st.tags.contains("dead_lettered")
  val crash_redelivery_witness = st.tags.contains("crash_lost_commit") and st.tags.contains("local_delivered")
  val takeover_witness = st.tags.contains("claim_lost")
  val retry_witness = st.tags.contains("retry")
  val store_crash_witness = st.tags.contains("store_crash") and st.tags.contains("claim_lost")
}
