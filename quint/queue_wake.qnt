/// Design-level protocol model of the receive wait path (W1, the rules being
/// implemented in crates/queue/src/manager/{mod.rs,receive_wait.rs}) and of
/// the N1 slot-generation wake design
/// (docs/plans/incomplete/fable-n1-queue-wake-design.md).
/// It checks abstract transitions, not a driver, watch delivery, or timing.
///
/// Shape: one queue (so one wake slot), two API nodes, three messages, four
/// receive slots (waiters 1, 2, 4 on node 1, waiter 3 on node 2), one shared
/// database. Node memory (observation, deep flag, slot state) is only a hint;
/// the claim transaction is the only authority.
///
/// Time: one true clock `now`; node 2 may run DELTA ahead (cfg.skewed). Due
/// times are absolute and written with the writer's clock. Transactions and
/// wake reactions finish inside the tick in which they become enabled: `Tick`
/// is enabled only when no node has urgent work (timer at or before now,
/// transaction in flight, watch that must fire, re-arm that is due). This
/// timeliness assumption turns the bounded-response requirement into a
/// checkable state invariant (`w2BoundedResponse`).
module queue_wake {
  pure val NODES = 1.to(2)
  pure val WAITERS = 1.to(4)
  pure val MSGS = 1.to(3)
  pure val MSG_LIST = [1, 2, 3]
  pure def nodeOf(w: int): int = if (w == 3) 2 else 1
  pure val LIMIT = 2
  pure val P = 2          // probe interval
  pure val R = 1          // N1 minimum re-arm interval
  pure val F = 6          // N1 fallback interval
  pure val V = 2          // visibility timeout
  pure val WAIT = 6       // long-poll wait time
  pure val BOUND = 2      // waiting receives per node
  pure val BACKOFF = 1    // short backoff after a claim that left due messages
  pure val DELTA = 1
  pure val HORIZON = 14
  pure val NONE = 1000
  pure val NEVER = -1000

  type Msg = { present: bool, due: int, holder: int, gen: int, sentBy: int, delayed: bool }
  /// Node-local observation of the queue (a hint). `source` names the
  /// evidence behind `nextDue` for witnesses.
  type Obs = { observedAt: int, seq: int, nextDue: int, gen: int, epoch: int, evidenceAt: int, deep: bool, source: str }
  type Probe = { active: bool, startedAt: int, seq: int, gen: int, epoch: int, nextDue: int }
  type Waiter = {
    phase: str, deadline: int, waitTime: int, oneShot: bool,
    claimStart: int, claimSeq: int, claimGen: int, claimSnap: Set[(int, int, int)], reason: str,
  }
  /// N1 slot machine of one node. txn: "none", "arming" (one read
  /// transaction: generation, watch, probe), or "probed" (wrong order only).
  type Slot = {
    state: str, armGen: int, armedAt: int, firedAt: int, lost: bool,
    txn: str, txnSeq: int, txnGen: int, txnObsGen: int, txnEpoch: int, txnAt: int, txnNextDue: int, txnSeen: Set[int], afterFire: bool,
  }
  type Config = {
    design: str,                 // "probe" (W1 wait path) or "n1"
    gateUsesDrained: bool,       // fix: a claim that left due entries records due + backoff
    claimReportsNextDue: bool,   // fix: a claim returns the earliest future due time
    expireEvidence: bool,        // fix: claim evidence (signal, full page, probe) is valid for P
    overBoundShared: bool,       // fix: an over-bound receive uses the shared observation/probe
    slotLifecycleFix: bool,      // fix: no arming without waiters; Armed lingers until fallback
    armThenProbe: bool,          // N1 rule: arm and probe in one transaction
    fallback: bool,
    lossy: bool,                 // watches may be lost or fire spuriously
    crashes: bool,               // a Relaxed claim may be undone (no commit signal)
    underfill: bool,             // a claim may return fewer than the due entries (slot, redrive)
    skewed: bool,
    trustHint: bool,             // mutation: an over-bound receive answers from its probe
    seqOrder: bool,              // fix: order observations by a start sequence, not by start instant
  }
  type State = {
    cfg: Config, now: int, msgs: int -> Msg, slotGen: int, seqs: int -> int,
    obs: int -> Obs, probe: int -> Probe, slot: int -> Slot, waiters: int -> Waiter,
    leftDue: int -> Set[int], exposedSince: int -> int, reads: int -> int, dirty: int -> bool, prevDirty: int -> bool,
    lastCommit: int, writes: int, responses: Set[(int, int, bool)], outcomes: Set[str], violations: Set[str],
  }

  pure val emptyObs: Obs = { observedAt: NEVER, seq: 0, nextDue: NONE, gen: 0, epoch: 0, evidenceAt: NEVER, deep: false, source: "none" }
  pure val idleProbe: Probe = { active: false, startedAt: 0, seq: 0, gen: 0, epoch: 0, nextDue: NONE }
  pure val idleWaiter: Waiter = { phase: "idle", deadline: 0, waitTime: 0, oneShot: false,
    claimStart: 0, claimSeq: 0, claimGen: 0, claimSnap: Set(), reason: "none" }
  pure val unwatched: Slot = { state: "Unwatched", armGen: 0, armedAt: 0, firedAt: 0, lost: false,
    txn: "none", txnSeq: 0, txnGen: 0, txnObsGen: 0, txnEpoch: 0, txnAt: 0, txnNextDue: NONE, txnSeen: Set(), afterFire: false }
  pure val absent: Msg = { present: false, due: 0, holder: 0, gen: 0, sentBy: 0, delayed: false }
  pure def initialState(cfg: Config): State = {
    cfg: cfg, now: 0, msgs: MSGS.mapBy(_ => absent), slotGen: 0, seqs: NODES.mapBy(_ => 0),
    obs: NODES.mapBy(_ => emptyObs), probe: NODES.mapBy(_ => idleProbe), slot: NODES.mapBy(_ => unwatched),
    waiters: WAITERS.mapBy(_ => idleWaiter),
    leftDue: NODES.mapBy(_ => Set()), exposedSince: NODES.mapBy(_ => NONE), reads: NODES.mapBy(_ => 0),
    dirty: NODES.mapBy(_ => false), prevDirty: NODES.mapBy(_ => false),
    lastCommit: 0, writes: 0, responses: Set(), outcomes: Set(), violations: Set(),
  }
  /// The rules as written.
  pure val probeAsWritten: Config = {
    design: "probe", gateUsesDrained: false, claimReportsNextDue: false, expireEvidence: false,
    overBoundShared: false, slotLifecycleFix: false, armThenProbe: true, fallback: true,
    lossy: false, crashes: false, underfill: false, skewed: false, trustHint: false, seqOrder: false,
  }
  pure val probeFixed: Config = probeAsWritten.with("gateUsesDrained", true).with("expireEvidence", true)
    .with("overBoundShared", true).with("seqOrder", true)
  pure val n1AsWritten: Config = probeAsWritten.with("design", "n1")
  pure val n1Fixed: Config = probeFixed.with("design", "n1").with("claimReportsNextDue", true).with("slotLifecycleFix", true)

  var s: State

  action init = s' = initialState(probeFixed.with("underfill", true).with("crashes", true).with("skewed", true))
  // Probe-only wait path: rules as written (expected to FAIL W3/W4), the
  // corrected rules (expected to hold), and one corrected rule removed at a time.
  action initProbeAsWritten = s' = initialState(probeAsWritten.with("underfill", true).with("crashes", true).with("skewed", true))
  action initProbeFixed = s' = initialState(probeFixed.with("underfill", true).with("crashes", true).with("skewed", true))
  action initProbeNoDrainedFlag = s' = initialState(probeFixed.with("gateUsesDrained", false).with("underfill", true))
  action initProbeNoEvidenceExpiry = s' = initialState(probeFixed.with("expireEvidence", false))
  action initProbeOverBoundOwnProbe = s' = initialState(probeFixed.with("overBoundShared", false))
  action initProbeTrustHint = s' = initialState(probeFixed.with("trustHint", true).with("overBoundShared", false))
  // N1: as written, corrected with healthy watches (bound R), corrected with
  // lost/spurious watches and lost Relaxed claims (bound F), one rule removed at a time.
  action initN1AsWritten = s' = initialState(n1AsWritten.with("underfill", true))
  action initN1Fixed = s' = initialState(n1Fixed.with("underfill", true).with("skewed", true))
  action initN1FixedLossy = s' = initialState(n1Fixed.with("underfill", true).with("lossy", true).with("crashes", true).with("skewed", true))
  action initN1NoNextDue = s' = initialState(n1Fixed.with("claimReportsNextDue", false))
  action initN1NoDrainedFlag = s' = initialState(n1Fixed.with("gateUsesDrained", false).with("underfill", true))
  action initN1NoLifecycleFix = s' = initialState(n1Fixed.with("slotLifecycleFix", false))
  action initN1WrongOrder = s' = initialState(n1Fixed.with("armThenProbe", false))
  action initN1InstantOrder = s' = initialState(n1Fixed.with("seqOrder", false))
  action initN1NoFallback = s' = initialState(n1Fixed.with("lossy", true).with("fallback", false))

  // ---------------------------------------------------------------- helpers
  pure def minInt(a: int, b: int): int = if (a < b) a else b
  pure def clock(st: State, n: int): int = st.now + (if (st.cfg.skewed and n == 2) DELTA else 0)
  pure def visible(st: State, n: int, m: int): bool = st.msgs.get(m).present and st.msgs.get(m).due <= clock(st, n)
  pure def visibleSet(st: State, n: int): Set[int] = MSGS.filter(m => visible(st, n, m))
  pure def earliestDue(st: State): int = MSGS.fold(NONE, (acc, m) =>
    if (st.msgs.get(m).present) minInt(acc, st.msgs.get(m).due) else acc)
  pure def earliestFutureDue(st: State, after: int): int = MSGS.fold(NONE, (acc, m) =>
    if (st.msgs.get(m).present and st.msgs.get(m).due > after) minInt(acc, st.msgs.get(m).due) else acc)
  pure def waitersOn(st: State, n: int, phases: Set[str]): Set[int] =
    WAITERS.filter(w => nodeOf(w) == n and phases.contains(st.waiters.get(w).phase))
  /// A long poll registers in its node's slot before its first probe; short
  /// polls and over-bound receives never register.
  pure val REGISTERED = Set("decide", "waiting", "probing", "claiming")
  pure def registered(st: State, n: int): int =
    waitersOn(st, n, REGISTERED).filter(w => st.waiters.get(w).waitTime > 0 and not(st.waiters.get(w).oneShot)).size()
  pure def claimInFlight(st: State, n: int): bool = waitersOn(st, n, Set("claiming")).size() > 0
  pure def fresh(st: State, n: int): bool = st.obs.get(n).observedAt + P > clock(st, n)
  pure def evidenceOk(st: State, n: int): bool =
    not(st.cfg.expireEvidence) or st.obs.get(n).evidenceAt + P > clock(st, n)
  pure def dueKnown(st: State, n: int): bool = st.obs.get(n).nextDue <= clock(st, n) or st.obs.get(n).deep
  pure def claimReason(st: State, n: int): bool = dueKnown(st, n) and evidenceOk(st, n)
  /// One claim in flight per queue per node unless the last claim was a full page.
  pure def gateOpen(st: State, n: int): bool = not(claimInFlight(st, n)) or st.obs.get(n).deep
  pure def needsProbe(st: State, n: int): bool =
    (st.cfg.design == "probe" and not(fresh(st, n))) or (dueKnown(st, n) and not(evidenceOk(st, n)))
  pure def wakeTag(source: str): str =
    if (source == "signal") "wake_signal" else if (source == "probe") "wake_probe" else if (source == "watch") "wake_watch"
    else if (source == "arm") "wake_arm" else if (source == "full") "wake_full" else if (source == "backoff") "wake_backoff"
    else "wake_other"
  pure def tag(st: State, t: str): State = st.with("outcomes", st.outcomes.union(Set(t)))
  pure def violate(st: State, v: str): State = st.with("violations", st.violations.union(Set(v)))
  pure def setWaiter(st: State, w: int, wt: Waiter): State = st.with("waiters", st.waiters.set(w, wt))
  pure def setObs(st: State, n: int, o: Obs): State = st.with("obs", st.obs.set(n, o))
  pure def read(st: State, n: int): State = st.with("reads", st.reads.set(n, st.reads.get(n) + 1))
  pure def markDirty(st: State): State = st.with("dirty", NODES.mapBy(_ => true)).with("lastCommit", st.now)
  /// A message that changes hands or state is no longer "left due" by an earlier claim.
  pure def touch(st: State, m: int): State = st.with("leftDue", NODES.mapBy(n => st.leftDue.get(n).exclude(Set(m))))
  pure def clearLeft(st: State, n: int): State = st.with("leftDue", st.leftDue.set(n, Set()))

  /// QueueWaitState::record: a probe or claim that started at `started` at
  /// local generation `gen`. A newer observation wins; a local commit during
  /// the work keeps its earlier due time.
  pure def record(st: State, n: int, gen: int, started: int, startSeq: int, nextDue: int, deep: bool, source: str): State = {
    val o = st.obs.get(n)
    // As written: compare start instants (`observed_at > started`), so two
    // observations started in the same instant overwrite in finish order.
    // Corrected: compare a per-node sequence number taken at start.
    // An older observation may still lower the next due time (it costs at
    // most one claim); it never raises it.
    val older = if (st.cfg.seqOrder) o.seq > startSeq else o.observedAt > started
    if (older and st.cfg.seqOrder and nextDue < o.nextDue)
      setObs(st, n, o.with("nextDue", nextDue).with("evidenceAt", started).with("source", source))
    else if (older) st
    else {
      val due = if (o.gen == gen) nextDue else minInt(o.nextDue, nextDue)
      setObs(st, n, o.with("observedAt", started).with("seq", startSeq).with("nextDue", due).with("evidenceAt", started)
        .with("deep", deep).with("source", if (o.gen == gen) source else "signal"))
    }
  }
  pure def nextSeq(st: State, n: int): State = st.with("seqs", st.seqs.set(n, st.seqs.get(n) + 1))
  /// A local commit that can make a message due (send, visibility change).
  pure def signal(st: State, n: int, due: int): State = {
    val o = st.obs.get(n)
    setObs(st, n, o.with("gen", o.gen + 1).with("nextDue", minInt(o.nextDue, due))
      .with("evidenceAt", clock(st, n)).with("source", "signal"))
  }
  /// forget on purge/delete/attributes: a new epoch discards in-flight probes.
  pure def forget(st: State, n: int): State = {
    val o = st.obs.get(n)
    setObs(st, n, emptyObs.with("gen", o.gen + 1).with("epoch", o.epoch + 1).with("seq", o.seq))
  }
  pure def startProbe(st: State, n: int): State =
    if (st.probe.get(n).active) st
    else nextSeq(read(st, n), n).with("probe", st.probe.set(n, { active: true, startedAt: clock(st, n), seq: st.seqs.get(n) + 1,
      gen: st.obs.get(n).gen, epoch: st.obs.get(n).epoch, nextDue: earliestDue(st) }))

  // ------------------------------------------------------------- claims
  pure def startClaim(st: State, w: int, oneShot: bool): State = {
    val n = nodeOf(w)
    val wt = st.waiters.get(w)
    val snap = MSGS.filter(m => st.msgs.get(m).present).map(m => (m, st.msgs.get(m).due, st.msgs.get(m).holder))
    val idle = MSGS.forall(m => not(st.msgs.get(m).present)) and st.now >= st.lastCommit + P + 1
    val parallel = claimInFlight(st, n)
    val started = nextSeq(setWaiter(st, w, wt.with("phase", "claiming").with("oneShot", oneShot)
      .with("claimStart", clock(st, n)).with("claimSeq", st.seqs.get(n) + 1).with("claimGen", st.obs.get(n).gen).with("claimSnap", snap)
      .with("reason", st.obs.get(n).source)), n).with("writes", st.writes + 1)
    val checked = if (idle) violate(started, "W4a") else started
    if (parallel) tag(checked, "deep_parallel") else checked
  }
  pure def claimAvailable(st: State, w: int): List[int] =
    MSG_LIST.select(m => st.msgs.get(m).present and st.msgs.get(m).due <= st.waiters.get(w).claimStart)
  pure def claimConflicted(st: State, w: int): bool = st.waiters.get(w).claimSnap.exists(e =>
    not(st.msgs.get(e._1).present) or st.msgs.get(e._1).due != e._2 or st.msgs.get(e._1).holder != e._3)
  pure def canClaimCommit(st: State, w: int, mode: str): bool = {
    val wt = st.waiters.get(w)
    all {
      wt.phase == "claiming",
      mode == "normal"
        or (mode == "contention" and claimConflicted(st, w))
        or (mode == "underfill" and st.cfg.underfill and claimAvailable(st, w).length() > 0),
    }
  }
  pure def claimCommit(st: State, w: int, mode: str): State = {
    val n = nodeOf(w)
    val wt = st.waiters.get(w)
    val avail = claimAvailable(st, w)
    val count = if (mode == "normal") minInt(LIMIT, avail.length())
      else if (mode == "underfill") minInt(LIMIT, avail.length()) - 1 else 0
    val takeList = avail.slice(0, count)
    val take = takeList.foldl(Set(), (acc, m) => acc.union(Set(m)))
    val leftover = avail.foldl(Set(), (acc, m) => acc.union(Set(m))).exclude(take)
    val msgs = take.fold(st.msgs, (acc, m) => acc.set(m, acc.get(m).with("holder", w).with("due", clock(st, n) + V)))
    val committed = take.fold(if (take.size() > 0) markDirty(st) else st, (acc, m) => touch(acc, m)).with("msgs", msgs)
    val full = take.size() == LIMIT
    val drained = mode == "normal"
    val reported = if (st.cfg.claimReportsNextDue) earliestFutureDue(committed, wt.claimStart) else NONE
    val recorded =
      if (full) record(committed, n, wt.claimGen, wt.claimStart, wt.claimSeq, wt.claimStart, true, "full")
      else if (st.cfg.gateUsesDrained and not(drained))
        record(committed, n, wt.claimGen, wt.claimStart, wt.claimSeq, clock(st, n) + BACKOFF, false, "backoff")
      else record(committed, n, wt.claimGen, wt.claimStart, wt.claimSeq, reported, false, "claim")
    val others = waitersOn(st, n, Set("waiting")).size() > 0
    val tagged0 = if (take.size() == 1 and not(full) and others) tag(recorded, "herd_single") else recorded
    val tagged1 = if (take.size() > 0) tag(tagged0, wakeTag(wt.reason)) else tagged0
    val tagged2 = if (take.exists(m => st.msgs.get(m).sentBy != n) and wt.reason == "probe") tag(tagged1, "wake_probe_other_node") else tagged1
    val tagged3 = if (take.exists(m => st.msgs.get(m).delayed) and wt.reason == "probe" and st.obs.get(n).evidenceAt < st.obs.get(n).nextDue)
      tag(tagged2, "wake_delayed_timer") else tagged2
    val tagged = if (mode != "normal" and leftover.size() > 0) tag(tagged3, "claim_left_due") else tagged3
    val withLeft = tagged.with("leftDue", tagged.leftDue.set(n, if (full) Set() else leftover))
    val responded = withLeft.with("responses", withLeft.responses.union(take.map(m => (w, m, true))))
    val nextPhase = if (take.size() > 0 or wt.oneShot or st.now >= wt.deadline) "done" else "waiting"
    leave(setWaiter(responded, w, wt.with("phase", nextPhase)), w)
  }

  /// Exit path: in the design as written, the slot reacts to the event
  /// "last local waiter left" only while it is Armed.
  pure def leave(st: State, w: int): State = {
    val n = nodeOf(w)
    val sl = st.slot.get(n)
    if (st.waiters.get(w).phase != "done" or st.cfg.design != "n1" or st.cfg.slotLifecycleFix) st
    else if (registered(st, n) == 0 and sl.state == "Armed" and sl.txn == "none")
      st.with("slot", st.slot.set(n, unwatched))
    else st
  }
  pure def finish(st: State, w: int, t: str): State =
    leave(tag(setWaiter(st, w, st.waiters.get(w).with("phase", "done")), t), w)

  // ------------------------------------------------------------- receives
  pure def canArrive(st: State, w: int): bool = Set("idle", "done").contains(st.waiters.get(w).phase)
  pure def arrive(st: State, w: int, waitTime: int): State =
    setWaiter(st, w, idleWaiter.with("phase", "decide").with("waitTime", waitTime).with("deadline", st.now + waitTime))

  pure def overBound(st: State, w: int): bool =
    waitersOn(st, nodeOf(w), Set("waiting", "probing", "claiming")).exclude(Set(w)).size() >= BOUND

  pure def waiterUrgent(st: State, w: int): bool = {
    val n = nodeOf(w)
    val wt = st.waiters.get(w)
    if (wt.phase == "decide") true
    else if (wt.phase == "waiting")
      st.now >= wt.deadline or (claimReason(st, n) and gateOpen(st, n)) or (needsProbe(st, n) and not(st.probe.get(n).active))
    else if (wt.phase == "probing" or wt.phase == "ob_wait") not(st.probe.get(n).active)
    else false
  }

  pure def decide(st: State, w: int): State = {
    val n = nodeOf(w)
    val wt = st.waiters.get(w)
    if (overBound(st, w)) {
      if (st.cfg.overBoundShared) {
        if (fresh(st, n) and evidenceOk(st, n))
          (if (claimReason(st, n)) startClaim(st, w, true) else finish(st, w, "over_bound_empty"))
        else startProbe(setWaiter(st, w, wt.with("phase", "ob_wait")), n)
      } else {
        // As written: the over-bound receive runs its own probe.
        val probed = read(st, n)
        if (earliestDue(st) <= clock(st, n)) {
          if (st.cfg.trustHint) {
            val m = MSGS.filter(x => st.msgs.get(x).present and st.msgs.get(x).due <= clock(st, n))
              .fold(0, (acc, x) => if (acc == 0 or x < acc) x else acc)
            violate(finish(probed.with("responses", probed.responses.union(Set((w, m, false)))), w, "hint_answered"), "W1")
          } else startClaim(probed, w, true)
        } else finish(probed, w, "over_bound_empty")
      }
    } else if (claimReason(st, n) and gateOpen(st, n)) startClaim(st, w, wt.waitTime == 0)
    else if (claimReason(st, n)) setWaiter(st, w, wt.with("phase", "waiting"))
    else if (wt.waitTime == 0) {
      if (fresh(st, n) and evidenceOk(st, n)) finish(st, w, "short_poll_empty")
      else startProbe(setWaiter(st, w, wt.with("phase", "probing")), n)
    } else {
      val waiting = setWaiter(st, w, wt.with("phase", "waiting"))
      if (fresh(st, n)) waiting else startProbe(waiting, n)
    }
  }

  pure def waiterStep(st: State, w: int): State = {
    val n = nodeOf(w)
    val wt = st.waiters.get(w)
    if (wt.phase == "decide") decide(st, w)
    else if (wt.phase == "waiting") {
      if (st.now >= wt.deadline) finish(st, w, "deadline_empty")
      else if (claimReason(st, n) and gateOpen(st, n)) startClaim(st, w, false)
      else startProbe(st, n)
    } else {
      // "probing" (short poll) or "ob_wait" after the shared probe finished.
      if (not(fresh(st, n))) startProbe(st, n)
      else if (claimReason(st, n) and (gateOpen(st, n) or wt.phase == "ob_wait")) startClaim(st, w, true)
      else finish(st, w, "probe_empty")
    }
  }

  pure def probeEnd(st: State, n: int): State = {
    val p = st.probe.get(n)
    val ended = st.with("probe", st.probe.set(n, idleProbe))
    if (p.epoch != st.obs.get(n).epoch) tag(ended, "forgotten_probe_discarded")
    else record(clearLeft(ended, n), n, p.gen, p.startedAt, p.seq, p.nextDue, false, "probe")
  }

  // ----------------------------------------------------------- N1 slot
  pure def slotUrgent(st: State, n: int): bool = {
    val sl = st.slot.get(n)
    val waiting = registered(st, n) > 0
    if (st.cfg.design != "n1") false
    else if (sl.txn != "none") true
    else if (sl.state == "Unwatched") waiting
    else if (sl.state == "Fired") st.now >= sl.firedAt + R
    else if (sl.state == "Armed")
      (not(sl.lost) and sl.armGen != st.slotGen)
        or (st.cfg.fallback and st.now >= sl.armedAt + F)
    else false
  }
  pure def beginArming(st: State, n: int, afterFire: bool): State = {
    val sl = st.slot.get(n)
    val noWaiters = registered(st, n) == 0
    val armed = nextSeq(read(st, n), n).with("slot", st.slot.set(n, sl.with("state", "Arming").with("txnSeq", st.seqs.get(n) + 1)
      .with("txn", if (st.cfg.armThenProbe) "arming" else "probed")
      .with("txnGen", st.slotGen).with("txnObsGen", st.obs.get(n).gen).with("txnEpoch", st.obs.get(n).epoch)
      .with("txnAt", clock(st, n)).with("txnNextDue", earliestDue(st))
      .with("txnSeen", MSGS.filter(m => st.msgs.get(m).present)).with("afterFire", afterFire)))
    if (noWaiters) violate(armed, "W4c") else armed
  }
  pure def slotStep(st: State, n: int): State = {
    val sl = st.slot.get(n)
    val waiting = registered(st, n) > 0
    if (sl.txn != "none") {
      // Commit of the arming transaction, or the separate watch transaction
      // of the wrong order (it reads the generation now).
      val gen = if (sl.txn == "arming") sl.txnGen else st.slotGen
      val readsMore = if (sl.txn == "probed") read(st, n) else st
      val missed = MSGS.exists(m => st.msgs.get(m).present and st.msgs.get(m).holder == 0
        and st.msgs.get(m).gen > 0 and st.msgs.get(m).gen <= gen and not(sl.txnSeen.contains(m)))
      val armed = readsMore.with("slot", readsMore.slot.set(n, sl.with("state", "Armed").with("txn", "none")
        .with("armGen", gen).with("armedAt", st.now).with("lost", false)))
      val recorded = if (sl.txnEpoch != st.obs.get(n).epoch) armed
        else record(clearLeft(armed, n), n, sl.txnObsGen, sl.txnAt, sl.txnSeq, sl.txnNextDue, false, if (sl.afterFire) "watch" else "arm")
      val checked = if (missed) violate(tag(recorded, "lost_wake"), "W5") else recorded
      if (not(st.cfg.slotLifecycleFix) and not(waiting)) tag(checked, "armed_without_waiters") else checked
    } else if (sl.state == "Unwatched") beginArming(st, n, false)
    else if (sl.state == "Fired") {
      if (waiting) beginArming(st, n, true) else st.with("slot", st.slot.set(n, unwatched))
    } else if (not(sl.lost) and sl.armGen != st.slotGen) {
      st.with("slot", st.slot.set(n, sl.with("state", "Fired").with("firedAt", st.now)))
    } else {
      // Fallback interval elapsed.
      if (waiting) tag(beginArming(st, n, sl.lost), if (sl.lost) "fallback_after_lost_watch" else "fallback")
      else if (st.cfg.slotLifecycleFix) st.with("slot", st.slot.set(n, unwatched))
      else beginArming(st, n, false)
    }
  }

  // ------------------------------------------------- other clients (adversary)
  pure def newGen(st: State): int = st.slotGen + 1
  pure def canSend(st: State, m: int): bool = not(st.msgs.get(m).present)
  pure def send(st: State, n: int, m: int, delay: int): State = {
    val due = clock(st, n) + delay
    val committed = markDirty(st).with("slotGen", newGen(st))
      .with("msgs", st.msgs.set(m, { present: true, due: due, holder: 0, gen: newGen(st), sentBy: n, delayed: delay > 0 }))
    signal(committed, n, due)
  }
  pure def canExtClaim(st: State, n: int, m: int): bool = visible(st, n, m)
  pure def extClaim(st: State, n: int, m: int): State =
    touch(markDirty(st), m).with("msgs", st.msgs.set(m, st.msgs.get(m).with("holder", -1).with("due", clock(st, n) + V)))
  pure def canDeleteMsg(st: State, m: int): bool = st.msgs.get(m).present and st.msgs.get(m).holder != 0
  pure def deleteMsg(st: State, m: int): State = touch(markDirty(st), m).with("msgs", st.msgs.set(m, absent))
  pure def canVisZero(st: State, n: int, m: int): bool =
    st.msgs.get(m).present and st.msgs.get(m).holder != 0 and st.msgs.get(m).due > clock(st, n)
  pure def visZero(st: State, n: int, m: int): State = {
    val committed = touch(markDirty(st), m).with("slotGen", newGen(st))
      .with("msgs", st.msgs.set(m, st.msgs.get(m).with("due", clock(st, n)).with("gen", newGen(st))))
    signal(committed, n, clock(st, n))
  }
  pure def canCrashUndo(st: State, m: int): bool = st.cfg.crashes and st.msgs.get(m).present and st.msgs.get(m).holder != 0
  /// A lost Relaxed claim: visible again with no commit signal and no slot advance.
  pure def crashUndo(st: State, m: int): State =
    touch(markDirty(st), m).with("msgs", st.msgs.set(m, st.msgs.get(m).with("holder", 0).with("due", st.now)))
  pure def purge(st: State, n: int): State =
    forget(markDirty(st).with("slotGen", newGen(st)).with("msgs", MSGS.mapBy(_ => absent)).with("leftDue", NODES.mapBy(_ => Set())), n)

  // ------------------------------------------------------------- time
  pure def urgent(st: State): bool =
    WAITERS.exists(w => waiterUrgent(st, w) or st.waiters.get(w).phase == "claiming")
      or NODES.exists(n => st.probe.get(n).active or slotUrgent(st, n))
  pure def tick(st: State): State = {
    val t = st.now + 1
    val boundary = t % P == 0
    st.with("now", t)
      .with("reads", if (boundary) NODES.mapBy(_ => 0) else st.reads)
      .with("prevDirty", if (boundary) st.dirty else st.prevDirty)
      .with("dirty", if (boundary) NODES.mapBy(_ => false) else st.dirty)
  }

  /// Ghost bookkeeping after each step: exposure start times.
  pure def exposed(st: State, n: int): bool = all {
    visibleSet(st, n).size() > 0,
    WAITERS.exists(w => nodeOf(w) == n and st.waiters.get(w).phase == "waiting" and st.waiters.get(w).deadline > st.now),
    not(claimInFlight(st, n)),
  }
  pure def settle(st: State): State = st.with("exposedSince", NODES.mapBy(n =>
    if (exposed(st, n)) (if (st.exposedSince.get(n) == NONE) st.now else st.exposedSince.get(n)) else NONE))

  // ---------------------------------------------------------------- actions
  action Arrive = {
    nondet waiter = WAITERS.oneOf()
    nondet waitTime = Set(0, WAIT).oneOf()
    all { canArrive(s, waiter), s.now + waitTime <= HORIZON, s' = settle(arrive(s, waiter, waitTime)) }
  }
  action WaiterStep = {
    nondet waiter = WAITERS.oneOf()
    all { waiterUrgent(s, waiter), s' = settle(waiterStep(s, waiter)) }
  }
  action ClaimCommit = {
    nondet waiter = WAITERS.oneOf()
    nondet mode = Set("normal", "contention", "underfill").oneOf()
    all { canClaimCommit(s, waiter, mode), s' = settle(claimCommit(s, waiter, mode)) }
  }
  action ProbeEnd = {
    nondet node = NODES.oneOf()
    all { s.probe.get(node).active, s' = settle(probeEnd(s, node)) }
  }
  action SlotStep = {
    nondet node = NODES.oneOf()
    all { slotUrgent(s, node), s' = settle(slotStep(s, node)) }
  }
  action LoseWatch = {
    nondet node = NODES.oneOf()
    all { s.cfg.lossy, s.slot.get(node).state == "Armed", not(s.slot.get(node).lost),
      s' = settle(tag(s.with("slot", s.slot.set(node, s.slot.get(node).with("lost", true)))
        .with("dirty", s.dirty.set(node, true)), "watch_lost")) }
  }
  action SpuriousFire = {
    nondet node = NODES.oneOf()
    all { s.cfg.lossy, s.slot.get(node).state == "Armed",
      s' = settle(s.with("slot", s.slot.set(node, s.slot.get(node).with("state", "Fired").with("firedAt", s.now)))
        .with("dirty", s.dirty.set(node, true))) }
  }
  action Send = {
    nondet node = NODES.oneOf()
    nondet message = MSGS.oneOf()
    nondet delay = Set(0, 2).oneOf()
    all { canSend(s, message), s' = settle(send(s, node, message, delay)) }
  }
  action ExtClaim = {
    nondet node = NODES.oneOf()
    nondet message = MSGS.oneOf()
    all { canExtClaim(s, node, message), s' = settle(extClaim(s, node, message)) }
  }
  action DeleteMsg = {
    nondet message = MSGS.oneOf()
    all { canDeleteMsg(s, message), s' = settle(deleteMsg(s, message)) }
  }
  action VisZero = {
    nondet node = NODES.oneOf()
    nondet message = MSGS.oneOf()
    all { canVisZero(s, node, message), s' = settle(visZero(s, node, message)) }
  }
  action CrashUndo = {
    nondet message = MSGS.oneOf()
    all { canCrashUndo(s, message), s' = settle(crashUndo(s, message)) }
  }
  action Purge = {
    nondet node = NODES.oneOf()
    all { MSGS.exists(m => s.msgs.get(m).present), s' = settle(purge(s, node)) }
  }
  action Tick = all { s.now < HORIZON, not(urgent(s)), s' = settle(tick(s)) }

  /// Node reactions are listed more often than the adversary so that wake
  /// paths are sampled; Tick waits for urgent work anyway.
  action step = any {
    WaiterStep, WaiterStep, ClaimCommit, ClaimCommit, ProbeEnd, ProbeEnd, SlotStep, SlotStep,
    Arrive, Arrive, Send, ExtClaim, DeleteMsg, VisZero, CrashUndo, Purge, LoseWatch, SpuriousFire,
    Tick, Tick, Tick, Tick, Tick, Tick, Tick, Tick,
  }

  // ------------------------------------------------------------- invariants
  /// W1: every response entry was claimed by a committed claim of that receive.
  val w1HintsNotAuthority = s.responses.forall(r => r._3) and not(s.violations.contains("W1"))
  /// W2 (safety): a waiter with time left on a node where a message is visible
  /// always has a pending wake source.
  pure def wakeSource(st: State, n: int): bool = {
    val sl = st.slot.get(n)
    val o = st.obs.get(n)
    claimInFlight(st, n) or st.probe.get(n).active or o.nextDue < NONE or o.deep
      or WAITERS.exists(w => nodeOf(w) == n and waiterUrgent(st, w)) or slotUrgent(st, n)
      or (st.cfg.design == "probe" and o.observedAt > NEVER)
      or (st.cfg.design == "n1" and (sl.txn != "none" or sl.state == "Fired"
        or (sl.state == "Armed" and ((not(sl.lost) and sl.armGen != st.slotGen) or st.cfg.fallback))))
  }
  val w2NoStuckWaiter = NODES.forall(n => exposed(s, n) implies wakeSource(s, n))
  /// W2 (bounded response): a visible message and a waiting receive on a node
  /// with no claim in flight persist for at most the design bound.
  pure def responseBound(cfg: Config): int =
    if (cfg.design == "probe") P else if (cfg.lossy or cfg.crashes) F else R
  val w2BoundedResponse = NODES.forall(n =>
    s.exposedSince.get(n) == NONE or s.now - s.exposedSince.get(n) <= responseBound(s.cfg))
  /// W3: a claim that left due entries never lets the node's waiters sleep
  /// with no claim in flight beyond the short backoff.
  val w3GateNoStarvation = NODES.forall(n => {
    val left = s.leftDue.get(n).filter(m => visible(s, n, m))
    (left.size() > 0 and exposed(s, n)) implies s.obs.get(n).nextDue <= clock(s, n) + BACKOFF
  })
  /// W4: (a) no write transaction once the queue is empty and nothing has
  /// committed for longer than P; (b) per node, read transactions in a quiet
  /// aligned window of length P are at most 1 (probe) or 2 (N1: one arming,
  /// one initial probe), whatever the number of waiters; (c) no slot read on a
  /// node without registered waiters.
  pure def readBound(cfg: Config): int = if (cfg.design == "probe") 1 else 2
  val w4aNoIdleWrites = not(s.violations.contains("W4a"))
  val w4bReadsBounded = NODES.forall(n => s.dirty.get(n) or s.prevDirty.get(n) or s.reads.get(n) <= readBound(s.cfg))
  val w4cNoOrphanArming = not(s.violations.contains("W4c"))
  val w4Cost = all { w4aNoIdleWrites, w4bReadsBounded, w4cNoOrphanArming }
  /// W5: an arming leaves no unheld message that its probe did not see and
  /// whose send cannot fire the watch.
  val w5ArmThenProbe = not(s.violations.contains("W5"))
  val inv = all { w1HintsNotAuthority, w2NoStuckWaiter, w2BoundedResponse, w3GateNoStarvation, w4Cost, w5ArmThenProbe }

  // -------------------------------------------------------------- witnesses
  val wake_local_signal_witness = s.outcomes.contains("wake_signal")
  val wake_probe_other_node_witness = s.outcomes.contains("wake_probe_other_node")
  val wake_slot_watch_witness = s.outcomes.contains("wake_watch")
  val wake_delayed_timer_witness = s.outcomes.contains("wake_delayed_timer")
  val lost_watch_fallback_witness = s.outcomes.contains("fallback_after_lost_watch")
  val herd_single_claim_witness = s.outcomes.contains("herd_single")
  val deep_parallel_witness = s.outcomes.contains("deep_parallel")
  val claim_left_due_witness = s.outcomes.contains("claim_left_due")
  val forgotten_probe_witness = s.outcomes.contains("forgotten_probe_discarded")

  // ------------------------------------------------------- deterministic runs
  pure def drain(st: State, k: int): State = 1.to(k).fold(st, (acc, _) => {
    val w = WAITERS.filter(x => waiterUrgent(acc, x)).fold(0, (a, x) => if (a == 0) x else a)
    val c = WAITERS.filter(x => canClaimCommit(acc, x, "normal")).fold(0, (a, x) => if (a == 0) x else a)
    val p = NODES.filter(n => acc.probe.get(n).active).fold(0, (a, x) => if (a == 0) x else a)
    val sl = NODES.filter(n => slotUrgent(acc, n)).fold(0, (a, x) => if (a == 0) x else a)
    settle(if (w > 0) waiterStep(acc, w) else if (c > 0) claimCommit(acc, c, "normal")
      else if (p > 0) probeEnd(acc, p) else if (sl > 0) slotStep(acc, sl) else acc)
  })
  pure def ticks(st: State, k: int): State = 1.to(k).fold(st, (acc, _) => drain(settle(tick(drain(acc, 12))), 12))

  run localSignalWakesWaiterTest = {
    val waiting = drain(arrive(initialState(probeFixed), 1, WAIT), 12)
    val woken = drain(send(waiting, 1, 1, 0), 12)
    all { assert(waiting.waiters.get(1).phase == "waiting"), assert(woken.responses == Set((1, 1, true))),
      assert(woken.outcomes.contains("wake_signal")) }
  }
  run otherNodeWakesByProbeTest = {
    val waiting = ticks(drain(arrive(initialState(probeFixed), 1, WAIT), 12), 1)
    val sent = send(waiting, 2, 1, 0)
    val woken = ticks(sent, P)
    all { assert(sent.responses == Set()), assert(woken.responses == Set((1, 1, true))),
      assert(woken.outcomes.contains("wake_probe_other_node")) }
  }
  run slotWatchWakesOtherNodeTest = {
    val waiting = drain(arrive(initialState(n1Fixed), 1, WAIT), 12)
    val fired = drain(send(ticks(waiting, 1), 2, 1, 0), 12)
    val woken = ticks(fired, R)
    all { assert(waiting.slot.get(1).state == "Armed"), assert(fired.slot.get(1).state == "Fired"),
      assert(woken.responses == Set((1, 1, true))), assert(woken.outcomes.contains("wake_watch")) }
  }
  run herdOfTwoClaimsOneMessageOnceTest = {
    val two = drain(arrive(drain(arrive(initialState(probeFixed), 1, WAIT), 12), 2, WAIT), 12)
    val woken = drain(send(two, 1, 1, 0), 12)
    all { assert(woken.responses.size() == 1), assert(woken.writes == 1), assert(woken.outcomes.contains("herd_single")),
      assert(Set(1, 2).filter(w => woken.waiters.get(w).phase == "waiting").size() == 1) }
  }
  run wrongOrderLosesWakeUntilFallbackTest = {
    val st0 = initialState(n1Fixed.with("armThenProbe", false))
    val arrived = waiterStep(arrive(st0, 1, WAIT), 1)
    val probedOnly = slotStep(probeEnd(arrived, 1), 1)
    val sent = send(probedOnly, 2, 1, 0)
    val armed = slotStep(sent, 1)
    all { assert(probedOnly.slot.get(1).txn == "probed"), assert(armed.violations.contains("W5")),
      assert(armed.slot.get(1).armGen == armed.slotGen) }
  }
  run rightOrderSeesEveryInterleavingTest = {
    val st0 = initialState(n1Fixed)
    val arrived = probeEnd(waiterStep(arrive(st0, 1, WAIT), 1), 1)
    val before = slotStep(slotStep(send(arrived, 2, 1, 0), 1), 1)
    val after = slotStep(send(slotStep(arrived, 1), 2, 1, 0), 1)
    all { assert(not(before.violations.contains("W5"))), assert(before.obs.get(1).nextDue <= 0),
      assert(not(after.violations.contains("W5"))), assert(after.slot.get(1).armGen != after.slotGen) }
  }
  run instantOrderLetsOlderProbeEraseArmingResultTest = {
    pure def scenario(cfg: Config): State = {
      val probing = waiterStep(arrive(initialState(cfg), 1, WAIT), 1)
      val sent = send(probing, 2, 1, 0)
      val armed = slotStep(slotStep(sent, 1), 1)
      probeEnd(armed, 1)
    }
    val asWritten = scenario(n1Fixed.with("seqOrder", false))
    val fixed = scenario(n1Fixed)
    all {
      assert(asWritten.obs.get(1).nextDue == NONE), assert(asWritten.slot.get(1).armGen == asWritten.slotGen),
      assert(exposed(asWritten, 1)), assert(not(waiterUrgent(asWritten, 1))), assert(not(slotUrgent(asWritten, 1))),
      assert(fixed.obs.get(1).nextDue == 0), assert(waiterUrgent(fixed, 1)),
    }
  }
  run claimMustReportLeaseEndUnderN1Test = {
    pure def scenario(cfg: Config): State = {
      val armed = probeEnd(slotStep(slotStep(waiterStep(arrive(initialState(cfg), 1, WAIT), 1), 1), 1), 1)
      val fired = slotStep(send(armed, 2, 1, 0), 1)
      val rearmed = slotStep(slotStep(tick(fired), 1), 1)
      val claiming = extClaim(waiterStep(rearmed, 1), 2, 1)
      claimCommit(claiming, 1, "normal")
    }
    val asWritten = scenario(n1Fixed.with("claimReportsNextDue", false))
    val fixed = scenario(n1Fixed)
    all {
      assert(asWritten.waiters.get(1).phase == "waiting"), assert(asWritten.obs.get(1).nextDue == NONE),
      assert(asWritten.slot.get(1).state == "Armed"), assert(asWritten.slot.get(1).armGen == asWritten.slotGen),
      assert(fixed.obs.get(1).nextDue == 1 + V),
    }
  }
  run partialPageIsNotDrainedTest = {
    pure def scenario(cfg: Config): State = {
      val waiting = drain(arrive(drain(arrive(initialState(cfg), 1, WAIT), 12), 2, WAIT), 12)
      val claiming = waiterStep(send(send(waiting, 1, 1, 0), 1, 2, 0), 1)
      claimCommit(claiming, 1, "underfill")
    }
    val asWritten = scenario(probeAsWritten.with("underfill", true))
    val fixed = scenario(probeFixed.with("underfill", true))
    all {
      assert(asWritten.responses.size() == 1), assert(exposed(asWritten, 1)), assert(asWritten.obs.get(1).nextDue == NONE),
      assert(not(waiterUrgent(asWritten, 2))),
      assert(fixed.obs.get(1).nextDue == BACKOFF), assert(waiterUrgent(tick(fixed), 2)),
    }
  }
  run staleSignalCausesIdleWriteTest = {
    pure def scenario(cfg: Config): State = {
      val sent = deleteMsg(extClaim(send(initialState(cfg), 1, 1, 0), 2, 1), 1)
      val quiet = 1.to(P + 1).fold(sent, (acc, _) => tick(acc))
      waiterStep(arrive(quiet, 1, 0), 1)
    }
    all {
      assert(scenario(probeFixed.with("expireEvidence", false)).violations.contains("W4a")),
      assert(scenario(probeFixed).violations == Set()), assert(scenario(probeFixed).writes == 0),
    }
  }
  run lastWaiterLeavingDuringArmingLeaksWatchTest = {
    pure def scenario(cfg: Config): State = {
      val arming = slotStep(waiterStep(arrive(initialState(cfg), 1, WAIT), 1), 1)
      val left = finish(probeEnd(arming, 1), 1, "client_disconnect")
      val armed = slotStep(left, 1)
      slotStep(1.to(F).fold(armed, (acc, _) => tick(acc)), 1)
    }
    val asWritten = scenario(n1Fixed.with("slotLifecycleFix", false))
    val fixed = scenario(n1Fixed)
    all {
      assert(asWritten.violations.contains("W4c")), assert(asWritten.outcomes.contains("armed_without_waiters")),
      assert(fixed.violations == Set()), assert(fixed.slot.get(1).state == "Unwatched"),
    }
  }
}
