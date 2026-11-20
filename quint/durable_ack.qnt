/// Design-level protocol model. This checks abstract transitions and assumptions,
/// not production implementation, provider atomicity, or crash durability.
///
/// When may a response be sent on a store whose native commit is not yet on
/// disk (SQLite `synchronous=NORMAL`, RocksDB WAL without per-write sync)?
///
/// Physical assumptions (owned by the provider, not checked here):
/// - A native commit appends to one commit order (`log`) and is visible to
///   every later observation at once.
/// - A barrier (WAL sync) that starts when the order has length `n` makes the
///   prefix `log[0..n)` durable when it completes. Nothing else is durable.
/// - A power crash truncates the order to its durable prefix. A process
///   restart without power loss keeps every native commit, durable or not,
///   and loses only volatile process state.
///
/// - A failed sync may lose dirty pages. After a failed barrier, a later
///   barrier that completes proves nothing (`poisoned`) until the process
///   restarts.
///
/// Operations, one attempt each, at most four:
/// - `write`   Durable class. Observe, first write (stage), native commit, settle.
/// - `replay`  Durable class. Read-write attempt that writes nothing and answers
///             from what it observed (an idempotency receipt of another attempt).
/// - `read`    Durable class (owner decision D11). Read-only attempt; settles
///             like `replay` when `readsWait`, otherwise answers at once.
/// - `relaxed` Relaxed class (P4). Writes, commits, answers right after commit.
///
/// Policies (`CFG.policy`):
/// - P0 every read-write attempt waits for a barrier that starts after it ends.
/// - P1 an attempt that wrote nothing never waits (known flawed change).
/// - P2 counter of writers that are not yet durable; a non-writing attempt
///      waits for a later barrier iff the counter is non-zero at completion.
/// - P3 P2 reader rule plus release by commit sequence (design item A1).
module durable_ack {
  type Config = {
    policy: str,
    // P3 only. "atCommit": the writer thread publishes a sequence in the step
    // that completes the native commit, before `wait`. "afterCommit": the
    // sequence is published when the attempt calls `wait`. "reserved": a
    // sequence is taken at the first write, before the commit.
    seqMode: str,
    // P3 only. "nextBarrier": a waiting non-writer needs a barrier that starts
    // after its completion. "publishedSeq": it needs the highest published
    // sequence at its completion.
    readerTarget: str,
    // P4. Enables `relaxed` operations.
    relaxedOps: bool,
    // P4 fix. A relaxed commit keeps one counter registration until a
    // completed barrier covers it (no client waits for it).
    relaxedHolds: bool,
    // Restart fix. A new process holds one registration until its first
    // barrier completes.
    startupHold: bool,
    // Enables process restarts that keep unsynced native commits.
    restarts: bool,
    // Enables commits by another process on the same file.
    external: bool,
    // Multi-process fix. A non-writer also waits when the native commit count
    // read at completion is above the capture of the last completed barrier.
    nativeClause: bool,
    // Writers hold the single writer slot from observation to commit.
    serialWriters: bool,
    // A plain error after the native commit settles like an unknown outcome.
    postCommitErrorWaits: bool,
    // "firstWrite": a writer registers before its first write can commit.
    // "wait": it registers only when it starts to wait, after native commit.
    registerAt: str,
    // "completion": a non-writer reads the counter after its last read.
    // "start": it reads the counter before its first read.
    readerCheckAt: str,
    // Enables one barrier that fails (the sync returns an error).
    barrierFails: bool,
    // Failed-barrier fix. Each writer that the failed barrier answers keeps a
    // hold for its commit until a later barrier covers it.
    failedBarrierHolds: bool,
    // A failed barrier stops the store: every waiter and every later
    // settlement fails, no barrier starts, and no attempt starts until restart.
    stopOnBarrierFail: bool,
    // D11. A read-only attempt settles like an attempt that wrote nothing.
    readsWait: bool,
    // The implemented type in detail (P3 "atCommit" only): a commit is visible
    // before the writer thread records its sequence (`Record`), a guard can be
    // dropped without settlement (`Abort`), the batch can close, non-writers
    // can end with a data-dependent error, a per-connection memo, and version
    // probes that fail. Holds are kept by sequence or by barrier index.
    extended: bool,
    // DUR-F1 fix: a data-dependent error (a failed condition with its item)
    // settles like an answer.
    dataErrorSettles: bool,
    // DUR-F2 fix: a registration ends only when the attempt and the writer
    // thread are both done, and then leaves a hold for an uncovered sequence
    // or, without a sequence, for the next barrier to start.
    abortHolds: bool,
    // DUR-F9: "off"; "safe" a non-writer waits for the highest sequence when
    // every registration has a sequence and no barrier hold exists; "unsafe"
    // ignores registrations without a sequence.
    fastReads: str,
    // A per-connection memo of a version known durable skips the probe.
    memo: bool,
    // A version probe (of a reader or of a barrier) can fail.
    probeFails: bool,
  }
  const CFG: Config

  type Op = {
    kind: str, phase: str, epoch: int,
    registered: bool, observedLen: int, commitLen: int, dependLen: int,
    success: bool, mustWait: bool, earlyCheck: bool, seq: int,
    waitKind: str, targetStart: int, targetSeq: int,
    releasedBy: int, waitedBarriers: int,
    revealed: bool, abandoned: bool, useMemo: bool,
  }

  /// Extended holds: a sequence that a barrier with that frontier releases,
  /// and a hold that a barrier with at least `target` as its number releases.
  type SeqHold = { owner: int, seq: int }
  type BarrierHold = { owner: int, target: int }

  /// A registration that is not an attempt: a relaxed commit (owner = op id)
  /// or a process start (owner = 0) that a barrier with capture >= need covers.
  type Hold = { owner: int, need: int }

  /// The last action and its choices, for model-driven tests of the Rust type.
  type Act =
    | DoInit
    | DoStart({ id: int, kind: str })
    | DoObserve({ id: int })
    | DoStage({ id: int })
    | DoCommit({ id: int, outcome: str })
    | DoSettle({ id: int })
    | DoComplete({ id: int, dataError: bool, viaMemo: bool, probeOk: bool })
    | DoRecord({ id: int })
    | DoAbort({ id: int })
    | DoClose
    | DoBarrierStart({ probeOk: bool })
    | DoBarrierComplete
    | DoBarrierFail
    | DoPowerCrash
    | DoProcessRestart
    | DoExternalCommit

  type State = {
    log: List[int], durable: int, ops: int -> Op, holds: Set[Hold],
    barrierRunning: bool, barrierCapture: int, barrierSeqFrontier: int,
    barrierNo: int, barrierGroup: Set[int],
    nextSeq: int, releasedSeq: int, releasedCapture: int, coveredLen: int,
    epoch: int, barrierStarts: int, barrierCompletions: int,
    lostDurable: int, lostRelaxed: int, lostReads: int,
    restarts: int, externalCommits: int, writesEver: int, barrierFailures: int,
    stopped: bool, poisoned: bool,
    seqHolds: Set[SeqHold], barrierHolds: Set[BarrierHold], closed: bool,
    memoLen: int, nativeCapture: int, barrierVersion: int,
    lastAction: str, act: Act,
  }

  pure val OPS = 1.to(4)
  pure val MAX_RESTARTS = 2

  pure val idleOp: Op = {
    kind: "none", phase: "none", epoch: 0,
    registered: false, observedLen: 0, commitLen: 0, dependLen: 0,
    success: false, mustWait: false, earlyCheck: false, seq: 0,
    waitKind: "none", targetStart: 0, targetSeq: 0,
    releasedBy: -1, waitedBarriers: 0,
    revealed: false, abandoned: false, useMemo: false,
  }

  pure val initial: State = {
    log: List(), durable: 0, ops: OPS.mapBy(_ => idleOp), holds: Set(),
    barrierRunning: false, barrierCapture: 0, barrierSeqFrontier: 0,
    barrierNo: 0, barrierGroup: Set(),
    nextSeq: 0, releasedSeq: 0, releasedCapture: 0, coveredLen: 0,
    epoch: 0, barrierStarts: 0, barrierCompletions: 0,
    lostDurable: 0, lostRelaxed: 0, lostReads: 0,
    restarts: 0, externalCommits: 0, writesEver: 0, barrierFailures: 0,
    stopped: false, poisoned: false,
    seqHolds: Set(), barrierHolds: Set(), closed: false,
    memoLen: -1, nativeCapture: 0, barrierVersion: -1,
    lastAction: "init", act: DoInit,
  }

  var s: State
  action init = s' = initial

  pure def max(a: int, b: int): int = if (a > b) a else b
  pure def counting: bool = CFG.policy == "P2" or CFG.policy == "P3"
  pure def isWriter(kind: str): bool = kind == "write" or kind == "relaxed"
  pure def isDurableClass(kind: str): bool = Set("write", "replay", "read").contains(kind)
  pure def live(st: State, id: int): bool = st.ops.get(id).epoch == st.epoch

  /// The P2 counter: live attempts between first write and acknowledgement,
  /// plus holds.
  pure def pendingWriters(st: State): int =
    OPS.filter(i => live(st, i) and st.ops.get(i).registered).size() + st.holds.size() +
      st.seqHolds.size() + st.barrierHolds.size()

  /// A live registration with no sequence yet (commit in progress, visible
  /// but not recorded, or an unknown outcome).
  pure def unticketed(st: State): bool =
    OPS.exists(i => live(st, i) and st.ops.get(i).registered and st.ops.get(i).seq == 0)
  pure def extendedP3: bool = CFG.extended and CFG.policy == "P3" and CFG.seqMode == "atCommit"

  /// Highest sequence whose attempt has completed its native commit step.
  pure def committedSeqFrontier(st: State): int =
    OPS.fold(0, (m, i) => {
      val op = st.ops.get(i)
      if (live(st, i) and op.seq > 0 and
          Set("committed", "waiting", "responded", "dropped").contains(op.phase)) max(m, op.seq) else m
    })

  pure def setOp(st: State, id: int, op: Op): State = st.with("ops", st.ops.set(id, op))

  // ---------------------------------------------------------------- attempts

  pure def canStart(st: State, id: int, kind: str): bool = all {
    not(st.stopped),
    not(st.closed),
    st.ops.get(id).phase == "none",
    kind != "relaxed" or CFG.relaxedOps,
  }
  pure def start(st: State, id: int, kind: str): State =
    setOp(st, id, { ...idleOp, kind: kind, phase: "active", epoch: st.epoch,
      earlyCheck: readerMustWait(st) })
      .with("lastAction", "start")

  pure def writerSlotFree(st: State, id: int): bool =
    not(CFG.serialWriters) or OPS.forall(i => i == id or not(live(st, i)) or
      not(isWriter(st.ops.get(i).kind)) or
      not(Set("observed", "staged").contains(st.ops.get(i).phase)))

  /// Snapshot read of the committed (not necessarily durable) order.
  pure def canObserve(st: State, id: int): bool = all {
    live(st, id), st.ops.get(id).phase == "active",
    not(isWriter(st.ops.get(id).kind)) or writerSlotFree(st, id),
  }
  pure def observe(st: State, id: int): State = {
    val op = st.ops.get(id)
    val n = st.log.length()
    setOp(st, id, { ...op, phase: "observed", observedLen: n, dependLen: n })
      .with("lastAction", "observe")
  }

  /// First write of an attempt: register before the commit can happen.
  pure def canStage(st: State, id: int): bool = all {
    live(st, id), isWriter(st.ops.get(id).kind), st.ops.get(id).phase == "observed",
  }
  pure def stage(st: State, id: int): State = {
    val op = st.ops.get(id)
    val reserve = CFG.policy == "P3" and CFG.seqMode == "reserved"
    setOp(st, id, { ...op, phase: "staged", registered: counting and CFG.registerAt == "firstWrite",
      seq: if (reserve) st.nextSeq + 1 else 0 })
      .with("nextSeq", if (reserve) st.nextSeq + 1 else st.nextSeq)
      .with("writesEver", st.writesEver + 1)
      .with("lastAction", "stage")
  }

  /// Native commit step. Outcomes: "ok", "unknownApplied", "unknownLost",
  /// "postCommitError" (committed, but a later step returned a plain error),
  /// "rollback" (no commit; registration ends). With "atCommit", the writer
  /// thread publishes a sequence only when the native commit returned success
  /// ("ok", "postCommitError"), relaxed commits included; an unknown outcome
  /// has no sequence and waits for a barrier that starts after it settles.
  pure def canCommit(st: State, id: int): bool =
    live(st, id) and st.ops.get(id).phase == "staged"
  pure def commit(st: State, id: int, outcome: str): State = {
    val op = st.ops.get(id)
    val applied = Set("ok", "unknownApplied", "postCommitError").contains(outcome)
    val log2 = if (applied) st.log.append(id) else st.log
    val len = log2.length()
    val publishAtCommit = CFG.policy == "P3" and CFG.seqMode == "atCommit" and
      Set("ok", "postCommitError").contains(outcome)
    // The writer thread of an abandoned attempt ends its registration.
    val endAbandoned = extendedP3 and op.abandoned and op.registered and
      Set("rollback", "unknownApplied", "unknownLost").contains(outcome)
    if (endAbandoned)
      setOp(st, id, { ...op, phase: "dropped", registered: false,
          commitLen: if (applied) len else 0 })
        .with("log", log2)
        .with("barrierHolds", if (CFG.abortHolds)
          st.barrierHolds.union(Set({ owner: id, target: st.barrierNo + 1 })) else st.barrierHolds)
        .with("lastAction", "abandoned_end")
    else if (outcome == "rollback")
      setOp(st, id, { ...op, phase: if (op.abandoned) "dropped" else "responded",
          registered: false, success: false })
        .with("lastAction", "rollback")
    else if (extendedP3 and publishAtCommit)
      // Visible now; the writer thread records the sequence in `Record`.
      setOp(st, id, { ...op, phase: "visible", commitLen: len, dependLen: len,
          success: outcome == "ok",
          mustWait: outcome != "postCommitError" or CFG.postCommitErrorWaits })
        .with("log", log2).with("lastAction", outcome)
    else {
      val committed = { ...op, phase: "committed",
        commitLen: if (applied) len else 0,
        dependLen: if (applied) len else op.dependLen,
        success: outcome == "ok",
        mustWait: outcome != "postCommitError" or CFG.postCommitErrorWaits,
        seq: if (publishAtCommit) st.nextSeq + 1 else op.seq }
      setOp(st, id, committed).with("log", log2).with("lastAction", outcome)
        .with("nextSeq", if (publishAtCommit) st.nextSeq + 1 else st.nextSeq)
    }
  }

  /// The writer thread records the sequence after the native commit returned.
  pure def canRecord(st: State, id: int): bool = live(st, id) and st.ops.get(id).phase == "visible"
  pure def record(st: State, id: int): State = {
    val op = st.ops.get(id)
    val seq = st.nextSeq + 1
    val st2 = st.with("nextSeq", seq).with("lastAction", "record")
    if (op.abandoned and op.registered)
      setOp(st2, id, { ...op, phase: "dropped", seq: seq, registered: false })
        .with("seqHolds", if (CFG.abortHolds and seq > st.releasedSeq)
          st.seqHolds.union(Set({ owner: id, seq: seq })) else st.seqHolds)
    else setOp(st2, id, { ...op, phase: if (op.abandoned) "dropped" else "committed", seq: seq })
  }

  /// The attempt's task ends without settling (abort, panic). Its writer
  /// thread may still commit.
  pure def canAbort(st: State, id: int): bool = all {
    live(st, id), isWriter(st.ops.get(id).kind), not(st.ops.get(id).abandoned),
    Set("staged", "visible", "committed", "waiting").contains(st.ops.get(id).phase),
  }
  pure def abort(st: State, id: int): State = {
    val op = st.ops.get(id)
    val writerDone = Set("committed", "waiting").contains(op.phase)
    if (not(CFG.abortHolds))
      // Old behaviour: the registration ends at once and leaves nothing.
      setOp(st, id, { ...op, abandoned: true, registered: false,
          phase: if (writerDone) "dropped" else op.phase })
        .with("lastAction", "abort")
    else if (not(writerDone))
      setOp(st, id, { ...op, abandoned: true }).with("lastAction", "abort")
    else
      setOp(st, id, { ...op, abandoned: true, registered: false, phase: "dropped" })
        .with("seqHolds", if (op.seq > 0 and op.seq > st.releasedSeq)
          st.seqHolds.union(Set({ owner: id, seq: op.seq })) else st.seqHolds)
        .with("barrierHolds", if (op.seq == 0 and op.registered)
          st.barrierHolds.union(Set({ owner: id, target: st.barrierNo + 1 })) else st.barrierHolds)
        .with("lastAction", "abort")
  }

  pure def respond(st: State, id: int, releasedBy: int): State = {
    val op = st.ops.get(id)
    setOp(st, id, { ...op, phase: "responded", registered: false, releasedBy: releasedBy })
  }

  /// After native commit: answer (relaxed, plain error) or wait for a barrier.
  /// With `relaxedHolds`, only a relaxed commit that succeeded answers at
  /// once; a relaxed error follows the writer path. A stopped store answers
  /// with an error unless a completed barrier already covers the commit.
  pure def canSettle(st: State, id: int): bool =
    live(st, id) and st.ops.get(id).phase == "committed"
  pure def stoppedRespond(st: State, id: int, op: Op): State =
    respond(setOp(st, id, { ...op, success: false }), id, -1).with("lastAction", "stopped_respond")
  pure def settle(st: State, id: int): State = {
    val op0 = st.ops.get(id)
    val op = { ...op0, registered: op0.registered or
      (counting and CFG.registerAt == "wait" and op0.kind != "relaxed" and op0.mustWait) }
    if (op.kind == "relaxed" and (op.success or not(CFG.relaxedHolds))) {
      if (st.stopped) stoppedRespond(st, id, op)
      else {
        val held = if (CFG.relaxedHolds and counting and not(extendedP3) and op.commitLen > st.coveredLen)
          st.holds.union(Set({ owner: id, need: op.commitLen })) else st.holds
        val seqHeld = if (extendedP3 and op.seq > st.releasedSeq)
          st.seqHolds.union(Set({ owner: id, seq: op.seq })) else st.seqHolds
        respond(setOp(st, id, op), id, -1).with("holds", held).with("seqHolds", seqHeld)
          .with("lastAction", "relaxed_respond")
      }
    } else if (not(op.mustWait)) {
      respond(setOp(st, id, op), id, -1).with("lastAction", "error_respond_without_wait")
    } else if (CFG.policy == "P3" and CFG.seqMode == "atCommit" and op.seq == 0) {
      if (st.stopped) stoppedRespond(st, id, op)
      else setOp(st, id, { ...op, phase: "waiting", waitKind: "barrier", targetStart: st.barrierNo })
        .with("lastAction", "unknown_waits")
    } else if (CFG.policy == "P3") {
      val publish = CFG.seqMode == "afterCommit"
      val seq = if (publish) st.nextSeq + 1 else op.seq
      val st2 = setOp(st, id, { ...op, seq: seq })
        .with("nextSeq", if (publish) st.nextSeq + 1 else st.nextSeq)
      if (seq <= st.releasedSeq)
        respond(st2, id, st.releasedCapture).with("lastAction", "writer_released_at_once")
      else if (st.stopped) stoppedRespond(st2, id, st2.ops.get(id))
      else
        setOp(st2, id, { ...st2.ops.get(id), phase: "waiting", waitKind: "seq", targetSeq: seq })
          .with("lastAction", "writer_waits")
    } else if (st.stopped) stoppedRespond(st, id, op)
    else
      setOp(st, id, { ...op, phase: "waiting", waitKind: "group" })
        .with("lastAction", "writer_waits")
  }

  /// End of a non-writing attempt (replay or read). It answers with data or
  /// with a data-dependent error (`dataError`, for example a failed condition
  /// that returns the item). `viaMemo`: its connection memo applies;
  /// `probeOk`: its version probe works.
  pure def canComplete(st: State, id: int, viaMemo: bool): bool = all {
    live(st, id), not(isWriter(st.ops.get(id).kind)), st.ops.get(id).phase == "observed",
    // One memo user waits at a time, so the memo value has one writer.
    not(viaMemo) or OPS.forall(i => not(live(st, i) and st.ops.get(i).phase == "waiting" and
      st.ops.get(i).useMemo)),
  }
  pure def externalAt(st: State, op: Op, probeOk: bool): bool =
    if (not(CFG.nativeClause)) false
    else if (not(extendedP3)) st.log.length() > st.coveredLen
    else not(CFG.memo and op.useMemo and op.observedLen == st.memoLen) and
      (not(probeOk) or st.log.length() != st.nativeCapture)
  pure def readerMustWait(st: State): bool =
    if (CFG.policy == "P0") true
    else if (CFG.policy == "P1") false
    else pendingWriters(st) > 0 or (CFG.nativeClause and st.log.length() > st.coveredLen)
  pure def fastAllowed(st: State): bool =
    if (CFG.fastReads == "safe") not(unticketed(st)) and st.barrierHolds.size() == 0
    else if (CFG.fastReads == "unsafe") st.barrierHolds.size() == 0
    else false
  pure def memoAfter(st: State, op: Op): int =
    if (op.useMemo and (op.success or op.revealed)) op.observedLen else st.memoLen
  pure def respondReader(st: State, id: int, op: Op, releasedBy: int, label: str): State =
    respond(setOp(st, id, op), id, releasedBy).with("memoLen", memoAfter(st, op))
      .with("lastAction", label)
  pure def complete(st: State, id: int, dataError: bool, viaMemo: bool, probeOk: bool): State = {
    val op = { ...st.ops.get(id), success: not(dataError), revealed: dataError,
      useMemo: CFG.memo and viaMemo }
    val mustWait = if (CFG.readerCheckAt == "start") op.earlyCheck else readerMustWait(st)
    if (st.stopped) stoppedRespond(st, id, { ...op, revealed: false })
    else if (dataError and not(CFG.dataErrorSettles))
      respond(setOp(st, id, op), id, -1).with("lastAction", "data_error_respond")
    else if (op.kind == "read" and not(CFG.readsWait))
      respond(setOp(st, id, op), id, -1).with("lastAction", "read_respond")
    else if (extendedP3) {
      val pending = pendingWriters(st) > 0
      if (externalAt(st, op, probeOk) or (pending and not(fastAllowed(st))))
        setOp(st, id, { ...op, phase: "waiting", waitKind: "barrier", targetStart: st.barrierNo })
          .with("lastAction", "replay_waits")
      else if (pending and st.nextSeq > st.releasedSeq)
        setOp(st, id, { ...op, phase: "waiting", waitKind: "seq", targetSeq: st.nextSeq })
          .with("lastAction", "replay_waits_for_seq")
      else respondReader(st, id, op, -1, "replay_respond")
    }
    else if (not(mustWait))
      respond(setOp(st, id, op), id, -1).with("lastAction", if (op.kind == "read") "read_respond" else "replay_respond")
    else if (CFG.policy == "P3" and CFG.readerTarget == "publishedSeq") {
      val target = committedSeqFrontier(st)
      if (target <= st.releasedSeq)
        respond(setOp(st, id, op), id, st.releasedCapture).with("lastAction", "replay_released_at_once")
      else setOp(st, id, { ...op, phase: "waiting", waitKind: "seq", targetSeq: target })
        .with("lastAction", "replay_waits")
    } else if (CFG.policy == "P3")
      setOp(st, id, { ...op, phase: "waiting", waitKind: "barrier", targetStart: st.barrierNo })
        .with("lastAction", "replay_waits")
    else
      setOp(st, id, { ...op, phase: "waiting", waitKind: "group" })
        .with("lastAction", "replay_waits")
  }

  // ---------------------------------------------------------------- barrier

  pure def waiting(st: State): Set[int] =
    OPS.filter(i => live(st, i) and st.ops.get(i).phase == "waiting")

  /// A barrier starts only when someone needs it. Timing is otherwise free,
  /// which covers the immediate start for a lone writer, the running barrier
  /// as the collection window, and the bounded extra wait.
  pure def canStartBarrier(st: State): bool = all {
    not(st.stopped),
    not(st.barrierRunning),
    waiting(st).size() > 0 or st.holds.exists(h => h.need > st.coveredLen or h.owner == 0) or
      st.seqHolds.size() > 0 or st.barrierHolds.size() > 0,
  }
  pure def startBarrier(st: State, probeOk: bool): State = st
    .with("barrierRunning", true)
    .with("barrierVersion", if (probeOk) st.log.length() else -1)
    .with("barrierCapture", st.log.length())
    .with("barrierSeqFrontier", committedSeqFrontier(st))
    .with("barrierNo", st.barrierNo + 1)
    .with("barrierGroup", waiting(st).filter(i => st.ops.get(i).waitKind == "group"))
    .with("barrierStarts", st.barrierStarts + 1)
    .with("lastAction", "barrier_start")

  pure def releasedByBarrier(st: State, op: Op, id: int, frontier: int): bool =
    if (op.waitKind == "group") st.barrierGroup.contains(id)
    else if (op.waitKind == "seq") op.targetSeq <= frontier
    else op.targetStart < st.barrierNo

  pure def completeBarrier(st: State): State = {
    val capture = st.barrierCapture
    val frontier = max(st.releasedSeq, st.barrierSeqFrontier)
    val ops2 = OPS.mapBy(i => {
      val op = st.ops.get(i)
      if (live(st, i) and op.phase == "waiting") {
        if (releasedByBarrier(st, op, i, frontier))
          { ...op, phase: "responded", registered: false, releasedBy: capture }
        else { ...op, waitedBarriers: op.waitedBarriers + 1 }
      } else op
    })
    val released = OPS.filter(i => live(st, i) and st.ops.get(i).phase == "waiting" and
      ops2.get(i).phase == "responded")
    val memo = released.fold(st.memoLen, (m, i) => memoAfter(st.with("memoLen", m), st.ops.get(i)))
    st.with("ops", ops2)
      .with("memoLen", memo)
      .with("nativeCapture", if (st.barrierVersion >= 0) st.barrierVersion else st.nativeCapture)
      .with("seqHolds", st.seqHolds.filter(h => h.seq > frontier))
      .with("barrierHolds", st.barrierHolds.filter(h => h.target > st.barrierNo))
      .with("durable", if (st.poisoned) st.durable else max(st.durable, capture))
      .with("coveredLen", max(st.coveredLen, capture))
      .with("holds", st.holds.filter(h => h.need > capture))
      .with("releasedSeq", frontier)
      .with("releasedCapture", if (st.barrierSeqFrontier > st.releasedSeq) capture else st.releasedCapture)
      .with("barrierRunning", false)
      .with("barrierGroup", Set())
      .with("barrierCompletions", st.barrierCompletions + 1)
      .with("lastAction", "barrier_complete")
  }

  /// The sync returns an error: nothing new is durable, and later barriers
  /// prove nothing (`poisoned`). Without `stopOnBarrierFail`, every waiter
  /// the barrier would have released gets an error. With it, every waiter
  /// gets an error and the store stops.
  pure def failBarrier(st: State): State = {
    val frontier = max(st.releasedSeq, st.barrierSeqFrontier)
    val answered = OPS.filter(i => live(st, i) and st.ops.get(i).phase == "waiting" and
      (CFG.stopOnBarrierFail or releasedByBarrier(st, st.ops.get(i), i, frontier)))
    val newHolds = answered.filter(i => st.ops.get(i).commitLen > 0)
      .map(i => { owner: i, need: st.ops.get(i).commitLen })
    st.with("ops", OPS.mapBy(i => {
        val op = st.ops.get(i)
        if (answered.contains(i)) { ...op, phase: "responded", registered: false, success: false,
          revealed: false }
        else if (live(st, i) and op.phase == "waiting") { ...op, waitedBarriers: op.waitedBarriers + 1 }
        else op
      }))
      .with("holds", if (CFG.failedBarrierHolds) st.holds.union(newHolds) else st.holds)
      .with("barrierRunning", false)
      .with("barrierGroup", Set())
      .with("barrierFailures", st.barrierFailures + 1)
      .with("stopped", CFG.stopOnBarrierFail)
      .with("poisoned", true)
      .with("lastAction", "barrier_fail")
  }

  // ---------------------------------------------------------------- failures

  pure def respondedSuccess(st: State, i: int): bool =
    st.ops.get(i).phase == "responded" and st.ops.get(i).success

  /// An answer that carries data: a success, or an error that reveals what
  /// the attempt observed.
  pure def answeredWithData(st: State, i: int): bool =
    st.ops.get(i).phase == "responded" and (st.ops.get(i).success or st.ops.get(i).revealed)

  /// Volatile state is lost; native commits are kept (`truncate` false) or
  /// cut to the durable prefix (`truncate` true).
  pure def restart(st: State, truncate: bool): State = {
    val keep = if (truncate) st.durable else st.log.length()
    pure def lost(i: int): bool = answeredWithData(st, i) and st.ops.get(i).dependLen > keep
    val log2 = st.log.slice(0, keep)
    st.with("log", log2)
      .with("ops", OPS.mapBy(i => {
        val op = st.ops.get(i)
        if (Set("none", "responded", "dropped").contains(op.phase)) op
        else { ...op, phase: "dead", registered: false }
      }))
      .with("holds", Set())
      .with("seqHolds", Set())
      .with("barrierHolds", if (CFG.startupHold) Set({ owner: 0, target: 1 }) else Set())
      .with("closed", false).with("memoLen", -1).with("nativeCapture", -1).with("barrierVersion", -1)
      .with("barrierRunning", false).with("barrierGroup", Set())
      .with("barrierCapture", 0).with("barrierSeqFrontier", 0).with("barrierNo", 0)
      .with("nextSeq", 0).with("releasedSeq", 0).with("releasedCapture", 0).with("coveredLen", 0)
      .with("epoch", st.epoch + 1)
      .with("stopped", false).with("poisoned", false)
      .with("restarts", st.restarts + 1)
      .with("lostDurable", st.lostDurable +
        OPS.filter(i => lost(i) and isDurableClass(st.ops.get(i).kind)).size())
      .with("lostRelaxed", st.lostRelaxed +
        OPS.filter(i => lost(i) and st.ops.get(i).kind == "relaxed").size())
      .with("lostReads", st.lostReads +
        OPS.filter(i => lost(i) and st.ops.get(i).kind == "read").size())
      .with("lastAction", if (truncate) "power_crash" else "process_restart")
  }

  pure def externalCommit(st: State): State = st
    .with("log", st.log.append(0))
    .with("externalCommits", st.externalCommits + 1)
    .with("lastAction", "external_commit")

  // ---------------------------------------------------------------- actions

  action Start = {
    nondet id = OPS.oneOf()
    nondet kind = Set("write", "replay", "read", "relaxed").oneOf()
    all { canStart(s, id, kind), s' = start(s, id, kind).with("act", DoStart({ id: id, kind: kind })) }
  }
  action Observe = {
    nondet id = OPS.oneOf()
    all { canObserve(s, id), s' = observe(s, id).with("act", DoObserve({ id: id })) }
  }
  action Stage = {
    nondet id = OPS.oneOf()
    all { canStage(s, id), s' = stage(s, id).with("act", DoStage({ id: id })) }
  }
  action Commit = {
    nondet id = OPS.oneOf()
    nondet outcome = Set("ok", "ok", "unknownApplied", "unknownLost", "postCommitError", "rollback").oneOf()
    all { canCommit(s, id), s' = commit(s, id, outcome).with("act", DoCommit({ id: id, outcome: outcome })) }
  }
  action Settle = {
    nondet id = OPS.oneOf()
    all { canSettle(s, id), s' = settle(s, id).with("act", DoSettle({ id: id })) }
  }
  action Complete = {
    nondet id = OPS.oneOf()
    nondet dataError = Set(false, CFG.extended).oneOf()
    nondet viaMemo = Set(false, CFG.memo).oneOf()
    nondet probeOk = Set(true, not(CFG.probeFails)).oneOf()
    all {
      canComplete(s, id, viaMemo),
      s' = complete(s, id, dataError, viaMemo, probeOk)
        .with("act", DoComplete({ id: id, dataError: dataError, viaMemo: viaMemo, probeOk: probeOk })),
    }
  }
  action Record = {
    nondet id = OPS.oneOf()
    all { canRecord(s, id), s' = record(s, id).with("act", DoRecord({ id: id })) }
  }
  action Abort = {
    nondet id = OPS.oneOf()
    all { CFG.extended, canAbort(s, id), s' = abort(s, id).with("act", DoAbort({ id: id })) }
  }
  action Close = all {
    CFG.extended, not(s.closed), s' = s.with("closed", true).with("lastAction", "close").with("act", DoClose),
  }
  action BarrierStart = {
    nondet probeOk = Set(true, not(CFG.probeFails)).oneOf()
    all { canStartBarrier(s), s' = startBarrier(s, probeOk).with("act", DoBarrierStart({ probeOk: probeOk })) }
  }
  action BarrierComplete = all { s.barrierRunning, s' = completeBarrier(s).with("act", DoBarrierComplete) }
  action BarrierFail = all {
    CFG.barrierFails, s.barrierRunning, s.barrierFailures < 1,
    s' = failBarrier(s).with("act", DoBarrierFail),
  }
  action PowerCrash = all {
    s.restarts < MAX_RESTARTS, s' = restart(s, true).with("act", DoPowerCrash),
  }
  action ProcessRestart = all {
    CFG.restarts, s.restarts < MAX_RESTARTS, s' = restart(s, false).with("act", DoProcessRestart),
  }
  action ExternalCommit = all {
    CFG.external, s.externalCommits < 1, s' = externalCommit(s).with("act", DoExternalCommit),
  }

  action step = any {
    Start, Observe, Stage, Commit, Record, Settle, Complete, Abort, Close,
    BarrierStart, BarrierComplete, BarrierFail, PowerCrash, ProcessRestart, ExternalCommit,
  }

  // ---------------------------------------------------------------- properties

  /// Every Durable success response depends only on a durable prefix: a power
  /// crash now cannot contradict it. `dependLen` is the own commit for a
  /// writer, and the observed prefix for a replay or a read.
  pure def durableAcksAt(st: State): bool = OPS.forall(i => {
    val op = st.ops.get(i)
    not(answeredWithData(st, i) and isDurableClass(op.kind)) or op.dependLen <= st.durable
  })
  val durableAcks = durableAcksAt(s)
  /// A barrier never releases a writer whose commit it did not capture.
  pure def releaseSoundAt(st: State): bool = OPS.forall(i => {
    val op = st.ops.get(i)
    not(op.phase == "responded" and op.releasedBy >= 0 and op.commitLen > 0) or
      op.commitLen <= op.releasedBy
  })
  val releaseSound = releaseSoundAt(s)
  /// Relaxed: a crash leaves a prefix of the commit order; durable never
  /// exceeds what exists. (Losses of relaxed answers are allowed.)
  val prefixOnly = s.durable <= s.log.length() and s.coveredLen <= s.log.length()
  /// P2/P3: a store with only non-writing attempts runs no barrier (a failed
  /// version probe may ask for one).
  val idleWithoutBarrier =
    not(counting and s.writesEver == 0 and s.restarts == 0 and s.externalCommits == 0 and
      not(CFG.probeFails)) or
      s.barrierStarts == 0
  /// No waiter outlives two barrier completions.
  val boundedWait = OPS.forall(i => not(live(s, i) and s.ops.get(i).phase == "waiting") or
    s.ops.get(i).waitedBarriers <= 1)

  /// P3 with sequences published in the commit step (design rule 4): no
  /// writer with a sequence still waits for a commit that a completed barrier
  /// made durable (extended: whose sequence a completed barrier captured). An unknown outcome has no sequence and waits for a later
  /// barrier.
  val coveredCommitReleased = not(CFG.policy == "P3" and CFG.seqMode == "atCommit") or
    OPS.forall(i => not(live(s, i) and s.ops.get(i).phase == "waiting" and
      s.ops.get(i).seq > 0 and s.ops.get(i).commitLen > 0 and
      (if (CFG.extended) s.ops.get(i).seq <= s.releasedSeq
       else s.ops.get(i).commitLen <= s.durable)))

  /// D11: no read answer exposes data that a crash then removed.
  val readsSafe = s.lostReads == 0

  val inv = all {
    durableAcks, readsSafe, releaseSound, prefixOnly, idleWithoutBarrier, boundedWait,
    coveredCommitReleased,
  }

  // ---------------------------------------------------------------- witnesses

  pure def startedOps(st: State): int = OPS.filter(i => st.ops.get(i).phase != "none").size()

  val lone_writer_witness = s.barrierCompletions == 1 and startedOps(s) == 1 and
    OPS.exists(i => respondedSuccess(s, i) and s.ops.get(i).kind == "write")
  val idle_replays_witness = s.barrierStarts == 0 and
    OPS.filter(i => respondedSuccess(s, i) and s.ops.get(i).kind == "replay").size() >= 2
  val replay_waits_witness = OPS.exists(i => respondedSuccess(s, i) and
    s.ops.get(i).kind == "replay" and s.ops.get(i).releasedBy >= 0 and s.ops.get(i).dependLen > 0)
  val replay_of_unsynced_witness = OPS.exists(i => respondedSuccess(s, i) and
    s.ops.get(i).kind == "replay" and s.ops.get(i).observedLen > 0 and s.writesEver > 0)
  val rollback_witness = s.lastAction == "rollback" and s.writesEver > 0
  val back_to_back_witness = s.barrierCompletions >= 2 and
    OPS.exists(i => respondedSuccess(s, i) and s.ops.get(i).waitedBarriers == 1)
  val covered_but_waiting_witness = OPS.exists(i => live(s, i) and
    s.ops.get(i).phase == "waiting" and s.ops.get(i).commitLen > 0 and
    s.ops.get(i).commitLen <= s.durable)
  /// Design rule 4: a commit that a completed barrier captured is released at
  /// `wait` without another barrier.
  val released_at_once_witness = s.lastAction == "writer_released_at_once" and
    OPS.exists(i => respondedSuccess(s, i) and s.ops.get(i).kind == "write")
  val relaxed_lost_witness = s.lostRelaxed > 0
  val read_exposes_lost_witness = s.lostReads > 0
  /// A read that observed an unsynced commit answered after a barrier.
  val read_waits_witness = OPS.exists(i => respondedSuccess(s, i) and
    s.ops.get(i).kind == "read" and s.ops.get(i).releasedBy >= 0 and s.ops.get(i).dependLen > 0)
  val stopped_witness = s.stopped and s.barrierFailures > 0
  val crash_survivor_witness = s.restarts > 0 and
    OPS.exists(i => respondedSuccess(s, i) and isDurableClass(s.ops.get(i).kind) and
      s.ops.get(i).epoch < s.epoch and s.ops.get(i).dependLen > 0)

  // ---------------------------------------------------------------- scenarios

  /// The reviewer's hole: attempt 1 commits; attempt 2 replays its receipt.
  pure def replayScenario(st: State): State = {
    val a = commit(stage(observe(start(st, 1, "write"), 1), 1), 1, "ok")
    complete(observe(start(a, 2, "replay"), 2), 2, false, false, true)
  }
  run replayWaitsUnderCounterTest = {
    val st = replayScenario(initial)
    val op = st.ops.get(2)
    all {
      assert(canCommit(stage(observe(start(initial, 1, "write"), 1), 1), 1)),
      assert(not(counting) or CFG.readerTarget == "publishedSeq" or CFG.registerAt == "wait" or
        op.phase == "waiting"),
      // Registration after the native commit leaves the counter at zero here.
      assert(not(counting) or CFG.registerAt != "wait" or not(durableAcksAt(st))),
      // The published-sequence target misses a commit whose sequence is not yet published.
      assert(CFG.policy != "P3" or CFG.readerTarget != "publishedSeq" or not(durableAcksAt(st))),
      assert(CFG.policy != "P1" or (op.phase == "responded" and not(durableAcksAt(st)))),
    }
  }
  run loneWriterOneBarrierTest = {
    val a = settle(commit(stage(observe(start(initial, 1, "write"), 1), 1), 1, "ok"), 1)
    val b = completeBarrier(startBarrier(a, true))
    all {
      assert(canStartBarrier(a)),
      assert(b.ops.get(1).phase == "responded"),
      assert(b.barrierCompletions == 1),
      assert(b.durable == 1),
    }
  }
  run idleReplaysNeedNoBarrierTest = {
    val a = complete(observe(start(initial, 1, "replay"), 1), 1, false, false, true)
    val b = complete(observe(start(a, 2, "replay"), 2), 2, false, false, true)
    all {
      assert(CFG.policy == "P0" or b.ops.get(2).phase == "responded"),
      assert(CFG.policy == "P0" or not(canStartBarrier(b))),
    }
  }
  /// A barrier that starts between two concurrent writers' commits.
  run reservedSequenceScenarioTest = {
    val a = stage(observe(start(initial, 1, "write"), 1), 1)
    val b = stage(observe(start(a, 2, "write"), 2), 2)
    val c = startBarrier(settle(commit(b, 2, "ok"), 2), true)
    val d = settle(commit(completeBarrier(c), 1, "ok"), 1)
    all {
      assert(CFG.serialWriters == not(canObserve(start(a, 2, "write"), 2))),
      assert(CFG.serialWriters or CFG.policy != "P3" or CFG.seqMode != "reserved" or
        not(releaseSoundAt(d))),
      assert(CFG.serialWriters or CFG.policy != "P3" or CFG.seqMode == "reserved" or
        (releaseSoundAt(d) and d.ops.get(1).phase == "waiting")),
    }
  }
}

/// P0: every attempt waits (old behaviour, reads included), with every hazard
/// except a failed sync enabled. Holds. A failed sync breaks it like every
/// variant that keeps serving after the failure.
module durable_ack_p0 {
  import durable_ack(CFG = {
    policy: "P0",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: false,
    startupHold: false,
    restarts: true,
    external: true,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P1: a non-writing attempt never waits. Must be violated.
module durable_ack_p1 {
  import durable_ack(CFG = {
    policy: "P1",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 (W1 counter rule, reads included): one process, no restart, no relaxed class, syncs never fail. Holds.
module durable_ack_p2 {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 with a process restart that keeps unsynced commits. Must be violated.
module durable_ack_p2_restart {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 + P4 where a relaxed commit drops its registration at its answer. Must be violated.
module durable_ack_p2_relaxed {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 where a plain error after the native commit skips the wait. Must be violated.
module durable_ack_p2_post_commit_error {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: false,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 where a failed sync ends the writer registration. Must be violated.
module durable_ack_p2_barrier_fail {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 where a writer registers only after its native commit. Must be violated.
module durable_ack_p2_register_at_wait {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "wait",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 where a non-writer reads the counter before its first read. Must be violated.
module durable_ack_p2_check_at_start {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "start",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2 with a second process on the same file. Must be violated.
module durable_ack_p2_external {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: true,
    restarts: false,
    external: true,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// Corrected P2 (P2'): startup hold, relaxed holds, a failed sync stops the store, post-commit errors wait, reads wait; restarts, relaxed operations, sync failures and concurrent writers enabled. Holds.
module durable_ack_p2_fixed {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2' with a second process: adds the native clause. Holds.
module durable_ack_p2_fixed_external {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: true,
    nativeClause: true,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P3: sequences published by the writer in the native-commit step, waiting non-writers need a later barrier, plus the P2' corrections. Holds.
module durable_ack_p3 {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "atCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P3 with the sequence published only at `wait`: safe, but a commit that a
/// barrier captured before `wait` needs one more barrier (rule 4 is lost). Holds.
module durable_ack_p3_after_commit {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P3 with sequences reserved at the first write and concurrent writers. Must be violated.
module durable_ack_p3_reserved {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "reserved",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P3 with reserved sequences but one writer at a time. Holds.
module durable_ack_p3_reserved_serial {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "reserved",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P3 where a waiting non-writer targets the highest published sequence. Must be violated.
module durable_ack_p3_reader_seq {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "afterCommit",
    readerTarget: "publishedSeq",
    relaxedOps: false,
    relaxedHolds: false,
    startupHold: false,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: false,
    failedBarrierHolds: false,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2' where a read-only attempt never waits (the read behaviour before D11). Must be violated.
module durable_ack_p2_reads_skip {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: false,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// P2' where a failed sync keeps holds but the store keeps serving: a later
/// sync releases the holds without proving the data. Must be violated.
module durable_ack_p2_fail_holds_without_stop {
  import durable_ack(CFG = {
    policy: "P2",
    seqMode: "afterCommit",
    readerTarget: "nextBarrier",
    relaxedOps: false,
    relaxedHolds: true,
    startupHold: true,
    restarts: false,
    external: false,
    nativeClause: false,
    serialWriters: true,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: false,
    readsWait: true,
    extended: false,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "off",
    memo: false,
    probeFails: false,
  }).*
}

/// The implemented policy (`storage_common::durability::DurabilityBatch`):
/// P3 with sequences published at commit, non-writers wait for a later
/// barrier, reads wait, startup hold, relaxed holds, a failed sync stops the
/// store, and the native clause for a second process. Holds.
/// `crates/storage-common/src/quint_durability_tests.rs` replays its traces
/// against the Rust type.
module durable_ack_impl {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "atCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: true,
    nativeClause: true,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: true,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "safe",
    memo: true,
    probeFails: true,
  }).*
}

/// The implemented policy for one process without a native version probe
/// (RocksDB): `durable_ack_impl` without the second process. Holds.
module durable_ack_impl_local {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "atCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: true,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "safe",
    memo: false,
    probeFails: false,
  }).*
}

/// DUR-F1: a data-dependent error answers without settlement. Must be violated.
module durable_ack_impl_no_error_settle {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "atCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: true,
    dataErrorSettles: false,
    abortHolds: true,
    fastReads: "safe",
    memo: false,
    probeFails: false,
  }).*
}

/// DUR-F2: a dropped guard ends its registration at once with no hold. Must be violated.
module durable_ack_impl_drop_without_hold {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "atCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: true,
    dataErrorSettles: true,
    abortHolds: false,
    fastReads: "safe",
    memo: false,
    probeFails: false,
  }).*
}

/// DUR-F9 applied while a registration has no sequence yet. Must be violated.
module durable_ack_impl_fast_unsafe {
  import durable_ack(CFG = {
    policy: "P3",
    seqMode: "atCommit",
    readerTarget: "nextBarrier",
    relaxedOps: true,
    relaxedHolds: true,
    startupHold: true,
    restarts: true,
    external: false,
    nativeClause: false,
    serialWriters: false,
    postCommitErrorWaits: true,
    registerAt: "firstWrite",
    readerCheckAt: "completion",
    barrierFails: true,
    failedBarrierHolds: true,
    stopOnBarrierFail: true,
    readsWait: true,
    extended: true,
    dataErrorSettles: true,
    abortHolds: true,
    fastReads: "unsafe",
    memo: false,
    probeFails: false,
  }).*
}
