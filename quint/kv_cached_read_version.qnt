/// FoundationDB write attempts at a cached read version (proposal).
///
/// A write attempt reads its key at a cached read version `rv` that can be older than the
/// newest commit. A same-observed-value set may settle read-only without a resolver
/// check; cached strong outcomes then rerun Fresh. Explicit eventual reads and unknown
/// completion are outside this model. For mutation-bearing commits, the resolver aborts the commit when the key was written by a commit with a
/// version in (rv, commit]; the aborted attempt runs again at a new read version. An outcome
/// with no commit (a failed condition) has no resolver check, so it reads again at a new read
/// version before it answers. A consistent get always uses a new read version. A new read
/// version is the newest commit version at that instant.
///
/// The clock advances by one on every action; `start`, `end` and commit `at` are instants.
/// Value 0 is an absent item. The check is per operation, not a full linearizability search:
/// a get, failed condition or optimized no-op must observe a value committed at some instant
/// in [start, end]; a mutation-bearing success must read the state just before its commit.
module kv_cached_read_version {
  type Config = { cachedWrites: bool, rereadNoCommit: bool, redecide: bool, cachedGets: bool, stagedWriteDetector: bool }
  type Commit = { key: int, value: int, ver: int, at: int }
  type Op = {
    phase: str, kind: str, key: int, expect: int, value: int, start: int,
    fresh: bool, rv: int, obs: int, result: str, readOk: bool,
  }
  type Done = {
    client: int, kind: str, key: int, expect: int, value: int, start: int, end: int,
    obs: int, result: str, readOk: bool,
  }
  type State = {
    cfg: Config, clock: int, ver: int, commits: List[Commit],
    ops: int -> Op, cache: int -> int, history: List[Done], started: int,
    aborts: int, rereads: int, redecisions: int, staleCommits: int, optimized: int, changedFreshDecisions: int,
  }

  pure val CLIENTS = 1.to(2)
  pure val KEYS = 1.to(2)
  pure val VALUES = Set(1, 2)
  pure val EXPECTS = Set(0, 1, 2)
  pure val KINDS = Set("put", "cput", "get")
  pure val MAX_OPS = 6
  pure val idleOp: Op = {
    phase: "idle", kind: "", key: 0, expect: 0, value: 0, start: 0,
    fresh: false, rv: 0, obs: 0, result: "", readOk: true,
  }
  pure def initialState(cfg: Config): State = {
    cfg: cfg, clock: 0, ver: 0, commits: List(),
    ops: CLIENTS.mapBy(_ => idleOp), cache: CLIENTS.mapBy(_ => 0), history: List(), started: 0,
    aborts: 0, rereads: 0, redecisions: 0, staleCommits: 0, optimized: 0, changedFreshDecisions: 0,
  }
  pure val design: Config = { cachedWrites: true, rereadNoCommit: true, redecide: true, cachedGets: false, stagedWriteDetector: false }

  var state: State
  action init = state' = initialState(design)
  action initDesign = state' = initialState(design)
  // Control: every attempt requests a new read version (today's behaviour).
  action initFreshVersions = state' = initialState(design.with("cachedWrites", false))
  // Must fail: a failed condition answers from the cached read version.
  action initNoReread = state' = initialState(design.with("rereadNoCommit", false))
  // Must fail: a consistent get reads at the cached read version.
  action initCachedGet = state' = initialState(design.with("cachedGets", true))
  // Must fail: the second read only confirms the first decision; it answers "condition
  // failed" even when the new read satisfies the condition.
  action initRereadNoRedecide = state' = initialState(design.with("redecide", false))

  // Must fail: staged mutations are mistaken for a resolver-validated commit.
  action initStagedWriteDetector = state' = initialState(design.with("stagedWriteDetector", true))

  pure def maxInt(a: int, b: int): int = if (a > b) a else b
  pure def valueAtVer(s: State, k: int, v: int): int =
    s.commits.foldl(0, (out, c) => if (c.key == k and c.ver <= v) c.value else out)
  pure def valueAtTime(s: State, k: int, t: int): int =
    s.commits.foldl(0, (out, c) => if (c.key == k and c.at <= t) c.value else out)
  pure def lastWriteVer(s: State, k: int): int =
    s.commits.foldl(0, (out, c) => if (c.key == k) c.ver else out)
  pure def seenDuring(s: State, k: int, v: int, start: int, end: int): bool =
    start.to(end).exists(t => valueAtTime(s, k, t) == v)
  pure def tick(s: State): State = s.with("clock", s.clock + 1)
  pure def setOp(s: State, c: int, op: Op): State = s.with("ops", s.ops.set(c, op))
  // A new read version is the newest commit version; the client caches it.
  pure def newVersion(s: State, c: int): State = s.with("cache", s.cache.set(c, s.ver))

  pure def start(s: State, c: int, kind: str, k: int, e: int, v: int): State =
    tick(setOp(s, c, idleOp.with("phase", "run").with("kind", kind).with("key", k)
      .with("expect", e).with("value", v).with("start", s.clock)).with("started", s.started + 1))

  pure def consistentGet(s: State, c: int): State = {
    val op = s.ops.get(c)
    val rv = if (s.cfg.cachedGets) s.cache.get(c) else s.ver
    val s1 = if (s.cfg.cachedGets) s else newVersion(s, c)
    tick(setOp(s1, c, op.with("phase", "respond").with("result", "value").with("obs", valueAtVer(s, op.key, rv))))
  }

  pure def write(s: State, c: int): State = {
    val op = s.ops.get(c)
    val cached = s.cfg.cachedWrites and not(op.fresh)
    val rv = if (cached) s.cache.get(c) else s.ver
    val s1 = (if (cached) s else newVersion(s, c))
      .with("changedFreshDecisions", s.changedFreshDecisions +
        (if (op.result == "optimized" and valueAtVer(s, op.key, rv) != op.obs) 1 else 0))
    val obs = valueAtVer(s, op.key, rv)
    val commits = op.kind == "put" or obs == op.expect
    if (commits)
      tick(setOp(s1, c, op.with("phase", "commit").with("rv", rv).with("obs", obs)))
    // No commit, no resolver check. A read version obtained after the request started
    // (`fresh`) already lies in [start, end]; a cached one needs the second read.
    else if (cached and s.cfg.rereadNoCommit)
      tick(setOp(s1, c, op.with("phase", "reread").with("obs", obs)))
    else tick(setOp(s1, c, op.with("phase", "respond").with("result", "failed").with("obs", obs)))
  }

  // The resolver aborts the commit when the key was written in (rv, commit].
  pure def commit(s: State, c: int, optimize: bool): State = {
    val op = s.ops.get(c)
    // Native optimization is selected independently of policy and current database state.
    // The SDK may discard a set of the bytes read, settle read-only (version -1),
    // and skip the resolver even when a newer write conflicts with this read version.
    if (optimize and op.value == op.obs) {
      val s1 = s.with("optimized", s.optimized + 1)
      if (s.cfg.cachedWrites and not(op.fresh) and not(s.cfg.stagedWriteDetector))
        tick(setOp(s1, c, op.with("phase", "run").with("fresh", true).with("result", "optimized")))
      else tick(setOp(s1, c, op.with("phase", "respond").with("result", "noop")))
    }
    else if (lastWriteVer(s, op.key) > op.rv)
      tick(setOp(s, c, op.with("phase", "run").with("fresh", true)).with("aborts", s.aborts + 1))
    else {
      val cv = s.ver + 1
      tick(setOp(s, c, op.with("phase", "respond").with("result", "ok")
          .with("readOk", op.obs == valueAtVer(s, op.key, s.ver)))
        .with("commits", s.commits.append({ key: op.key, value: op.value, ver: cv, at: s.clock }))
        .with("ver", cv).with("cache", s.cache.set(c, maxInt(s.cache.get(c), cv)))
        .with("staleCommits", s.staleCommits + (if (op.rv < s.ver) 1 else 0)))
    }
  }

  pure def reread(s: State, c: int): State = {
    val op = s.ops.get(c)
    val obs = valueAtVer(s, op.key, s.ver)
    val s1 = newVersion(s, c).with("rereads", s.rereads + 1)
    if (s.cfg.redecide and obs == op.expect)
      // The condition holds at the new version: decide again; the attempt runs at a new version.
      tick(setOp(s1, c, op.with("phase", "run").with("fresh", true)).with("redecisions", s1.redecisions + 1))
    else tick(setOp(s1, c, op.with("phase", "respond").with("result", "failed").with("obs", obs)))
  }

  pure def respond(s: State, c: int): State = {
    val op = s.ops.get(c)
    tick(setOp(s, c, idleOp).with("history", s.history.append({
      client: c, kind: op.kind, key: op.key, expect: op.expect, value: op.value, start: op.start, end: s.clock,
      obs: op.obs, result: op.result, readOk: op.readOk })))
  }

  // ------------------------------------------------------------------ invariant
  pure def entryOk(s: State, d: Done): bool =
    if (d.result == "ok") d.readOk and (d.kind == "put" or d.obs == d.expect)
    // A no-write success linearizes at an observation during this request, not at
    // settlement: a later concurrent writer need not be overwritten.
    else if (d.result == "noop") d.obs == d.value and (d.kind == "put" or d.obs == d.expect)
      and seenDuring(s, d.key, d.obs, d.start, d.end)
    else if (d.result == "failed") d.obs != d.expect and seenDuring(s, d.key, d.obs, d.start, d.end)
    else seenDuring(s, d.key, d.obs, d.start, d.end)
  val linearizable = state.history.foldl(true, (ok, d) => ok and entryOk(state, d))
  val inv = linearizable

  // ------------------------------------------------------------------ witnesses
  val optimized_no_write_witness = state.history.select(d => d.result == "noop").length() > 0
  val changed_fresh_decision_witness = state.changedFreshDecisions > 0
  val stale_commit_witness = state.staleCommits > 0
  val abort_retry_witness = state.aborts > 0 and state.history.select(d => d.result == "ok").length() > 1
  val reread_failed_witness = state.rereads > 0 and state.history.select(d => d.result == "failed").length() > 0
  val redecide_witness = state.redecisions > 0
  val get_after_write_witness = state.history.indices().exists(i => state.history.indices().exists(j =>
    state.history.nth(i).result == "ok" and state.history.nth(j).kind == "get"
      and state.history.nth(i).client != state.history.nth(j).client
      and state.history.nth(i).end < state.history.nth(j).start))

  // ------------------------------------------------------------------ actions
  action Start = {
    nondet c = CLIENTS.oneOf()
    nondet kind = KINDS.oneOf()
    nondet k = KEYS.oneOf()
    nondet e = EXPECTS.oneOf()
    nondet v = VALUES.oneOf()
    all { state.ops.get(c).phase == "idle", state.started < MAX_OPS, state' = start(state, c, kind, k, e, v) }
  }
  action Run = {
    nondet c = CLIENTS.oneOf()
    val op = state.ops.get(c)
    all { op.phase == "run", state' = if (op.kind == "get") consistentGet(state, c) else write(state, c) }
  }
  action Resolve = {
    nondet c = CLIENTS.oneOf()
    nondet optimize = Set(false, true).oneOf()
    all { state.ops.get(c).phase == "commit", state' = commit(state, c, optimize) }
  }
  action Reread = {
    nondet c = CLIENTS.oneOf()
    all { state.ops.get(c).phase == "reread", state' = reread(state, c) }
  }
  action Respond = {
    nondet c = CLIENTS.oneOf()
    all { state.ops.get(c).phase == "respond", state' = respond(state, c) }
  }
  // A background read-version request refreshes one client's cache.
  action Refresh = {
    nondet c = CLIENTS.oneOf()
    state' = tick(newVersion(state, c))
  }
  action step = any { Start, Run, Resolve, Reread, Respond, Refresh }

  // ------------------------------------------------------------------ tests
  pure def finish(s: State, c: int): State = 0.to(6).fold(s, (out, _) => {
    val p = out.ops.get(c).phase
    if (p == "run") { if (out.ops.get(c).kind == "get") consistentGet(out, c) else write(out, c) }
    else if (p == "commit") commit(out, c, false)
    else if (p == "reread") reread(out, c)
    else if (p == "respond") respond(out, c)
    else out
  })
  pure def afterPut(cfg: Config): State = finish(start(initialState(cfg), 2, "put", 1, 0, 1), 2)

  pure def afterAcknowledgedChange(cfg: Config): State = {
    val a = afterPut(cfg)
    val cachedA = newVersion(a, 1)
    finish(start(cachedA, 2, "put", 1, 0, 2), 2)
  }
  pure def optimizeOldConditional(cfg: Config): State = {
    val s = afterAcknowledgedChange(cfg)
    commit(write(start(s, 1, "cput", 1, 1, 1), 1), 1, true)
  }
  run sameObservedSetMustRerunFreshTest = {
    val optimized = optimizeOldConditional(design)
    val good = finish(optimized, 1)
    val bad = finish(optimizeOldConditional(design.with("stagedWriteDetector", true)), 1)
    all { assert(optimized.commits.length() == 2), assert(optimized.ver == 2),
      assert(good.history.nth(2).result == "failed"), assert(good.changedFreshDecisions == 1),
      assert(entryOk(good, good.history.nth(2))), assert(bad.history.nth(2).result == "noop"),
      assert(not(entryOk(bad, bad.history.nth(2)))) }
  }
  run optimizedUnconditionalSetRerunsAndCommitsCurrentDecisionTest = {
    val s = afterAcknowledgedChange(design)
    val optimized = commit(write(start(s, 1, "put", 1, 0, 1), 1), 1, true)
    val done = finish(optimized, 1)
    all { assert(optimized.ver == 2), assert(done.ver == 3),
      assert(done.changedFreshDecisions == 1), assert(done.history.nth(2).result == "ok"),
      assert(done.history.nth(2).obs == 2), assert(entryOk(done, done.history.nth(2))),
      assert(valueAtVer(done, 1, done.ver) == 1) }
  }
  run freshOptimizedNoopMayPrecedeConcurrentWriteTest = {
    val a = afterPut(design)
    val begun = start(a, 1, "cput", 1, 1, 1)
    val observed = write(setOp(begun, 1, begun.ops.get(1).with("fresh", true)), 1)
    val b = finish(start(observed, 2, "put", 1, 0, 2), 2)
    val done = respond(commit(b, 1, true), 1)
    all { assert(done.commits.length() == 2), assert(done.ver == 2),
      assert(valueAtVer(done, 1, done.ver) == 2), assert(done.history.nth(2).result == "noop"),
      assert(entryOk(done, done.history.nth(2))) }
  }
  run staleConditionFailureNeedsTheSecondReadTest = {
    val bad = finish(start(afterPut(design.with("rereadNoCommit", false)), 1, "cput", 1, 1, 2), 1)
    val good = finish(start(afterPut(design), 1, "cput", 1, 1, 2), 1)
    all { assert(bad.history.nth(1).result == "failed"), assert(not(entryOk(bad, bad.history.nth(1)))),
      assert(good.history.nth(1).result == "ok"), assert(good.redecisions == 1), assert(good.history.foldl(true, (ok, d) => ok and entryOk(good, d))) }
  }
  run cachedGetIsStaleAfterAcknowledgedWriteTest = {
    val bad = finish(start(afterPut(design.with("cachedGets", true)), 1, "get", 1, 0, 0), 1)
    val good = finish(start(afterPut(design), 1, "get", 1, 0, 0), 1)
    all { assert(bad.history.nth(1).obs == 0), assert(not(entryOk(bad, bad.history.nth(1)))),
      assert(good.history.nth(1).obs == 1), assert(entryOk(good, good.history.nth(1))) }
  }
  run cachedWriteAbortsOnANewerCommitTest = {
    val done = finish(start(afterPut(design), 1, "put", 1, 0, 2), 1)
    all { assert(done.aborts == 1), assert(done.history.nth(1).result == "ok"), assert(done.history.nth(1).readOk),
      assert(valueAtVer(done, 1, done.ver) == 2) }
  }
  run cachedWriteOnAnotherKeyCommitsWithoutAbortTest = {
    val done = finish(start(afterPut(design), 1, "put", 2, 0, 2), 1)
    all { assert(done.aborts == 0), assert(done.staleCommits == 1), assert(done.history.nth(1).readOk) }
  }
}
