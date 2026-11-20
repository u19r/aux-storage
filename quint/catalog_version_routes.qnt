/// Fresh-only proposal. Native snapshots, atomic catalog+token publication and protected
/// metadata reads checked by the native commit oracle are premises, not native proof.
/// major/minor represent all 10 token bytes; minor changes with equal first 8 bytes.
/// Sequential requests with retained per-request pins, one table/schema and writer route.
/// Excludes item/stream ordering, codecs, SDK access, TTL, eviction and network recovery.
module catalog_version_routes_model {
  const BROKEN: str
  type Facts = { table: int, schema: int, route: int }
  type Catalog = { facts: Facts, major: int, minor: int }
  type Read = { snapshot: int, facts: Facts, reused: bool, disabled: bool, validated: bool }
  type Write = { revision: int, facts: Facts }
  type Publication = { acknowledged: bool, captured: int, generation: int }
  type State = {
    history: List[Catalog], cache: Catalog, hasCandidate: bool, phase: str, snapshot: int,
    observed: Facts, pin: int, firstPin: int, mutated: bool, checked: bool,
    generation: int, captured: int, acknowledged: bool, deferred: Catalog,
    reads: List[Read], writes: List[Write], publications: List[Publication],
    reused: int, refreshed: int, retagged: int, conflicts: int, disabled: int,
    suppressed: int, late: int, progress: int, outcome: str, unrelatedWrites: int,
  }
  var s: State
  pure val original = { table: 1, schema: 1, route: 1 }
  pure val initialCatalog = { facts: original, major: 1, minor: 0 }
  pure def current(x: State): Catalog = x.history[x.history.length() - 1]
  pure def sameToken(a: Catalog, b: Catalog): bool = a.major == b.major and
    (BROKEN == "truncate" or a.minor == b.minor)
  action init = s' = {
    history: List(initialCatalog), cache: initialCatalog, hasCandidate: true, phase: "idle", snapshot: 0,
    observed: original, pin: 0, firstPin: 0, mutated: false, checked: false,
    generation: 0, captured: 0, acknowledged: false, deferred: initialCatalog,
    reads: List(), writes: List(), publications: List(), reused: 0, refreshed: 0,
    retagged: 0, conflicts: 0, disabled: 0, suppressed: 0, late: 0, progress: 0, outcome: "", unrelatedWrites: 0,
  }
  // Durable facts change independently of what the cache believes or publishes.
  action change(kind: str): bool = {
    val old = current(s)
    val facts = if (kind == "table") old.facts.with("table", old.facts.table + 1)
      else if (kind == "schema") old.facts.with("schema", old.facts.schema + 1)
      else if (kind == "split") old.facts.with("route", old.facts.route + 1)
      else old.facts
    all { s.history.length() < 5,
      s' = s.with("history", s.history.append({ facts: facts, major: old.major,
        minor: old.minor + (if (BROKEN == "publisher") 0 else 1) })) }
  }
  action progressOnly = all { s.progress < 1, s' = s.with("progress", s.progress + 1) }
  action begin = all { Set("idle", "retry").contains(s.phase),
    s' = s.with("snapshot", s.history.length() - 1).with("phase", "resolve")
      .with("mutated", false).with("checked", false).with("acknowledged", false)
      .with("captured", s.generation) }
  pure def writtenFacts(f: Facts): Facts = f.with("schema", f.schema + 1).with("route", f.route + 1)
  action resolve = {
    val actual = s.history[s.snapshot]
    val reuse = s.hasCandidate and not(s.mutated) and sameToken(s.cache, actual)
    val facts = if (s.mutated) writtenFacts(actual.facts) else if (reuse) s.cache.facts else actual.facts
    val rejectsPin = s.pin != 0 and s.pin != facts.table
    all { s.phase == "resolve",
      s' = s.with("observed", facts).with("checked", true)
        .with("pin", if (s.pin == 0) facts.table else s.pin)
        .with("firstPin", if (s.firstPin == 0) actual.facts.table else s.firstPin)
        .with("phase", if (rejectsPin) "rejected" else "scan")
        .with("deferred", actual.with("facts", facts))
        .with("reused", s.reused + (if (reuse) 1 else 0))
        .with("refreshed", s.refreshed + (if (not(reuse)) 1 else 0))
        .with("retagged", s.retagged + (if (not(reuse) and s.cache.facts == actual.facts) 1 else 0)) }
  }
  // The ledger records the dependent schema/GSI scan, not just the final answer.
  action scan = {
    val early = BROKEN == "scan" and s.phase == "resolve"
    val facts = if (early) s.cache.facts else s.observed
    all { s.phase == "scan" or early,
      s' = s.with("phase", "settle").with("observed", facts)
        .with("reads", s.reads.append({ snapshot: s.snapshot, facts: facts,
          validated: s.checked, reused: early or (not(s.mutated) and sameToken(s.cache, s.history[s.snapshot])), disabled: s.mutated })) }
  }
  // This abstracts a protected/written local binding after catalog staging.
  // The old native token becomes unavailable; joined views use written facts.
  action mutateHere = all { s.phase == "settle", not(s.mutated),
    s' = s.with("mutated", true).with("checked", false).with("phase", "resolve")
      .with("disabled", s.disabled + 1) }
  // The token is a snapshot hint. A write conflicts only when the facts it
  // depends on (its table's stamp, writer routes) changed after its snapshot:
  // reused facts are protected by conflict ranges, read facts by conflict reads.
  // BROKEN "unprotected" trusts a reused token without protecting the stamps.
  action settle(outcome: str): bool = {
    val lastReused = s.reads.length() > 0 and s.reads[s.reads.length() - 1].reused
    val changedFacts = s.history[s.snapshot].facts != current(s).facts
    val protectedFacts = not(BROKEN == "unprotected" and lastReused)
    val conflict = (outcome == "write" or s.mutated) and changedFacts and protectedFacts
    val success = Set("read", "write").contains(outcome) and not(conflict)
    val commitsMetadata = success and s.mutated
    val history = if (commitsMetadata) s.history.append({ facts: writtenFacts(current(s).facts),
      major: current(s).major, minor: current(s).minor + 1 }) else s.history
    all { s.phase == "settle",
      s' = s.with("history", history).with("generation", s.generation + (if (commitsMetadata) 1 else 0))
        .with("hasCandidate", s.hasCandidate and not(commitsMetadata)).with("phase", if (conflict) "retry" else "publish")
        .with("acknowledged", success).with("outcome", if (conflict) "abort" else outcome)
        .with("conflicts", s.conflicts + (if (conflict) 1 else 0))
        .with("unrelatedWrites", s.unrelatedWrites + (if (success and outcome == "write"
          and s.snapshot != s.history.length() - 1) 1 else 0))
        .with("writes", if ((outcome == "write" or commitsMetadata) and success) s.writes.append({
          revision: history.length() - 1, facts: s.observed }) else s.writes) }
  }
  action knownConflict = all { s.phase == "settle",
    s' = s.with("phase", "retry").with("conflicts", s.conflicts + 1) }
  // Another local acknowledged mutation clears cache generation; a remote mutation
  // need not touch this process. Neither action rewrites an in-flight snapshot.
  action invalidateLocal = all { s.generation < 2,
    s' = s.with("generation", s.generation + 1).with("hasCandidate", false) }
  action publish = {
    val allowed = s.acknowledged and not(s.mutated) and
      (s.captured == s.generation or BROKEN == "generation")
    all { s.phase == "publish",
      s' = s.with("phase", "done").with("hasCandidate", allowed or s.hasCandidate).with("cache", if (allowed) s.deferred else s.cache)
        .with("publications", if (allowed) s.publications.append({ acknowledged: s.acknowledged,
          captured: s.captured, generation: s.generation }) else s.publications)
        .with("suppressed", s.suppressed + (if (not(allowed)) 1 else 0))
        .with("late", s.late + (if (allowed and s.snapshot < s.history.length() - 1) 1 else 0)) }
  }
  action finishRejected = all { s.phase == "rejected", s' = s.with("phase", "done") }
  action nextRequest = all { s.phase == "done",
    s' = s.with("phase", "idle").with("pin", 0).with("firstPin", 0) }
  action idle = all { s.phase == "done", s' = s }
  action step = any { change("table"), change("schema"), change("split"), change("unrelated"),
    progressOnly, begin, resolve, scan, mutateHere, settle("write"), settle("read"),
    settle("abort"), settle("unknown"), knownConflict, invalidateLocal, publish, finishRejected, nextRequest, idle }

  val scansUseSnapshot = s.reads.indices().forall(i => { val r = s.reads[i]; r.validated and r.facts == (if (r.disabled) writtenFacts(s.history[r.snapshot].facts) else s.history[r.snapshot].facts) })
  val writesUseCommitCatalog = s.writes.indices().forall(i => { val w = s.writes[i]; w.facts == s.history[w.revision].facts })
  val pinSurvives = s.firstPin == 0 or s.pin == s.firstPin
  val noReuseAfterMutation = s.reads.indices().forall(i => { val r = s.reads[i]; not(r.disabled and r.reused) })
  val publicationAllowed = s.publications.indices().forall(i => { val p = s.publications[i]; p.acknowledged and p.captured == p.generation })
  val inv = scansUseSnapshot and writesUseCommitCatalog and pinSurvives and noReuseAfterMutation and publicationAllowed
  val successfulReuse = s.phase == "done" and s.acknowledged and s.reused > 0 and s.reads.length() > 0
  val splitConflict = s.conflicts > 0 and current(s).facts.route > 1
  val orderOnlyChange = s.refreshed > 0 and current(s).minor > 0 and current(s).major == 1
  val sameAttemptDisabled = s.disabled > 0 and s.reads.length() > 1 and s.suppressed > 0
  val abortSuppressed = s.phase == "done" and s.outcome == "abort" and s.suppressed > 0
  val unknownSuppressed = s.phase == "done" and s.outcome == "unknown" and s.suppressed > 0
  val latePublication = s.late > 0
  val unrelatedRetag = s.retagged > 0
  val retainedPin = s.conflicts > 0 and s.phase == "done" and s.pin == 1 and current(s).facts.table > 1
  val unrelatedNoConflict = s.unrelatedWrites > 0 and s.conflicts == 0
  val progressUnchanged = s.progress == 1 and current(s).minor == 0 and s.reused > 0

  run reuseTest = init.then(begin).then(resolve).then(scan).then(settle("write")).then(publish).expect(successfulReuse and inv)
  run splitConflictTest = init.then(begin).then(resolve).then(scan).then(change("split"))
    .then(settle("write")).expect(splitConflict and inv)
  run schemaRefreshTest = init.then(change("schema")).then(begin).then(resolve).then(scan)
    .then(settle("read")).then(publish).expect(orderOnlyChange and inv)
  run sameAttemptTest = init.then(begin).then(resolve).then(scan).then(mutateHere).then(resolve)
    .then(scan).then(settle("read")).then(publish).expect(sameAttemptDisabled and inv)
  run abortTest = init.then(begin).then(resolve).then(scan).then(settle("abort")).then(publish).expect(abortSuppressed and inv)
  run unknownTest = init.then(begin).then(resolve).then(scan).then(settle("unknown")).then(publish).expect(unknownSuppressed and inv)
  run lateReadTest = init.then(begin).then(resolve).then(scan).then(change("table"))
    .then(settle("read")).then(publish).expect(latePublication and inv)
    .then(nextRequest).then(begin).then(resolve).then(scan).expect(s.refreshed > 0 and inv)
  run unrelatedTest = init.then(change("unrelated")).then(begin).then(resolve).then(scan)
    .then(settle("read")).then(publish).expect(unrelatedRetag and inv)
  run retainPinTest = init.then(begin).then(resolve).then(scan).then(knownConflict).then(change("table"))
    .then(begin).then(resolve).then(finishRejected).expect(retainedPin and inv)
  run unrelatedWriteTest = init.then(begin).then(resolve).then(scan).then(change("unrelated"))
    .then(settle("write")).expect(unrelatedNoConflict and inv)
  run progressTest = init.then(progressOnly).then(begin).then(resolve).expect(progressUnchanged and inv)
  run localPublicationTest = init.then(begin).then(resolve).then(scan).then(settle("read"))
    .then(invalidateLocal).then(publish).expect(s.suppressed == 1 and inv)
}
module catalog_version_routes { import catalog_version_routes_model(BROKEN = "none").* }
module catalog_version_routes_publisher { import catalog_version_routes_model(BROKEN = "publisher").* }
module catalog_version_routes_truncate { import catalog_version_routes_model(BROKEN = "truncate").* }
module catalog_version_routes_scan { import catalog_version_routes_model(BROKEN = "scan").* }
module catalog_version_routes_unprotected { import catalog_version_routes_model(BROKEN = "unprotected").* }
module catalog_version_routes_generation { import catalog_version_routes_model(BROKEN = "generation").* }
