/// N8 protocol candidate. Native shared/exclusive catalog locks, key locks held
/// through commit, atomic checkpoint CAS plus deletes, and durable commit are
/// premises. Locator order/SQL query plans and native drivers are separate proof.
module expiry_pages_model {
  const BROKEN: str
  type Page = { phase: int, key: int, previous: int, generation: int, incarnation: int, scanned: int, remove: bool }
  type Commit = { previous: int, actual: int, generation: int, currentGeneration: int, incarnation: int, currentIncarnation: int, scanned: int, before: int, after: int }
  type State = {
    items: int -> int, metadata: int -> int, filled: Set[int], fillPosition: int,
    enabled: bool, generation: int, incarnation: int, checkpoint: int, pages: int -> Page,
    locks: int -> int, commits: List[Commit], conflicts: int, renewals: int,
    staleCommands: int, policyRaces: int, sweepEnds: int, activations: int,
    lostReply: bool, recoveries: int, recoveredEnabled: bool,
  }
  // Item image 0 absent, 1 expired, 2 renewed. Metadata is the image's expiry class.
  pure val idle: Page = { phase: 0, key: 0, previous: 0, generation: 0, incarnation: 0, scanned: 0, remove: false }
  var s: State
  action init = s' = {
    items: Map(0 -> 1, 1 -> 1), metadata: Map(0 -> 0, 1 -> 0), filled: Set(), fillPosition: 0,
    enabled: false, generation: 0, incarnation: 0, checkpoint: 0, pages: Map(0 -> idle, 1 -> idle),
    locks: Map(0 -> -1, 1 -> -1), commits: List(), conflicts: 0, renewals: 0,
    staleCommands: 0, policyRaces: 0, sweepEnds: 0, activations: 0,
    lostReply: false, recoveries: 0, recoveredEnabled: false,
  }
  val readers = Set(0, 1).filter(id => s.pages.get(id).phase > 0)
  action scanPage(id: int, key: int, expectedGeneration: int, expectedIncarnation: int): bool = {
    val p = s.pages.get(id)
    all { p.phase == 0,
      s' = if (expectedGeneration != s.generation or expectedIncarnation != s.incarnation)
        s.with("staleCommands", s.staleCommands + 1)
      else s.with("pages", s.pages.set(id, { phase: 1, key: key,
        previous: s.checkpoint, generation: s.generation, incarnation: s.incarnation,
        scanned: s.items.get(key), remove: false })) }
  }
  action lockAndRead(id: int): bool = {
    val p = s.pages.get(id)
    all { p.phase == 1, s.locks.get(p.key) == -1,
      s' = s.with("locks", s.locks.set(p.key, id)).with("pages", s.pages.set(id,
        p.with("phase", 2).with("remove", (if (BROKEN == "expiry") p.scanned else s.items.get(p.key)) == 1))) }
  }
  action commitPage(id: int): bool = {
    val p = s.pages.get(id)
    val conflict = p.previous != s.checkpoint and BROKEN != "checkpoint"
    val before = s.items.get(p.key)
    val after = if (p.remove) 0 else before
    val entry = { previous: p.previous, actual: s.checkpoint, generation: p.generation,
      currentGeneration: s.generation, incarnation: p.incarnation, currentIncarnation: s.incarnation, scanned: p.scanned, before: before, after: after }
    val result = if (conflict) s.with("conflicts", s.conflicts + 1)
      else s.with("items", s.items.set(p.key, after))
        .with("metadata", s.metadata.set(p.key, if (p.remove) 0 else s.metadata.get(p.key)))
        .with("checkpoint", p.previous + 1).with("commits", s.commits.append(entry))
    all { p.phase == 2,
      s' = result.with("pages", s.pages.set(id, idle)).with("locks", s.locks.set(p.key, -1)) }
  }
  action renewItem(key: int): bool = all {
    s.locks.get(key) == -1, s.items.get(key) == 1,
    s' = s.with("items", s.items.set(key, 2))
      .with("metadata", s.metadata.set(key, 2)).with("renewals", s.renewals + 1),
  }
  action changePolicy = all {
    s.generation == 0, readers.size() == 0 or BROKEN == "policy",
    s' = s.with("generation", 1).with("enabled", false).with("filled", Set())
      .with("fillPosition", 0).with("metadata", Map(0 -> 0, 1 -> 0))
      .with("policyRaces", s.policyRaces + (if (readers.size() == 0) 0 else 1)),
  }
  // Identity change abstracts a dropped/recreated table. The modeled checkpoint
  // is a cumulative oracle; native job rows are separately scoped by incarnation.
  action replaceTable = all {
    s.incarnation == 0, readers.size() == 0,
    s' = s.with("incarnation", 1).with("enabled", false).with("filled", Set())
      .with("fillPosition", 0).with("metadata", Map(0 -> 0, 1 -> 0)),
  }
  // This is the existing exclusive protected fill owner. Current key images,
  // metadata, durable fill progress and final activation publish together.
  action fillItem(key: int, loseReply: bool): bool = {
    val filled = s.filled.union(Set(key))
    val complete = filled.size() == 2
    all { readers.size() == 0, not(s.filled.contains(key)),
      s' = s.with("metadata", s.metadata.set(key, s.items.get(key)))
        .with("filled", filled).with("fillPosition", filled.size())
        .with("enabled", complete and BROKEN != "publication")
        .with("activations", s.activations + (if (complete) 1 else 0))
        .with("lostReply", loseReply) }
  }
  action finishSweep = all {
    s.sweepEnds == 0,
    s' = s.with("sweepEnds", 1).with("enabled", s.enabled or BROKEN == "activation"),
  }
  // Crash discards every uncommitted page. A lost response never rolls back a
  // durable fill/activation result; reopening observes that published state.
  action reopen = all {
    s.lostReply,
    s' = s.with("pages", Map(0 -> idle, 1 -> idle)).with("locks", Map(0 -> -1, 1 -> -1))
      .with("lostReply", false).with("recoveries", s.recoveries + 1)
      .with("recoveredEnabled", s.enabled),
  }
  action step = {
    nondet id = Set(0, 1).oneOf()
    nondet key = Set(0, 1).oneOf()
    nondet generation = Set(0, 1).oneOf()
    nondet incarnation = Set(0, 1).oneOf()
    nondet loseReply = Set(false, true).oneOf()
    any { scanPage(id, key, generation, incarnation), lockAndRead(id), commitPage(id),
      renewItem(key), changePolicy, replaceTable, fillItem(key, loseReply), finishSweep, reopen }
  }
  val checkpointMatchesCommit = s.commits.indices().forall(i => { val e = s.commits[i]; e.previous == e.actual })
    and s.checkpoint == s.commits.length()
  val replacementSurvives = s.commits.indices().forall(i => { val e = s.commits[i]; not(e.before == 2 and e.after == 0) })
  val policyProtected = s.commits.indices().forall(i => { val e = s.commits[i]; e.generation == e.currentGeneration and e.incarnation == e.currentIncarnation })
  val activationRequiresFill = not(s.enabled) or s.filled == Set(0, 1)
  val fillPublicationAtomic = s.enabled == (s.fillPosition == 2)
  val fillUsesCurrentImage = s.filled.forall(key => s.metadata.get(key) == s.items.get(key))
  val inv = all { checkpointMatchesCommit, replacementSurvives, policyProtected,
    activationRequiresFill, fillPublicationAtomic, fillUsesCurrentImage }
  val concurrentPages = readers.size() == 2 and
    s.pages.get(0).previous == s.pages.get(1).previous
  val checkpointConflict = s.conflicts > 0
  val renewedCandidate = s.renewals > 0 and s.commits.indices().exists(i => s.commits[i].scanned == 1 and s.commits[i].before == 2 and s.commits[i].after == 2)
  val stalePolicyRejected = s.staleCommands > 0 and s.generation == 1
  val activated = s.enabled and s.activations > 0
  val recoveredActivation = s.recoveries > 0 and s.recoveredEnabled

  run concurrentCheckpointTest = init.then(scanPage(0, 0, 0, 0)).then(scanPage(1, 1, 0, 0))
    .then(lockAndRead(0)).then(lockAndRead(1)).then(commitPage(0)).then(commitPage(1))
    .expect(s.conflicts == 1 and s.items.get(0) == 0 and s.items.get(1) == 1 and inv)
  run renewalAfterLocatorTest = init.then(scanPage(0, 0, 0, 0)).then(renewItem(0))
    .then(lockAndRead(0)).then(commitPage(0)).expect(renewedCandidate and inv)
  run sweepCannotActivateTest = init.then(finishSweep)
    .expect(not(s.enabled) and s.filled.size() == 0 and inv)
  run stalePolicyTest = init.then(changePolicy).then(scanPage(0, 0, 0, 0))
    .expect(stalePolicyRejected and s.pages.get(0) == idle and inv)
  run staleIncarnationTest = init.then(replaceTable).then(scanPage(0, 0, 0, 0))
    .expect(s.staleCommands == 1 and s.pages.get(0) == idle and inv)
  run finalFillLostReplyTest = init.then(fillItem(0, false)).then(fillItem(1, true))
    .then(reopen).expect(recoveredActivation and s.fillPosition == 2 and inv)
}
module expiry_pages { import expiry_pages_model(BROKEN = "none").* }
module expiry_pages_missing_checkpoint { import expiry_pages_model(BROKEN = "checkpoint").* }
module expiry_pages_stale_expiry { import expiry_pages_model(BROKEN = "expiry").* }
module expiry_pages_sweep_activation { import expiry_pages_model(BROKEN = "activation").* }
module expiry_pages_missing_policy { import expiry_pages_model(BROKEN = "policy").* }
module expiry_pages_partial_publication { import expiry_pages_model(BROKEN = "publication").* }
