/// Proposal only: one request, at most three attempts, catalog 1 -> absent -> 2.
/// Native Fresh snapshots and mutation-bearing metadata conflict validation are premises.
/// Cached RV is the initial catalog snapshot; independent catalog actions may precede start.
/// Unknown/permanent outcomes, public schema guards, tokens and native traffic are excluded.
module cached_table_identity_model {
  const BROKEN: str
  type Event = { at: int, generation: int }
  type Commit = { at: int, target: int }
  type Observation = { at: int, generation: int }
  type State = {
    clock: int, catalog: List[Event], phase: str, start: int, attempt: int,
    fresh: bool, observed: int, observedVersion: int, pin: int,
    freshReads: List[Observation], commits: List[Commit], result: str,
    conflicts: int, reruns: int, discarded: int, retained: int,
  }
  var s: State
  action init = s' = {
    clock: 0, catalog: List({ at: 0, generation: 1 }), phase: "idle", start: -1,
    attempt: 0, fresh: false, observed: -1, observedVersion: -1, pin: 0,
    freshReads: List(), commits: List(), result: "", conflicts: 0, reruns: 0,
    discarded: 0, retained: 0,
  }
  pure def current(x: State): int = x.catalog[x.catalog.length() - 1].generation
  pure def firstFresh(x: State): int = x.freshReads.foldl(0, (pin, o) =>
    if (pin == 0 and o.generation != 0) o.generation else pin)
  action deleteTable = all {
    current(s) == 1,
    s' = s.with("clock", s.clock + 1).with("catalog", s.catalog.append({ at: s.clock + 1, generation: 0 })),
  }
  action createReplacement = all {
    current(s) == 0,
    s' = s.with("clock", s.clock + 1).with("catalog", s.catalog.append({ at: s.clock + 1, generation: 2 })),
  }
  action begin = all {
    s.phase == "idle",
    s' = s.with("clock", s.clock + 1).with("start", s.clock + 1).with("phase", "ready"),
  }
  // Optional authoritative preparation occurs in this request, before any Cached attempt.
  action prepareFresh = all {
    s.phase == "ready", s.attempt == 0, s.freshReads.length() == 0, current(s) != 0,
    s' = s.with("clock", s.clock + 1).with("pin", current(s))
      .with("freshReads", s.freshReads.append({ at: s.clock + 1, generation: current(s) })),
  }
  action observe = {
    val generation = if (s.fresh) current(s) else 1
    val reads = if (s.fresh) s.freshReads.append({ at: s.clock + 1, generation: generation }) else s.freshReads
    // Cached identity mismatches are provisional too: never publish their rejection.
    val reject = s.fresh and (generation == 0 or (s.pin != 0 and s.pin != generation))
    val pin = if (s.fresh or BROKEN == "promote") {
      if (s.pin == 0 and generation != 0) generation else s.pin
    } else s.pin
    all { s.phase == "ready", s.attempt < 3,
      s' = s.with("clock", s.clock + 1).with("attempt", s.attempt + 1)
        .with("observed", generation).with("observedVersion", if (s.fresh) s.catalog.length() else 1)
        .with("freshReads", reads).with("pin", pin)
        .with("result", if (reject) "missing" else "")
        .with("phase", if (reject) "publish" else "decide") }
  }
  pure def retry(x: State): State = x.with("phase", if (x.attempt < 3) "ready" else "exhausted")
    .with("fresh", true).with("observed", -1).with("observedVersion", -1)
    .with("discarded", x.discarded + (if (not(x.fresh)) 1 else 0))
    .with("retained", x.retained + (if (x.pin != 0 and x.fresh) 1 else 0))
    .with("pin", if (BROKEN == "discard") 0 else x.pin)
  action noMutation = all {
    s.phase == "decide",
    s' = (if (s.fresh) s.with("phase", "publish").with("result", "noop")
      else retry(s).with("reruns", s.reruns + 1)).with("clock", s.clock + 1),
  }
  action commitMutation = {
    val conflict = s.observedVersion != s.catalog.length() or s.observed != current(s) or
      (s.pin != 0 and s.pin != s.observed)
    all { s.phase == "decide",
      s' = (if (conflict and BROKEN != "unchecked") retry(s).with("conflicts", s.conflicts + 1)
        else s.with("phase", "publish").with("result", "written")
          .with("pin", if (s.pin == 0) s.observed else s.pin)
          .with("commits", s.commits.append({ at: s.clock + 1, target: s.observed })))
        .with("clock", s.clock + 1) }
  }
  // A known item conflict can happen without a catalog change. Fresh pins survive it.
  action knownConflict = all {
    s.phase == "decide",
    s' = retry(s).with("clock", s.clock + 1).with("conflicts", s.conflicts + 1),
  }
  action publish = all { s.phase == "publish", s' = s.with("phase", "done").with("clock", s.clock + 1) }
  action idle = all { Set("done", "exhausted").contains(s.phase), s' = s }
  action step = any { deleteTable, createReplacement, begin, prepareFresh, observe,
    noMutation, commitMutation, knownConflict, publish, idle }

  // Independent observation/commit ledgers provide the oracle, not the implementation pin.
  val writesCurrent = s.commits.indices().forall(i => { val c = s.commits[i]; c.target != 0 and c.target == s.catalog.foldl(1, (g, e) => if (e.at <= c.at) e.generation else g) })
  val noRetarget = s.commits.indices().forall(i => firstFresh(s) == 0 or s.commits[i].target == firstFresh(s))
    and (firstFresh(s) == 0 or s.pin == firstFresh(s))
  val noFalseMissing = s.result != "missing" or s.observed == 0 or
    (firstFresh(s) != 0 and firstFresh(s) != s.observed)
  val observationsWithinRequest = s.freshReads.indices().forall(i => { val o = s.freshReads[i]; o.at >= s.start and
    s.catalog.foldl(1, (g, e) => if (e.at <= o.at) e.generation else g) == o.generation })
  val pinHasAuthority = s.pin == 0 or s.freshReads.indices().exists(i => s.freshReads[i].generation == s.pin)
    or s.commits.indices().exists(i => s.commits[i].target == s.pin)
  val inv = writesCurrent and noRetarget and noFalseMissing and observationsWithinRequest and pinHasAuthority
  val replacementBeforeStart = s.catalog.indices().exists(i => s.catalog[i].generation == 2 and s.catalog[i].at < s.start)
  val conflictThenReplacement = s.phase == "done" and s.result == "written" and s.pin == 2 and
    replacementBeforeStart and s.conflicts > 0 and s.discarded > 0
  val noMutationThenReplacement = s.phase == "done" and s.pin == 2 and replacementBeforeStart and s.reruns > 0
  val preparedThenReject = s.phase == "done" and s.result == "missing" and firstFresh(s) == 1 and s.observed == 2
  val cachedCommit = s.phase == "done" and s.result == "written" and not(s.fresh) and s.pin == 1
  val discardedProvisional = s.discarded > 0 and s.observed == -1 and s.pin == 0
  val retainedFreshPin = s.retained > 0 and s.pin == 1
  val realMissing = s.phase == "done" and s.result == "missing" and s.observed == 0

  run preRequestConflictTest = init.then(deleteTable).then(createReplacement).then(begin).then(observe)
    .then(commitMutation).then(observe).then(commitMutation).then(publish).expect(conflictThenReplacement and inv)
  run preRequestNoMutationTest = init.then(deleteTable).then(createReplacement).then(begin).then(observe)
    .then(noMutation).then(observe).then(noMutation).then(publish).expect(noMutationThenReplacement and inv)
  run preparedRecreationTest = init.then(begin).then(prepareFresh).then(deleteTable).then(createReplacement)
    .then(observe).then(commitMutation).then(observe).then(publish).expect(preparedThenReject and inv)
  run matchingCachedCommitTest = init.then(begin).then(observe).then(commitMutation).then(publish).expect(cachedCommit and inv)
  run discardProvisionalTest = init.then(begin).then(observe).then(knownConflict).expect(discardedProvisional and inv)
  run retainFreshConflictTest = init.then(begin).then(prepareFresh).then(observe).then(knownConflict)
    .then(observe).then(knownConflict).expect(retainedFreshPin and inv)
  run realMissingTest = init.then(begin).then(observe).then(noMutation).then(deleteTable)
    .then(observe).then(publish).expect(realMissing and inv)
}
module cached_table_identity { import cached_table_identity_model(BROKEN = "none").* }
module cached_table_identity_promote { import cached_table_identity_model(BROKEN = "promote").* }
module cached_table_identity_discard { import cached_table_identity_model(BROKEN = "discard").* }
module cached_table_identity_unchecked { import cached_table_identity_model(BROKEN = "unchecked").* }
