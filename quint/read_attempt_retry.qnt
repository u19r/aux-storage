/// One read, two items, two incarnations, at most three native attempts.
/// Native snapshots, typed errors and atomic settlement are premises. The
/// committed history is an oracle independent of response assembly. No cached
/// read version, native lock or provider SDK behavior is proved here.
module read_attempt_retry_model {
  const BROKEN: str
  type Snapshot = { incarnation: int, left: int, right: int }
  type Publication = { attempt: int, incarnation: int }
  type State = {
    history: List[Snapshot], phase: str, attempt: int, snapshot: int,
    pin: int, firstPin: int, candidate: int, left: int, right: int,
    publications: List[Publication], pending: List[Publication], successful: Set[int],
    aborted: Set[int], terminalFailures: Set[int], partialAborts: Set[int],
    refreshed: bool, rejected: bool, exhausted: bool, partialSnapshot: int, lastFailure: str,
  }
  var s: State
  action init = s' = {
    history: List({ incarnation: 1, left: 0, right: 0 }), phase: "ready",
    attempt: 0, snapshot: 0, pin: 0, firstPin: 0, candidate: 1,
    left: -1, right: -1, publications: List(), pending: List(), successful: Set(),
    aborted: Set(), terminalFailures: Set(), partialAborts: Set(),
    refreshed: false, rejected: false, exhausted: false, partialSnapshot: -1, lastFailure: "",
  }
  action write = {
    val current = s.history[s.history.length() - 1]
    all { s.history.length() < 5,
      s' = s.with("history", s.history.append(current.with("left", current.left + 1)
        .with("right", current.right + 1))) }
  }
  action recreate = all {
    s.history[s.history.length() - 1].incarnation == 1, s.history.length() < 5,
    s' = s.with("history", s.history.append({ incarnation: 2, left: 10, right: 10 })),
  }
  action begin = all {
    s.phase == "ready", s.attempt < 3,
    s' = s.with("phase", "observe").with("attempt", s.attempt + 1)
      .with("snapshot", s.history.length() - 1)
      .with("pending", if (BROKEN == "metadata") s.pending else List())
      .with("left", if (BROKEN == "partial") s.left else -1).with("right", -1),
  }
  action observe = {
    val incarnation = s.history[s.snapshot].incarnation
    val reject = s.pin != 0 and s.pin != incarnation
    all { s.phase == "observe",
      s' = s.with("phase", if (reject) "failed" else "left")
        .with("rejected", reject).with("pin", if (reject) s.pin else incarnation)
        .with("firstPin", if (s.firstPin == 0) incarnation else s.firstPin)
        .with("refreshed", s.refreshed or (s.pin == 0 and s.candidate != incarnation))
        .with("candidate", incarnation)
        .with("pending", if (reject) s.pending else
          s.pending.append({ attempt: s.attempt, incarnation: incarnation })) }
  }
  action readLeft = all {
    s.phase == "left",
    s' = s.with("phase", "right").with("left", if (s.left == -1)
      s.history[s.snapshot].left else s.left)
      .with("publications", if (BROKEN == "early")
        s.publications.append({ attempt: s.attempt, incarnation: s.pin }) else s.publications),
  }
  action readRight = all {
    s.phase == "right",
    s' = s.with("phase", "settle").with("right", s.history[s.snapshot].right),
  }
  action failAttempt(kind: str): bool = {
    val conflict = kind == "conflict"
    val replay = conflict or BROKEN == "unknown" and kind == "unknown"
    all { Set("observe", "left", "right", "settle").contains(s.phase),
      s' = s.with("phase", if (replay and s.attempt < 3) "retry" else "failed")
        .with("aborted", if (conflict) s.aborted.union(Set(s.attempt)) else s.aborted)
        .with("terminalFailures", if (not(conflict)) s.terminalFailures.union(Set(s.attempt)) else s.terminalFailures)
        .with("partialAborts", if (conflict and s.left != -1) s.partialAborts.union(Set(s.attempt)) else s.partialAborts)
        .with("partialSnapshot", if (conflict and s.left != -1) s.snapshot else s.partialSnapshot)
        .with("lastFailure", kind).with("exhausted", conflict and s.attempt == 3) }
  }
  action retry = all {
    s.phase == "retry",
    s' = s.with("phase", "ready").with("pin", if (BROKEN == "pin") 0 else s.pin),
  }
  action settle = all {
    s.phase == "settle",
    s' = s.with("phase", "done").with("successful", s.successful.union(Set(s.attempt)))
      .with("publications", s.publications.concat(s.pending)).with("pending", List()),
  }
  action idle = all { Set("done", "failed").contains(s.phase), s' = s }
  action step = {
    nondet kind = Set("conflict", "permanent", "unknown").oneOf()
    any { write, recreate, begin, observe, readLeft, readRight, failAttempt(kind), retry, settle, idle }
  }
  val completeSnapshot = s.phase != "done" or
    (s.left == s.history[s.snapshot].left and s.right == s.history[s.snapshot].right)
  val retainsPin = s.pin == 0 or s.pin == s.firstPin
  val settledPublications = s.publications.indices().forall(i => { val p = s.publications[i]
    s.successful.contains(p.attempt) and not(s.aborted.contains(p.attempt)) })
  val noTerminalReplay = s.terminalFailures.forall(a => s.attempt <= a)
  val inv = completeSnapshot and retainsPin and settledPublications and noTerminalReplay and s.attempt <= 3
  val newSnapshot = s.phase == "done" and s.partialAborts.size() > 0 and s.snapshot > s.partialSnapshot and s.left > 0
  val discardedMetadata = s.phase == "done" and s.partialAborts.size() > 0 and s.publications.length() == 1
  val pinnedRecreation = s.rejected and s.firstPin == 1 and s.history[s.snapshot].incarnation == 2
  val staleCandidate = s.refreshed and s.firstPin == 2
  val retryExhaustion = s.exhausted and s.attempt == 3
  val unknownOrPermanent = s.phase == "failed" and s.terminalFailures.contains(1) and s.attempt == 1

  val unknownTerminal = unknownOrPermanent and s.lastFailure == "unknown"
  val permanentTerminal = unknownOrPermanent and s.lastFailure == "permanent"

  run givenPartialReadWhenConflictThenNewCompleteSnapshotTest = init.then(begin).then(observe)
    .then(readLeft).then(failAttempt("conflict")).then(write).then(retry).then(begin).then(observe)
    .then(readLeft).then(readRight).then(settle).expect(newSnapshot and discardedMetadata and inv)
  run givenPinWhenRecreatedThenRejectTest = init.then(begin).then(observe).then(failAttempt("conflict"))
    .then(recreate).then(retry).then(begin).then(observe).expect(pinnedRecreation and inv)
  run givenStaleCandidateWhenFirstObservedThenRefreshTest = init.then(recreate).then(begin)
    .then(observe).expect(staleCandidate and inv)
  run givenThreeConflictsWhenBudgetSpentThenStopTest = init.then(begin).then(failAttempt("conflict"))
    .then(retry).then(begin).then(failAttempt("conflict")).then(retry).then(begin)
    .then(failAttempt("conflict")).expect(retryExhaustion and inv)
  run givenUnknownWhenSettledThenStopTest = init.then(begin).then(failAttempt("unknown")).expect(unknownOrPermanent and inv)
  run givenPermanentWhenSettledThenStopTest = init.then(begin).then(failAttempt("permanent")).expect(unknownOrPermanent and inv)
}
module read_attempt_retry { import read_attempt_retry_model(BROKEN = "none").* }
module read_attempt_partial { import read_attempt_retry_model(BROKEN = "partial").* }
module read_attempt_pin { import read_attempt_retry_model(BROKEN = "pin").* }
module read_attempt_early { import read_attempt_retry_model(BROKEN = "early").* }
module read_attempt_unknown { import read_attempt_retry_model(BROKEN = "unknown").* }

module read_attempt_metadata { import read_attempt_retry_model(BROKEN = "metadata").* }
