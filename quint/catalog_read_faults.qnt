/// Stream retry fixture, not a provider implementation. A present, equal full
/// native catalog token permits reuse; mismatch reads the protected table stamp before reusing unchanged metadata.
/// Native atomic catalog publication, fresh stable snapshots and known-abort /
/// unknown classification are premises. Unrelated publications preserve stream
/// contents. The five/four schedule is the measured fixture, not every API path.
/// The three stream-read labels count probe-visible read waves abstractly; they
/// are not assertions about specific native keys or settlement calls.
/// Does not prove SDK conflicts, codecs, real stream response shape or fairness.
module catalog_read_faults_model {
  const BROKEN: str
  type Call = { attempt: int, index: int, snapshot: int, name: str }
  type Publication = { attempt: int, token: int, acknowledged: bool }
  type State = {
    token: int, cache: int, phase: str, baseline: int, target: int,
    attempt: int, snapshot: int, schedule: List[str], index: int,
    baselineCalls: List[Call], baselineAcknowledged: bool, calls: List[Call], fired: int, aborted: Set[int], publications: List[Publication],
    left: int, right: int, data: int, snapshotData: int, previousSnapshot: int,
    acknowledged: bool, unknownAttempt: int, remote: int, setup: int,
  }
  var s: State
  pure val warm = List("catalog-token", "stream-read-a", "stream-read-b", "stream-read-c")
  pure val cold = List("catalog-token", "table-stamp", "stream-read-a", "stream-read-b", "stream-read-c")
  action init = s' = { token: 1, cache: 1, phase: "baseline", baseline: 0, target: 0,
    attempt: 0, snapshot: 0, schedule: List(), index: 0, baselineCalls: List(), baselineAcknowledged: false, calls: List(), fired: 0,
    aborted: Set(), publications: List(), left: -1, right: -1, data: 0,
    snapshotData: 0, previousSnapshot: -1, acknowledged: false, unknownAttempt: 0,
    remote: 0, setup: 0 }
  // Setup and measurement are unarmed. Their native calls are executed, counted
  // and acknowledged before the baseline cache token can be published.
  action baselineSetup = all { s.phase == "baseline",
    s' = s.with("token", s.token + 1).with("phase", "baseline-ready") }
  action baselineBegin = all { s.phase == "baseline-ready",
    s' = s.with("snapshot", s.token).with("schedule", if (s.token == s.cache) warm else cold)
      .with("index", 0).with("phase", "baseline-read") }
  action baselineCall = all { s.phase == "baseline-read", s.index < s.schedule.length(),
    s' = s.with("baselineCalls", s.baselineCalls.append({ attempt: 0, index: s.index,
        snapshot: s.snapshot, name: s.schedule[s.index] }))
      .with("index", s.index + 1)
      .with("phase", if (s.index + 1 == s.schedule.length()) "baseline-ack" else "baseline-read") }
  action baselineAcknowledge = all { s.phase == "baseline-ack",
    s' = s.with("baselineAcknowledged", true).with("phase", "baseline-publish") }
  action baselinePublish = all { s.phase == "baseline-publish", s.baselineAcknowledged,
    s' = s.with("cache", s.snapshot).with("baseline", s.baselineCalls.length()).with("phase", "prepare") }
  action prepare(target: int): bool = all { s.phase == "prepare", target >= 0, target < s.baseline,
    s' = s.with("target", target).with("phase", "ready")
      .with("token", s.token + (if (BROKEN == "original") 0 else 1))
      .with("setup", if (BROKEN == "original") 0 else 1) }
  // Independent remote publishers need not invalidate this process's cache.
  action remotePublish = all { s.remote < 4,
    s' = s.with("token", s.token + 1).with("remote", s.remote + 1) }
  action writeData = all { s.phase == "retry", s.data == 0,
    s' = s.with("data", 1).with("token", s.token + 1) }
  action begin = {
    val snapshot = if (BROKEN == "stale" and s.attempt > 0) s.snapshot else s.token
    all { s.phase == "ready" or s.phase == "retry",
      s' = s.with("attempt", s.attempt + 1).with("snapshot", snapshot)
        .with("snapshotData", if (BROKEN == "stale" and s.attempt > 0) s.snapshotData else s.data)
        .with("schedule", if (snapshot == s.cache) warm else cold)
        .with("index", 0).with("phase", "read")
        .with("left", if (BROKEN == "partial") s.left else -1).with("right", -1) }
  }
  action readCall = {
    val hit = s.attempt == 1 and s.index == s.target
    val name = s.schedule[s.index]
    val abort = hit
    all { s.phase == "read", s.index < s.schedule.length(),
      s' = s.with("calls", s.calls.append({ attempt: s.attempt, index: s.index,
          snapshot: s.snapshot, name: name }))
        .with("index", s.index + 1).with("fired", s.fired + (if (hit) 1 else 0))
        .with("phase", if (abort) "retry" else if (s.index + 1 == s.schedule.length()) "ack" else "read")
        .with("aborted", if (abort) s.aborted.union(Set(s.attempt)) else s.aborted)
        .with("previousSnapshot", if (abort) s.snapshot else s.previousSnapshot)
        .with("left", if (name == "stream-read-a" and s.left == -1) s.snapshotData else s.left)
        .with("right", if (name == "stream-read-b") s.snapshotData else s.right)
        .with("cache", if (abort and BROKEN == "early") s.snapshot else s.cache)
        .with("publications", if (abort and BROKEN == "early") s.publications.append({
          attempt: s.attempt, token: s.snapshot, acknowledged: false }) else s.publications) }
  }
  action acknowledge = all { s.phase == "ack",
    s' = s.with("phase", "publish").with("acknowledged", true) }
  action publish = all { s.phase == "publish",
    s' = s.with("phase", "done").with("cache", s.snapshot)
      .with("publications", s.publications.append({ attempt: s.attempt,
        token: s.snapshot, acknowledged: s.acknowledged })) }
  action unknown = all { s.phase == "read",
    s' = s.with("unknownAttempt", s.attempt).with("phase", if (BROKEN == "unknown") "retry" else "failed") }
  action idle = all { Set("done", "failed").contains(s.phase), s' = s }
  action step = { nondet target = 0.to(4).oneOf()
    any { baselineSetup, baselineBegin, baselineCall, baselineAcknowledge, baselinePublish, prepare(target), remotePublish, writeData, begin, readCall, acknowledge, publish, unknown, idle } }
  val coverage = s.phase != "done" or (s.fired == 1 and s.attempt == 2)
  val completeSnapshot = s.phase != "done" or (s.left == s.snapshotData and s.right == s.snapshotData)
  val freshSnapshot = s.attempt < 2 or (s.snapshot >= s.previousSnapshot and s.snapshotData == s.data)
  val discardedAbort = s.publications.indices().forall(i => {
    val p = s.publications[i]; p.acknowledged and not(s.aborted.contains(p.attempt)) })
  val noUnknownReplay = s.unknownAttempt == 0 or s.attempt <= s.unknownAttempt
  val measuredBaseline = s.baseline == 0 or (s.baselineAcknowledged and s.baseline == s.baselineCalls.length())
  val inv = measuredBaseline and coverage and completeSnapshot and freshSnapshot and discardedAbort and noUnknownReplay
  val changingFiveToFour = s.baseline == 5 and s.attempt == 1 and s.schedule.length() == 4
  val firedLastTarget = s.phase == "done" and s.target == 4 and s.fired == 1 and s.attempt == 2
  val wholeReadRetry = s.phase == "done" and s.data == 1 and s.left == 1 and s.right == 1 and s.snapshot > s.previousSnapshot
  val repeatedPublicationProgress = s.phase == "done" and s.remote == 4 and s.fired == 1
  val unknownTerminal = s.phase == "failed" and s.unknownAttempt == s.attempt
  val abortedPublicationDiscarded = s.phase == "done" and s.aborted.size() == 1 and s.publications.length() == 1
  run baselineDeferredRetagTest = init.then(baselineSetup).then(baselineBegin)
    .then(baselineCall).then(baselineCall).then(remotePublish)
    .then(baselineCall).then(baselineCall).then(baselineCall)
    .expect(s.cache == 1 and not(s.baselineAcknowledged) and s.baselineCalls.length() == 5)
    .then(baselineAcknowledge).then(remotePublish).then(baselinePublish)
    .expect(s.cache == s.snapshot and s.cache < s.token and s.baseline == 5 and inv)
  run freshWholeReadTest = init.then(baselineSetup).then(baselineBegin)
    .then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall)
    .then(baselineAcknowledge).then(baselinePublish).then(prepare(4)).then(begin)
    .then(readCall).then(readCall).then(readCall).then(readCall).then(readCall)
    .then(writeData).then(begin).then(readCall).then(readCall).then(readCall).then(readCall).then(readCall)
    .then(acknowledge).then(publish).expect(wholeReadRetry and inv)
  run repeatedPublishersTest = init.then(baselineSetup).then(baselineBegin)
    .then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall)
    .then(baselineAcknowledge).then(baselinePublish).then(prepare(4))
    .then(remotePublish).then(begin).then(readCall).then(readCall)
    .then(remotePublish).then(readCall).then(readCall).then(readCall)
    .then(remotePublish).then(begin).then(readCall).then(readCall)
    .then(remotePublish).then(readCall).then(readCall).then(readCall)
    .then(acknowledge).then(publish).expect(repeatedPublicationProgress and inv)
  run unknownStopsTest = init.then(baselineSetup).then(baselineBegin)
    .then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall)
    .then(baselineAcknowledge).then(baselinePublish).then(prepare(4)).then(begin)
    .then(readCall).then(unknown).expect(unknownTerminal and inv)
  run lastTargetTest = init.then(baselineSetup).then(baselineBegin)
    .then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall).then(baselineCall)
    .then(baselineAcknowledge).then(baselinePublish).then(prepare(4)).then(begin)
    .then(readCall).then(readCall).then(readCall).then(readCall).then(readCall)
    .then(begin).then(readCall).then(readCall).then(readCall).then(readCall).then(readCall)
    .then(acknowledge).then(publish).expect(firedLastTarget and abortedPublicationDiscarded and inv)
}
module catalog_read_faults { import catalog_read_faults_model(BROKEN = "none").* }
module catalog_read_faults_original { import catalog_read_faults_model(BROKEN = "original").* }
module catalog_read_faults_stale { import catalog_read_faults_model(BROKEN = "stale").* }
module catalog_read_faults_partial { import catalog_read_faults_model(BROKEN = "partial").* }
module catalog_read_faults_early { import catalog_read_faults_model(BROKEN = "early").* }
module catalog_read_faults_unknown { import catalog_read_faults_model(BROKEN = "unknown").* }
