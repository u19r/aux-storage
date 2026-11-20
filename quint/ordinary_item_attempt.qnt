/// Two independently scheduled native attempts over one key. writeLog is a test
/// oracle, never persisted item metadata. Native protection is assumed: any
/// committed same-key write after a read aborts that attempt, including ABA.
/// Atomic commit publishes the staged primary, GSI and history together.
module ordinary_item_attempt_model {
  const BROKEN: str
  type Attempt = { phase: int, observed: int, readAt: int, operation: int, staged: int, retry: bool }
  type Entry = { oldImage: int, newImage: int, actualBefore: int, readAt: int, writeAt: int, ttl: bool }
  type State = {
    image: int, gsi: int, entries: List[Entry], writeLog: List[int], attempts: int -> Attempt,
    conflicts: int, abaConflicts: int, retries: int, absentDeletes: int,
    ttlRechecks: int, interference: int,
  }
  // 0 is absent, 1 is expired, 2 is an unexpired replacement.
  // operation -1 selects a previously found expired TTL candidate (image 1).
  pure val idle: Attempt = { phase: 0, observed: 0, readAt: 0, operation: 0, staged: 0, retry: false }
  var s: State
  action init = s' = {
    image: 0, gsi: 0, entries: List(), writeLog: List(), attempts: Map(0 -> idle, 1 -> idle),
    conflicts: 0, abaConflicts: 0, retries: 0, absentDeletes: 0, ttlRechecks: 0, interference: 0,
  }
  action readAttempt(id: int, operation: int): bool = {
    val a = s.attempts.get(id)
    all { a.phase == 0,
      s' = s.with("attempts", s.attempts.set(id, a.with("phase", 1)
        .with("observed", s.image).with("readAt", s.writeLog.length()).with("operation", operation))) }
  }
  action stageAttempt(id: int): bool = {
    val a = s.attempts.get(id)
    val result = if (a.operation == -1)
      (if (BROKEN == "ttl" or a.observed == 1) 0 else a.observed) else a.operation
    all { a.phase == 1,
      s' = s.with("attempts", s.attempts.set(id, a.with("phase", 2).with("staged", result))) }
  }
  action commitAttempt(id: int): bool = {
    val a = s.attempts.get(id)
    // Every log entry is a write to this one key. An empty delete adds none.
    val changed = s.writeLog.length() > a.readAt
    val rejected = changed and BROKEN != "conflict"
    val mutates = not(a.observed == 0 and a.staged == 0) and
      not(a.operation == -1 and a.staged == a.observed)
    val entry = { oldImage: if (BROKEN == "image") 0 else a.observed,
      newImage: a.staged, actualBefore: s.image, readAt: a.readAt,
      writeAt: s.writeLog.length(), ttl: a.operation == -1 }
    val result = if (rejected) s.with("conflicts", s.conflicts + 1)
        .with("abaConflicts", s.abaConflicts + (if (a.observed == 0 and s.image == 0) 1 else 0))
      else if (mutates) s.with("image", a.staged)
        .with("gsi", if (BROKEN == "gsi") s.gsi else a.staged)
        .with("writeLog", s.writeLog.append(a.staged)).with("entries", s.entries.append(entry))
      else s.with("absentDeletes", s.absentDeletes + (if (a.operation == 0 and a.observed == 0) 1 else 0))
    all { a.phase == 2,
      s' = result.with("attempts", s.attempts.set(id, a.with("phase", if (rejected) 4 else 3)))
        .with("interference", s.interference + (if (changed) 1 else 0))
        .with("retries", s.retries + (if (not(rejected) and a.retry) 1 else 0))
        .with("ttlRechecks", s.ttlRechecks + (if (not(rejected) and a.operation == -1 and a.observed == 2) 1 else 0)) }
  }
  action restartAttempt(id: int): bool = {
    val a = s.attempts.get(id)
    all { a.phase == 3 or a.phase == 4,
      s' = s.with("attempts", s.attempts.set(id, idle.with("retry", a.phase == 4))) }
  }
  action step = {
    nondet id = Set(0, 1).oneOf()
    nondet operation = Set(-1, 0, 1, 2).oneOf()
    any { readAttempt(id, operation), stageAttempt(id), commitAttempt(id), restartAttempt(id) }
  }
  val protectedAttempt = s.entries.indices().forall(i => { val e = s.entries[i] ; e.readAt == e.writeAt })
  val primaryMatchesGsi = s.image == s.gsi
  val exactImageChain = s.entries.indices().forall(i => {
    val e = s.entries[i]
    e.oldImage == e.actualBefore and e.oldImage == (if (i == 0) 0 else s.entries[i - 1].newImage)
  })
  val absentDeleteHasNoEntry = s.entries.indices().forall(i => { val e = s.entries[i] ; not(e.oldImage == 0 and e.newImage == 0) }) and
    s.entries.length() == s.writeLog.length()
  val ttlReplacementSurvives = s.entries.indices().forall(i => { val e = s.entries[i] ; not(e.ttl and e.actualBefore == 2 and e.newImage == 0) })
  val inv = all { protectedAttempt, primaryMatchesGsi, exactImageChain, absentDeleteHasNoEntry, ttlReplacementSurvives }
  val absent_delete_witness = s.absentDeletes > 0
  val interference_witness = s.interference > 0
  val delete_recreate_conflict_witness = s.conflicts > 0
  val aba_conflict_witness = s.abaConflicts > 0
  val stale_ttl_recheck_witness = s.ttlRechecks > 0
  val retry_image_chain_witness = s.retries > 0 and exactImageChain

  run givenAbsenceWhenOtherAttemptPutsAndDeletesThenStaleAttemptAbortsAndRetriesTest =
    init.then(readAttempt(0, 2)).then(stageAttempt(0))
      .then(readAttempt(1, 1)).then(stageAttempt(1)).then(commitAttempt(1)).then(restartAttempt(1))
      .then(readAttempt(1, 0)).then(stageAttempt(1)).then(commitAttempt(1))
      .then(commitAttempt(0)).expect(s.abaConflicts == 1 and s.image == 0 and inv)
      .then(restartAttempt(0)).then(readAttempt(0, 2)).then(stageAttempt(0)).then(commitAttempt(0))
      .expect(s.retries == 1 and s.image == 2 and inv)
  run givenExpiredCandidateWhenReplacementCommitsThenTtlRechecksReplacementTest =
    init.then(readAttempt(0, 1)).then(stageAttempt(0)).then(commitAttempt(0)).then(restartAttempt(0))
      .then(readAttempt(0, -1)).then(stageAttempt(0))
      .then(readAttempt(1, 2)).then(stageAttempt(1)).then(commitAttempt(1))
      .then(commitAttempt(0)).expect(s.conflicts == 1 and s.image == 2 and inv)
      .then(restartAttempt(0)).then(readAttempt(0, -1)).then(stageAttempt(0)).then(commitAttempt(0))
      .expect(s.image == 2 and s.ttlRechecks == 1 and s.retries == 1 and s.entries.length() == 2 and inv)
  run givenAbsenceWhenDeleteCommitsThenNoMutationOrEntryTest =
    init.then(readAttempt(0, 0)).then(stageAttempt(0)).then(commitAttempt(0))
      .expect(s.absentDeletes == 1 and s.writeLog.length() == 0 and s.entries.length() == 0 and inv)
}
module ordinary_item_attempt { import ordinary_item_attempt_model(BROKEN = "none").* }
module ordinary_item_missing_conflict { import ordinary_item_attempt_model(BROKEN = "conflict").* }
module ordinary_item_missing_image { import ordinary_item_attempt_model(BROKEN = "image").* }
module ordinary_item_stale_gsi { import ordinary_item_attempt_model(BROKEN = "gsi").* }
module ordinary_item_stale_ttl { import ordinary_item_attempt_model(BROKEN = "ttl").* }
