/// N9 proposal only. Native premises: one transaction executes requests in wire
/// order; an execution error aborts it; later requests return 25P02; COMMIT of an
/// aborted transaction returns ROLLBACK and publishes no writes. Successful COMMIT
/// atomically publishes staged writes. Lost ACK permits either durable outcome.
/// These are PostgreSQL premises, not a proof of SQL/driver/native atomicity.
module sql_write_window_model {
  const BROKEN: str
  type State = {
    phase: str, kind: str, canceled: bool, prepared: bool,
    sent: List[int], executed: int, drained: int, replies: List[str],
    pending: Set[int], durable: Set[int], serverEffects: Set[int], aborted: bool,
    cause: str, commandComplete: bool, successfulMutations: Set[int], settlement: str, settlements: int,
    response: bool, publication: bool, replay: bool, returnedError: str, called: bool, extraIo: bool,
  }
  var s: State
  val kinds = Set("healthy", "count", "receiptCount", "permanent", "prepare", "local", "lostCommit", "lostAbort", "callback", "callbackConflict")
  // Independent logical request inventory: two accepted mutations and receipt.
  // This oracle never reads sent/pending/replies or the implementation classifier.
  val expectedEffects = if (Set("healthy", "callback", "callbackConflict", "lostCommit").contains(s.kind)) Set(0, 1, 2) else Set()
  val expectedCause = if (s.kind == "count") "stream-count"
    else if (s.kind == "receiptCount") "receipt-count"
    else if (s.kind == "permanent") "overflow" else ""
  action init = s' = {
    phase: "ready", kind: "", canceled: false, prepared: false,
    sent: List(), executed: 0, drained: 0, replies: List(),
    pending: Set(), durable: Set(), serverEffects: Set(), aborted: false, cause: "",
    commandComplete: false, successfulMutations: Set(), settlement: "", settlements: 0, response: false, publication: false,
    replay: false, returnedError: "", called: false, extraIo: false,
  }
  action prepare(kind: str): bool = all {
    s.phase == "ready",
    s' = s.with("kind", kind).with("prepared", not(Set("prepare", "local").contains(kind)))
      .with("phase", if (Set("prepare", "local").contains(kind) and BROKEN != "skip") "localAbort" else "dispatch"),
  }
  action localAbort = all {
    s.phase == "localAbort",
    s' = s.with("phase", "callback").with("settlement", "aborted").with("settlements", 1),
  }
  // All binds/result space/response must exist before even mutation zero.
  action dispatch = all {
    s.phase == "dispatch", s.sent.length() < 4,
    s' = s.with("sent", s.sent.append(s.sent.length()))
      .with("phase", if (s.sent.length() == 3) "running" else "dispatch")
      .with("publication", BROKEN == "publish"),
  }
  // Actual server ledger is separate from the logical request inventory above.
  // Request 1 is the middle asserted mutation. Request 2 is the receipt.
  action execute(commitArrives: bool): bool = {
    val i = s.executed
    val fault = (i == 1 and Set("count", "lostAbort", "permanent").contains(s.kind)) or
      (i == 2 and s.kind == "receiptCount")
    val fails = fault and not(BROKEN == "lateCount" and s.kind != "permanent")
    val reply = if (i == 3) (if (s.aborted) "ROLLBACK" else if (s.kind == "lostCommit" and not(commitArrives)) "LOST" else "COMMIT")
      else if (s.aborted) "25P02" else if (fails) (if (s.kind == "permanent") "22003" else "22012") else "OK"
    val omitted = BROKEN == "skip" and s.kind == "local" and i == 1
    all { Set("dispatch", "running").contains(s.phase), i < s.sent.length(),
      s' = s.with("executed", i + 1).with("replies", s.replies.append(reply))
        .with("aborted", s.aborted or fails)
        .with("pending", if (i < 3 and not(s.aborted or fails or omitted)) s.pending.union(Set(i)) else s.pending)
        .with("durable", if (i == 3 and reply == "COMMIT") s.pending else s.durable)
        .with("serverEffects", if (i == 3 and reply == "COMMIT") s.pending else s.serverEffects),
    }
  }
  // Replies can be delayed arbitrarily relative to later server execution.
  // tokio-postgres exposes generic command completion, not COMMIT/ROLLBACK text.
  // The server tags above are oracle facts only; client settlement cannot read them.
  action drain = {
    val nativeReply = if (s.drained == 3)
      (if (Set("lostCommit", "lostAbort").contains(s.kind)) "missing" else "command-complete")
      else s.replies[s.drained]
    val reply = nativeReply
    val mapped = if (reply == "22012") (if (BROKEN == "raw") "raw-division" else if (s.drained == 2) "receipt-count" else "stream-count")
      else if (reply == "22003") "overflow" else if (reply == "25P02") "failed-transaction" else ""
    all { s.phase == "running", s.drained < s.executed,
      s' = s.with("drained", s.drained + 1)
        .with("commandComplete", s.commandComplete or reply == "command-complete")
        .with("successfulMutations", if (s.drained < 3 and reply == "OK") s.successfulMutations.union(Set(s.drained)) else s.successfulMutations)
        .with("cause", if (mapped != "" and (s.cause == "" or BROKEN == "follower")) mapped else s.cause),
    }
  }
  action settle = {
    val result = if (not(s.commandComplete)) "unknown"
      else if (s.cause != "" and BROKEN != "ignoreError") "aborted"
      else if (s.successfulMutations == Set(0, 1, 2) or BROKEN == "ignoreError") "committed"
      else "unknown"
    all { s.phase == "running", s.executed == 4, (s.drained == 4 or (BROKEN == "drain" and s.drained == 2)),
      s' = s.with("settlement", result).with("settlements", 1).with("phase", "callback"),
    }
  }
  // Callback catches the native failure and returns an ordinary complete value.
  // A callback error/caller cancellation only suppresses publication, not settlement.
  action callback = {
    val accepts = s.settlement == "committed" and s.cause == "" and not(s.canceled) and not(Set("callback", "callbackConflict").contains(s.kind))
    // The callback may return a retryable Conflict after the terminal write has
    // already committed. The write owner must not expose that retry class.
    val returnedError = if (s.settlement == "unknown") "unknown"
      else if (s.kind == "callbackConflict")
        (if (s.settlement == "committed" and BROKEN != "callbackRetry") "postcommit-permanent" else "conflict")
      else if (s.kind == "callback") "permanent" else if (s.cause != "") "permanent" else ""
    all { s.phase == "callback",
      s' = s.with("phase", "done").with("returnedError", returnedError).with("response", accepts).with("publication", accepts)
        .with("replay", BROKEN == "replay" and s.settlement == "unknown"),
    }
  }
  action cancel = all {
    Set("dispatch", "running").contains(s.phase), not(s.canceled), s' = s.with("canceled", true),
  }
  action callAgain = all {
    s.phase == "done", not(s.called),
    s' = s.with("called", true).with("extraIo", BROKEN == "io")
      .with("settlements", if (BROKEN == "double") 2 else s.settlements),
  }
  action step = {
    nondet kind = kinds.oneOf()
    nondet commitArrives = Set(false, true).oneOf()
    nondet cancelChoice = 0.to(7).oneOf()
    any { prepare(kind), localAbort, dispatch, execute(commitArrives), drain, settle, callback, all { cancelChoice == 0, cancel }, callAgain }
  }
  val preparationBeforeDispatch = s.sent.length() == 0 or s.prepared
  val orderedRequests = s.sent.indices().forall(i => s.sent[i] == i)
  val logicalEffects = s.executed < 4 or s.durable == expectedEffects or
    (s.kind == "lostCommit" and s.durable == Set())
  val settlementPreservesServer = s.durable == s.serverEffects
  val knownAbortRollsBack = s.settlement != "aborted" or s.durable == Set()
  val causePreserved = s.drained < (if (s.kind == "receiptCount") 3 else 2) or expectedCause == "" or s.cause == expectedCause or
    (s.kind == "lostAbort" and s.cause == "stream-count")
  val fullDrain = s.settlement == "" or s.sent.length() == 0 or s.drained == 4
  val truthfulSettlement = s.settlement != "committed" or s.kind == "healthy" or Set("callback", "callbackConflict").contains(s.kind)
  val publicationAfterAck = not(s.response or s.publication) or
    (s.phase == "done" and s.settlement == "committed" and s.kind == "healthy" and not(s.canceled) and s.durable == Set(0, 1, 2))
  val acknowledgedSuccess = s.settlement != "committed" or
    (s.commandComplete and s.successfulMutations == Set(0, 1, 2) and s.cause == "")
  // Conflict is the reachable representative of the whole replay/control class.
  // Native tests must instantiate every concrete class; this model does not.
  val returnsKnownAbortRetryClass = Set("conflict", "fresh-validation", "capacity",
    "transaction-bytes", "unavailable", "logical-write-retry").contains(s.returnedError)
  val acknowledgedWriteCannotRetry = s.settlement != "committed" or not(returnsKnownAbortRetryClass)
  val unknownErrorWins = s.phase != "done" or s.settlement != "unknown" or s.returnedError == "unknown"
  val unknownNeverReplays = not(s.replay)
  val terminalOnce = s.settlements <= 1 and not(s.extraIo)
  val inv = preparationBeforeDispatch and orderedRequests and logicalEffects and knownAbortRollsBack and
    settlementPreservesServer and causePreserved and fullDrain and truthfulSettlement and acknowledgedSuccess and publicationAfterAck and acknowledgedWriteCannotRetry and unknownErrorWins and unknownNeverReplays and terminalOnce
  val healthyCommit = s.response and s.publication
  val middleRollback = s.phase == "done" and s.kind == "count" and s.cause == "stream-count" and s.durable == Set()
  val receiptRollback = s.phase == "done" and s.kind == "receiptCount" and s.cause == "receipt-count" and s.durable == Set()
  val permanentRetained = s.phase == "done" and s.kind == "permanent" and s.cause == "overflow"
  val prepareNoDispatch = s.phase == "done" and s.kind == "prepare" and s.sent.length() == 0
  val localNoDispatch = s.phase == "done" and s.kind == "local" and s.sent.length() == 0
  val unknownDurable = s.phase == "done" and s.kind == "lostCommit" and s.durable == Set(0, 1, 2) and not(s.response)
  val unknownAbsent = s.phase == "done" and s.kind == "lostCommit" and s.durable == Set() and not(s.response)
  val abortAckLost = s.phase == "done" and s.kind == "lostAbort" and s.settlement == "unknown"
  val callbackSuppressed = s.phase == "done" and s.kind == "callback" and s.durable == Set(0, 1, 2) and not(s.response)
  val callbackConflictStopped = s.phase == "done" and s.kind == "callbackConflict" and
    s.durable == Set(0, 1, 2) and s.settlement == "committed" and s.returnedError == "postcommit-permanent" and not(s.response or s.publication)
  val delayedReply = s.executed == 4 and s.drained == 0
  val canceledDrained = s.phase == "done" and s.canceled and s.drained == 4 and not(s.response or s.publication)
  action runServer = execute(true).then(execute(true)).then(execute(true)).then(execute(true))
  action drainAll = drain.then(drain).then(drain).then(drain)
  action complete = dispatch.then(dispatch).then(dispatch).then(dispatch).then(runServer).then(drainAll).then(settle).then(callback)
  run givenHealthyWhenCompleteThenPublishTest = init.then(prepare("healthy")).then(complete).expect(healthyCommit and inv)
  run givenCountFailureWhenCaughtThenRollbackTest = init.then(prepare("count")).then(complete).expect(middleRollback and inv)
  run givenReceiptCountWhenCaughtThenRollbackTest = init.then(prepare("receiptCount")).then(complete).expect(receiptRollback and inv)
  run givenPermanentWhenDrainedThenPreserveCauseTest = init.then(prepare("permanent")).then(complete).expect(permanentRetained and inv)
  run givenPrepareFailureWhenCompleteThenNoDispatchTest = init.then(prepare("prepare")).then(localAbort).then(callback).expect(prepareNoDispatch and inv)
  run givenLocalFailureWhenCompleteThenNoDispatchTest = init.then(prepare("local")).then(localAbort).then(callback).expect(localNoDispatch and inv)
  run givenLostAckWhenDurableThenNoPublishTest = init.then(prepare("lostCommit")).then(complete).expect(unknownDurable and inv)
  run givenLostAbortAckWhenCompleteThenUnknownTest = init.then(prepare("lostAbort")).then(complete).expect(abortAckLost and inv)
  run givenCallbackFailureWhenCommittedThenSuppressTest = init.then(prepare("callback")).then(complete).expect(callbackSuppressed and inv)
  run givenCallbackConflictAfterWriteCommitThenPreventRetryTest = init.then(prepare("callbackConflict"))
    .then(complete).expect(callbackConflictStopped and inv)
  run givenLostAckWhenNotDurableThenNoPublishTest = init.then(prepare("lostCommit"))
    .then(dispatch).then(dispatch).then(dispatch).then(dispatch).then(execute(true)).then(execute(true)).then(execute(true)).then(execute(false)).then(drainAll)
    .then(settle).then(callback).expect(unknownAbsent and inv)
  run givenCanceledWhenDrainedThenNoPublicationTest = init.then(prepare("healthy"))
    .then(dispatch).then(dispatch).then(dispatch).then(dispatch).then(cancel)
    .then(runServer).expect(delayedReply and inv).then(drainAll).then(settle).then(callback)
    .then(callAgain).expect(canceledDrained and s.settlements == 1 and not(s.extraIo) and inv)
}
module sql_write_window { import sql_write_window_model(BROKEN = "none").* }
module sql_write_window_lateCount { import sql_write_window_model(BROKEN = "lateCount").* }
module sql_write_window_skip { import sql_write_window_model(BROKEN = "skip").* }
module sql_write_window_ignoreError { import sql_write_window_model(BROKEN = "ignoreError").* }
module sql_write_window_raw { import sql_write_window_model(BROKEN = "raw").* }
module sql_write_window_follower { import sql_write_window_model(BROKEN = "follower").* }
module sql_write_window_replay { import sql_write_window_model(BROKEN = "replay").* }
module sql_write_window_publish { import sql_write_window_model(BROKEN = "publish").* }
module sql_write_window_io { import sql_write_window_model(BROKEN = "io").* }
module sql_write_window_drain { import sql_write_window_model(BROKEN = "drain").* }
module sql_write_window_double { import sql_write_window_model(BROKEN = "double").* }

module sql_write_window_callbackRetry { import sql_write_window_model(BROKEN = "callbackRetry").* }
