/// Bounded terminal SQL read, distinct from generic read-attempt retry.
/// Premises: one immutable native snapshot; ordered protocol requests execute
/// atomically within that snapshot; native acknowledgements report commit,
/// abort or unknown honestly. No driver, SQL optimizer, cancellation supervisor,
/// connection pool, snapshot isolation or PostgreSQL protocol is proved here.
module sql_read_window_model {
  const BROKEN: str
  type State = {
    phase: str, kind: str, dispatched: List[str], drained: int,
    server: str, settlement: str, settlements: int, rejected: bool,
    prefixLedger: int, observation: int, firstPin: int, pin: int,
    callback: str, outcome: str, fresh: bool, freshSettled: bool,
    freshStamp: int, replay: bool, response: bool, cache: bool,
    ioAfterTerminal: int, abandoned: bool, blockedFinishingCall: bool,
    internalDropped: bool, released: str, calledAfterTerminal: bool,
  }
  var s: State
  val requests = List("BEGIN", "prefix", "item0", "item1", "COMMIT")
  val schema = Set("schema", "prepare").contains(s.kind)
  val nativeFailure = Set("schema", "prefix").contains(s.kind)
  val oracleServer = if (s.kind == "unknown") "unknown"
    else if (nativeFailure or s.kind == "prepare") "aborted" else "committed"
  val oracleAccepted = s.kind == "success" and not(s.abandoned)
  action init = s' = {
    phase: "ready", kind: "", dispatched: List(), drained: 0,
    server: "", settlement: "", settlements: 0, rejected: false,
    prefixLedger: 0, observation: 0, firstPin: 0, pin: 0,
    callback: "", outcome: "", fresh: false, freshSettled: false,
    freshStamp: 0, replay: false, response: false, cache: false,
    ioAfterTerminal: 0, abandoned: false, blockedFinishingCall: false,
    internalDropped: false, released: "held", calledAfterTerminal: false,
  }
  // Candidate stamp 1 is only a comparison baseline, never an observation.
  action prepare(kind: str): bool = all {
    s.phase == "ready",
    s' = s.with("kind", kind).with("phase", if (kind == "prepare") "prepareAbort" else "dispatch")
      .with("pin", if (BROKEN == "candidate") 1 else 0),
  }
  // Prepare-all failure dispatched no prefix; failed settlement is still explicit.
  action abortPreparation = all {
    s.phase == "prepareAbort",
    s' = s.with("server", "aborted").with("settlement", "aborted")
      .with("settlements", 1).with("rejected", true).with("phase", "callback"),
  }
  // Poll each future in protocol order before awaiting any reply.
  action dispatch = all {
    s.phase == "dispatch", s.dispatched.length() < requests.length(),
    s' = s.with("dispatched", s.dispatched.append(requests[s.dispatched.length()]))
      .with("phase", if (s.dispatched.length() == 4) "drain" else "dispatch"),
  }
  // Ledger records actual server observation separately from retained envelope.
  // A middle schema failure leaves the actual prefix available. Followers drain.
  action drain = {
    val prefix = s.drained == 1 and s.kind != "prefix"
    val failure = (s.drained == 1 and s.kind == "prefix") or
      (s.drained == 2 and Set("schema", "decode").contains(s.kind))
    all { s.phase == "drain", s.drained < 5,
      s' = s.with("drained", s.drained + 1)
        .with("prefixLedger", if (prefix) 1 else s.prefixLedger)
        .with("observation", if (prefix and BROKEN != "prefix") 1 else s.observation)
        .with("rejected", s.rejected or failure)
        .with("phase", if (s.drained == 4 or (BROKEN == "drain" and s.drained == 2)) "settle" else "drain"),
    }
  }
  action settle = all {
    s.phase == "settle",
    s' = s.with("server", oracleServer).with("settlement", oracleServer)
      .with("settlements", s.settlements + 1).with("phase", "callback"),
  }
  // Shared confirmation pins every completed original observation, even on abort.
  // Callback Ok can carry SchemaFailure or Complete; neither decides settlement.
  action callback = {
    val value = if (s.kind == "callback" or s.abandoned) "error" else if (schema) "schema" else "complete"
    val accepted = s.settlement == "committed" and not(s.rejected) and value == "complete"
    val outcome = if (accepted or (BROKEN == "abort" and s.settlement == "aborted") or
        (BROKEN == "decode" and s.kind == "decode")) "completed"
      else if (s.settlement == "aborted" and value != "error") "aborted" else "error"
    all { s.phase == "callback",
      s' = s.with("callback", value).with("outcome", outcome)
        .with("rejected", if (BROKEN == "latch") false else s.rejected)
        .with("pin", if (s.observation == 1) 1 else s.pin)
        .with("firstPin", s.prefixLedger)
        .with("phase", "accepted"),
    }
  }
  // A Fresh read is a new attempt, after full drain and known failed settlement.
  action fresh(stamp: int): bool = all {
    (s.phase == "accepted" and s.callback == "schema" and s.outcome == "aborted") or
      (BROKEN == "early" and s.phase == "drain" and schema),
    not(s.fresh),
    s' = s.with("fresh", true).with("freshStamp", stamp).with("phase", "freshSettle")
      .with("pin", if (s.pin == 0 or BROKEN == "pin") stamp else s.pin),
  }
  action settleFresh = all {
    s.phase == "freshSettle",
    s' = s.with("freshSettled", true).with("phase", "done")
      .with("replay", s.freshStamp != 1 and s.firstPin == 0),
  }
  action finish = all {
    s.phase == "accepted",
    s' = s.with("phase", "done")
      .with("released", if (s.settlement == "unknown") "discarded" else "reusable")
      .with("response", s.outcome == "completed")
      .with("cache", s.outcome == "completed")
      .with("replay", BROKEN == "unknown" and s.settlement == "unknown"),
  }
  // Hostile callers cannot issue native work or duplicate settlement after terminal.
  action callAfterTerminal = all {
    s.phase == "done",
    s' = s.with("calledAfterTerminal", true)
      .with("ioAfterTerminal", if (BROKEN == "io") 1 else 0)
      .with("settlements", if (BROKEN == "double") s.settlements + 1 else s.settlements),
  }
  // Continued-owner cancellation: a live owner may still drain and settle.
  // This is a permitted path, not proof that an internal future retains an owner.
  action abandon = all {
    s.phase == "drain", not(s.abandoned), s' = s.with("abandoned", true),
  }
  action callDuringFinishing = all {
    s.phase == "drain", s.abandoned,
    s' = s.with("blockedFinishingCall", true)
      .with("ioAfterTerminal", if (BROKEN == "finishing") 1 else 0),
  }
  // Internal future loss: discard the client without waiting for application
  // replies. Unknown is a local classification, not a fabricated server ack.
  // internalDropped records why release is unsafe, independently of released.
  action dropInternalFuture = all {
    s.phase == "drain",
    s' = s.with("internalDropped", true).with("abandoned", true)
      .with("settlement", "unknown").with("rejected", true)
      .with("callback", "discarded").with("outcome", "error").with("phase", "done")
      .with("released", if (BROKEN == "reuse") "reusable" else "discarded"),
  }
  action step = {
    nondet kind = Set("success", "schema", "prepare", "prefix", "decode", "unknown", "callback").oneOf()
    nondet stamp = Set(1, 2).oneOf()
    any { prepare(kind), abortPreparation, dispatch, drain, settle, callback,
      fresh(stamp), settleFresh, finish, callAfterTerminal, abandon, callDuringFinishing, dropInternalFuture }
  }
  val orderedDispatch = s.dispatched.indices().forall(i => s.dispatched[i] == requests[i])
    and (s.drained == 0 or s.dispatched == requests)
  val completeDrain = s.settlement == "" or s.internalDropped or s.kind == "prepare" or s.drained == 5
  val retainsPrefix = s.observation == s.prefixLedger
  val candidateNeverPins = s.phase != "prepareAbort" and s.phase != "dispatch" or s.pin == 0
  val retainsPin = s.firstPin == 0 or s.pin == s.firstPin
  val freshAfterAbort = not(s.fresh) or
    (s.settlement == "aborted" and completeDrain and schema and s.outcome == "aborted")
  val noUnknownReplay = not(s.replay) or
    (s.settlement == "aborted" and s.freshSettled and s.freshStamp != 1 and s.firstPin == 0)
  val acceptedOnlySuccess = s.outcome != "completed" or oracleAccepted
  val publishedOnlyAccepted = not(s.response or s.cache) or
    (oracleAccepted and s.outcome == "completed" and s.callback == "complete" and
      s.settlement == "committed" and not(s.rejected))
  val rejectionSticky = not((s.drained >= 3 and Set("schema", "decode").contains(s.kind)) or
    (s.drained >= 2 and s.kind == "prefix") or s.settlement == "aborted") or s.rejected
  val discardedConnection = not(s.internalDropped) or
    (s.released == "discarded" and s.settlement == "unknown" and s.outcome == "error" and
      not(s.response or s.cache or s.fresh or s.replay) and s.settlements == 0)
  val terminalNative = s.settlements <= 1 and s.ioAfterTerminal == 0
  val inv = orderedDispatch and completeDrain and retainsPrefix and candidateNeverPins and
    retainsPin and freshAfterAbort and noUnknownReplay and acceptedOnlySuccess and
    publishedOnlyAccepted and rejectionSticky and discardedConnection and terminalNative

  val internalUnknownDiscarded = s.internalDropped and s.phase == "done" and s.drained < 5 and s.released == "discarded"
  val abandonedDrained = s.phase == "done" and s.abandoned and s.blockedFinishingCall and s.drained == 5 and s.settlements == 1 and not(s.response or s.cache)
  val committedPublication = s.response and s.cache and s.server == "committed"
  val caughtAbort = s.phase == "done" and s.kind == "schema" and s.outcome == "aborted"
  val localDecodeRejected = s.phase == "done" and s.kind == "decode" and s.server == "committed" and s.rejected and not(s.response)
  val unknownDiscarded = s.phase == "done" and s.kind == "unknown" and not(s.response) and not(s.replay)
  val prefixPinned = s.freshSettled and s.firstPin == 1 and s.pin == 1 and s.freshStamp == 2 and not(s.replay)
  val prepareFresh = s.freshSettled and s.kind == "prepare" and s.firstPin == 0 and s.pin == 2 and s.replay
  val unchangedCorruption = s.freshSettled and s.freshStamp == 1 and not(s.replay)
  val catalogFailure = s.phase == "done" and s.kind == "prefix" and not(s.fresh)
  val hostileCallRejected = s.calledAfterTerminal and s.phase == "done" and s.settlements == 1 and s.ioAfterTerminal == 0
  action pipeline = dispatch.then(dispatch).then(dispatch).then(dispatch).then(dispatch)
    .then(drain).then(drain).then(drain).then(drain).then(drain).then(settle).then(callback)
  run givenSuccessWhenCompleteThenPublishTest = init.then(prepare("success")).then(pipeline).then(finish).expect(committedPublication and inv)
  run givenSchemaWhenCaughtThenAbortedTest = init.then(prepare("schema")).then(pipeline).then(finish).expect(caughtAbort and inv)
  run givenDecodeWhenCommitAcknowledgedThenRejectTest = init.then(prepare("decode")).then(pipeline).then(finish).expect(localDecodeRejected and inv)
  run givenUnknownWhenCallbackReturnsThenDiscardTest = init.then(prepare("unknown")).then(pipeline).then(finish).expect(unknownDiscarded and inv)
  run givenPrefixWhenFreshRecreatedThenKeepPinTest = init.then(prepare("schema")).then(pipeline).then(fresh(2)).then(settleFresh).expect(prefixPinned and inv)
  run givenPrepareFailureWhenFreshChangedThenAllowReplayTest = init.then(prepare("prepare")).then(abortPreparation).then(callback).then(fresh(2)).then(settleFresh).expect(prepareFresh and inv)
  run givenSchemaWhenFreshUnchangedThenNoReplayTest = init.then(prepare("schema")).then(pipeline).then(fresh(1)).then(settleFresh).expect(unchangedCorruption and inv)
  run givenPrefixFailureWhenCaughtThenNoFreshTest = init.then(prepare("prefix")).then(pipeline).then(finish).expect(catalogFailure and inv)
  run givenCallbackErrorWhenCommittedThenNoPublicationTest = init.then(prepare("callback")).then(pipeline).then(finish).expect(s.server == "committed" and s.outcome == "error" and not(s.response or s.cache) and inv)
  run givenContinuedOwnerWhenQueryAttemptedThenDrainAndRejectTest = init.then(prepare("success"))
    .then(dispatch).then(dispatch).then(dispatch).then(dispatch).then(dispatch)
    .then(abandon).then(callDuringFinishing).then(drain).then(drain).then(drain).then(drain).then(drain)
    .then(settle).then(callback).then(finish).expect(abandonedDrained and inv)
  run givenInternalFutureDroppedWhenRepliesOutstandingThenUnknownAndDiscardTest = init.then(prepare("schema"))
    .then(dispatch).then(dispatch).then(dispatch).then(dispatch).then(dispatch)
    .then(drain).then(drain).then(dropInternalFuture).then(callAfterTerminal)
    .expect(internalUnknownDiscarded and not(s.response or s.cache or s.fresh or s.replay) and inv)
  run givenTerminalWhenCalledAgainThenNoNativeWorkTest = init.then(prepare("success")).then(pipeline).then(finish).then(callAfterTerminal).expect(hostileCallRejected and inv)
}
module sql_read_window { import sql_read_window_model(BROKEN = "none").* }
module sql_read_window_abort { import sql_read_window_model(BROKEN = "abort").* }
module sql_read_window_unknown { import sql_read_window_model(BROKEN = "unknown").* }
module sql_read_window_early { import sql_read_window_model(BROKEN = "early").* }
module sql_read_window_prefix { import sql_read_window_model(BROKEN = "prefix").* }
module sql_read_window_candidate { import sql_read_window_model(BROKEN = "candidate").* }
module sql_read_window_decode { import sql_read_window_model(BROKEN = "decode").* }
module sql_read_window_double { import sql_read_window_model(BROKEN = "double").* }
module sql_read_window_io { import sql_read_window_model(BROKEN = "io").* }
module sql_read_window_pin { import sql_read_window_model(BROKEN = "pin").* }
module sql_read_window_drain { import sql_read_window_model(BROKEN = "drain").* }
module sql_read_window_latch { import sql_read_window_model(BROKEN = "latch").* }
module sql_read_window_finishing { import sql_read_window_model(BROKEN = "finishing").* }
module sql_read_window_reuse { import sql_read_window_model(BROKEN = "reuse").* }
