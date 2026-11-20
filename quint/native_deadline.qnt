/// One operation/attempt. Ordered atomic native execution, retained error priority,
/// and durable acknowledgement are premises supplied by existing models.
/// clientDone means the actual owned native task/resource has finished cleanup;
/// neither dropping a future nor sending cancellation establishes that fact.
module native_deadline_model {
  const BROKEN: str
  type State = { clock: int, gate: str, started: bool, sent: bool,
    serverDone: bool, committed: bool, abortAck: bool, commitAck: bool, replyAt: int, resultReady: bool,
    cancelSent: bool, clientDone: bool, permit: bool, waiter: bool,
    answer: str, ackAtResponse: bool, resultAtResponse: bool, settlements: int }
  var s: State
  action init = s' = { clock: 0, gate: "open", started: false, sent: false,
    serverDone: false, committed: false, abortAck: false, commitAck: false, replyAt: -1, resultReady: false,
    cancelSent: false, clientDone: false, permit: true, waiter: true,
    answer: "", ackAtResponse: false, resultAtResponse: false, settlements: 0 }
  action tick = all { s.clock < 3, s' = s.with("clock", s.clock + 1) }
  // Any native protocol dispatch, including preparation before BEGIN.
  action startWork = all { s.clock == 0, s.gate == "open", not(s.started),
    s' = s.with("started", true) }
  // One atomic decision precedes polling ANY joined final mutation/read/COMMIT.
  action dispatch = all { s.clock <= 1, s.gate == "open", not(s.sent),
    s' = s.with("gate", if (BROKEN == "lateGate") "open" else "committing")
      .with("started", true).with("sent", true) }
  action expire = all { s.clock >= 1, s.gate == "open",
    s' = s.with("gate", "abandoned").with("cancelSent", s.started) }
  // Atomic transaction effects are independent of the response classifier.
  action serverCommit = all { s.sent, not(s.serverDone),
    s' = s.with("serverDone", true).with("committed", true) }
  // Record the clock when the reply is actually observed, before later hooks.
  action commitReply = all { s.serverDone, s.committed, not(s.clientDone), s.replyAt == -1,
    s' = s.with("replyAt", s.clock)
      .with("commitAck", s.clock <= 2 or BROKEN == "lateAck") }
  action finishCallback = all { not(s.resultReady), s.answer == "",
    s' = s.with("resultReady", true) }
  // Positive acknowledgement must cover all dispatched work, including rollback.
  action serverAbort(ack: bool): bool = all {
    s.gate == "abandoned", s.started, not(s.sent), not(s.serverDone),
    s' = s.with("serverDone", true).with("abortAck", ack) }
  // Actual resource completion is a separate observation, never a timer action.
  // The implementation must prove this predicate for its owned client work.
  action cancelNative = all { s.clock >= 2, s.started, not(s.clientDone), not(s.cancelSent),
    s' = s.with("cancelSent", true) }
  action cleanup = all { not(s.clientDone),
    s.serverDone or s.cancelSent or (s.gate == "abandoned" and not(s.started)),
    s' = s.with("clientDone", true) }
  action respond = {
    val answer = if (s.commitAck and BROKEN == "timeoutAck") "unavailable"
      else if (s.commitAck and (s.resultReady or BROKEN == "inventResult")) "success"
      else if (s.commitAck) "committedFailure"
      else if (s.gate == "abandoned" and (not(s.started) or s.abortAck or BROKEN == "dropAbort")) "unavailable"
      else "unknown"
    all { s.answer == "", s.clock >= 1, s.gate != "open",
      s.commitAck or s.abortAck or not(s.started) or s.clock >= 2,
      s' = s.with("answer", answer).with("ackAtResponse", s.commitAck).with("resultAtResponse", s.resultReady)
        .with("settlements", s.settlements + 1)
        .with("permit", if (BROKEN == "releaseResponse") false else s.permit) }
  }
  action cancelWaiter = all { s.waiter, s' = s.with("waiter", false)
    .with("permit", if (BROKEN == "releaseResponse") false else s.permit) }
  action release = all { s.clientDone, s.permit, s' = s.with("permit", false) }
  action step = { nondet ack = Set(true, false).oneOf()
    any { tick, startWork, dispatch, expire, serverCommit, commitReply,
      serverAbort(ack), finishCallback, cancelNative, cleanup, respond, cancelWaiter, release, s' = s } }
  // Oracle facts do not use the implementation's answer calculation.
  val inv = (s.gate != "abandoned" or not(s.sent)) and
    (s.answer != "unavailable" or (not(s.committed) and not(s.sent) and
      (not(s.started) or s.abortAck))) and
    (s.answer != "success" or (s.replyAt >= 0 and s.replyAt <= 2 and s.resultAtResponse)) and
    (s.answer != "committedFailure" or (s.replyAt >= 0 and s.replyAt <= 2 and not(s.resultAtResponse))) and
    (not(s.ackAtResponse) or s.answer == (if (s.resultAtResponse) "success" else "committedFailure")) and
    (s.permit or s.clientDone) and s.settlements == (if (s.answer == "") 0 else 1)
  // A ready ACK and response value must win even at the boundary.
  val readyAck = s.commitAck and s.answer == "success"
  val lateReplyUnknown = s.replyAt == 3 and s.answer == "unknown"
  val blockedCallback = s.answer == "committedFailure" and s.commitAck and not(s.resultReady)
  val beforeDispatch = s.answer == "unavailable" and not(s.started)
  val activeAbort = s.answer == "unavailable" and s.abortAck
  val lostAbort = s.answer == "unknown" and s.serverDone and not(s.sent) and s.started
  val dispatchWon = s.sent and s.clock == 1 and s.gate == "committing"
  val returnedHeld = s.answer == "unknown" and not(s.clientDone) and s.permit
  val lateFinish = s.answer == "unknown" and s.clientDone and s.committed
  val canceledHeld = not(s.waiter) and not(s.clientDone) and s.permit
  run expiryBeforeDispatchTest = init.then(tick).then(expire).then(respond).expect(beforeDispatch and inv)
  run activeAbortTest = init.then(startWork).then(tick).then(expire).then(serverAbort(true))
    .then(respond).expect(activeAbort and inv)
  run lostAbortTest = init.then(startWork).then(tick).then(expire).then(serverAbort(false))
    .then(tick).then(respond).expect(lostAbort and inv)
  run finalDispatchTest = init.then(tick).then(dispatch).expect(dispatchWon and inv)
  run readyReplyTest = init.then(dispatch).then(serverCommit).then(commitReply)
    .then(finishCallback).then(tick).then(respond).expect(readyAck and inv)
  run graceBoundaryReplyTest = init.then(dispatch).then(tick).then(tick)
    .then(serverCommit).then(commitReply).then(finishCallback).then(respond).expect(readyAck and inv)
  run lateBufferedReplyTest = init.then(dispatch).then(tick).then(tick).then(tick)
    .then(serverCommit).then(commitReply).then(finishCallback).then(respond)
    .expect(lateReplyUnknown and inv)
  run acknowledgedBlockedCallbackTest = init.then(dispatch).then(serverCommit)
    .then(commitReply).then(tick).then(respond).expect(blockedCallback and inv)
  run responseKeepsPermitTest = init.then(startWork).then(tick).then(expire)
    .then(serverAbort(true)).then(respond).expect(s.permit and not(s.clientDone) and inv)
  run unknownLateFinishTest = init.then(dispatch).then(tick).then(tick).then(respond)
    .expect(returnedHeld and inv).then(serverCommit).then(cleanup).then(release)
    .expect(lateFinish and inv)
  run clientCleanupBeforeServerTest = init.then(dispatch).then(tick).then(tick)
    .then(respond).then(cancelNative).then(cleanup).then(release)
    .expect(not(s.serverDone) and s.clientDone and inv).then(serverCommit).expect(inv)
  run canceledWaiterTest = init.then(startWork).then(cancelWaiter).expect(canceledHeld and inv)
}
module native_deadline { import native_deadline_model(BROKEN = "none").* }
module native_deadline_lateGate { import native_deadline_model(BROKEN = "lateGate").* }
module native_deadline_dropAbort { import native_deadline_model(BROKEN = "dropAbort").* }
module native_deadline_releaseResponse { import native_deadline_model(BROKEN = "releaseResponse").* }
module native_deadline_timeoutAck { import native_deadline_model(BROKEN = "timeoutAck").* }
module native_deadline_inventResult { import native_deadline_model(BROKEN = "inventResult").* }
module native_deadline_lateAck { import native_deadline_model(BROKEN = "lateAck").* }
