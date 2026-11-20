/// Ordinary FDB attempt only: callback value precedes eager COMMIT submission.
/// Atomic effects/error priority remain premises of their existing models.
/// Unlike SQL, destroying an unsubmitted FDB transaction discards local writes.
module foundationdb_deadline_model {
  const BROKEN: str
  type State = { clock: int, gate: str, ready: bool, sent: bool,
    destroyed: bool, clientDone: bool, cancel: bool, committed: bool,
    replyAt: int, answer: str, permit: bool }
  var s: State
  action init = s' = { clock: 0, gate: "open", ready: false, sent: false,
    destroyed: false, clientDone: false, cancel: false, committed: false,
    replyAt: -1, answer: "", permit: true }
  action tick = all { s.clock < 3, s' = s.with("clock", s.clock + 1) }
  action callback = all { s.gate == "open", s.clock == 0,
    s' = s.with("ready", true) }
  // Transaction::commit calls C immediately, before polling its future.
  action callCommit = all { s.ready, s.gate == "open", s.clock == 0, not(s.sent),
    s' = s.with("sent", true)
      .with("gate", if (BROKEN == "gateAfterCall") "open" else "commit") }
  action expire = all { s.clock >= 1, s.gate == "open",
    s' = s.with("gate", "abandoned").with("destroyed", true).with("clientDone", true) }
  // Client cancellation/destruction after submission cannot establish noncommit.
  action destroySubmittedHandles = all { s.sent, s.clock >= 2, not(s.clientDone),
    s' = s.with("cancel", true).with("clientDone", true).with("destroyed", true) }
  action serverCommit = all { s.sent, not(s.committed),
    s' = s.with("committed", true) }
  // Record the observation instant before later result/hook work.
  action observeAck = all { s.committed, not(s.clientDone), s.replyAt == -1,
    s' = s.with("replyAt", s.clock) }
  action disposeAcknowledged = all { s.replyAt >= 0, not(s.clientDone),
    s' = s.with("destroyed", true).with("clientDone", true) }
  action respond = {
    val timely = s.replyAt >= 0 and (s.replyAt <= 2 or BROKEN == "lateAck")
    all { s.answer == "", s.gate != "open", s.clock >= 1,
      s.gate == "abandoned" or s.replyAt >= 0 or s.clientDone or s.clock >= 2,
      s' = s.with("answer", if (timely) "success"
        else if (not(s.sent) and s.destroyed or BROKEN == "cancelAbort" and s.cancel)
          "unavailable" else "unknown")
        .with("permit", if (BROKEN == "releaseResponse") false else s.permit) }
  }
  action release = all { s.clientDone, s' = s.with("permit", false) }
  action step = any { tick, callback, callCommit, expire, destroySubmittedHandles,
    serverCommit, observeAck, disposeAcknowledged, respond, release, s' = s }
  val inv = (s.gate != "abandoned" or not(s.sent)) and
    (s.answer != "unavailable" or not(s.sent) and s.destroyed and not(s.committed)) and
    (s.answer != "success" or s.ready and s.replyAt >= 0 and s.replyAt <= 2) and
    (s.permit or s.clientDone)
  val precommitDisposed = s.answer == "unavailable" and s.destroyed
  val timelyAck = s.answer == "success" and s.replyAt == 2
  val lateAckUnknown = s.answer == "unknown" and s.replyAt == 3
  val submittedCancelled = s.answer == "unknown" and s.cancel and s.committed
  val responseHeld = s.answer == "unknown" and not(s.clientDone) and s.permit
  run precommitTest = init.then(callback).then(tick).then(expire).then(respond).expect(precommitDisposed and inv)
  run boundaryTest = init.then(callback).then(callCommit).then(tick).then(tick)
    .then(serverCommit).then(observeAck).then(respond).expect(timelyAck and s.permit and not(s.clientDone) and inv)
    .then(disposeAcknowledged).then(release).expect(not(s.permit) and inv)
  run cancelledCommitTest = init.then(callback).then(callCommit).then(tick).then(tick)
    .then(destroySubmittedHandles).then(respond).then(serverCommit).expect(submittedCancelled and inv)
  run heldResponseTest = init.then(callback).then(callCommit).then(tick).then(tick)
    .then(respond).expect(responseHeld and inv)
  run lateAckTest = init.then(callback).then(callCommit).then(tick).then(tick).then(tick)
    .then(serverCommit).then(observeAck).then(respond).expect(lateAckUnknown and inv)
}
module foundationdb_deadline { import foundationdb_deadline_model(BROKEN = "none").* }
module foundationdb_deadline_gateAfterCall { import foundationdb_deadline_model(BROKEN = "gateAfterCall").* }
module foundationdb_deadline_cancelAbort { import foundationdb_deadline_model(BROKEN = "cancelAbort").* }
module foundationdb_deadline_lateAck { import foundationdb_deadline_model(BROKEN = "lateAck").* }
module foundationdb_deadline_releaseResponse { import foundationdb_deadline_model(BROKEN = "releaseResponse").* }
