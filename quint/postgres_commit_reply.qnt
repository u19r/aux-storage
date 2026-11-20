/// Ordinary PostgreSQL COMMIT only. Timers, gates, atomicity, error priority and
/// resource cleanup belong to native_deadline; its durable commitReply premise
/// requires this qualification. No composition, liveness or native parity claim.
module postgres_commit_reply_model {
  const BROKEN: str
  type State = { phase: str, delivers: bool, qualified: bool, durable: bool,
    wire: List[str], pending: bool, flagged: bool, warned: bool, ack: bool,
    accepted: bool, idle: bool, reused: bool, reusedWarned: bool, notice: bool, canceled: bool }
  var s: State
  action init = s' = { phase: "new", delivers: true, qualified: false,
    durable: false, wire: [], pending: false, flagged: false, warned: false,
    ack: false, accepted: false, idle: false, reused: false, reusedWarned: false, notice: false, canceled: false }
  action configure(delivers: bool): bool = all { s.phase == "new",
    s' = s.with("delivers", delivers).with("phase", "configured") }
  action validate = all { s.phase == "configured",
    s' = s.with("qualified", s.delivers or BROKEN == "skipValidation")
      .with("phase", if (s.delivers or BROKEN == "skipValidation") "ready" else "rejected") }
  // Server oracle is separate from client observations: cancellation has already
  // locally committed, but has NOT met configured synchronous durability.
  // Both protocol messages are available together; no extra read separates them.
  action serverReply(cancel: bool, notice: bool): bool = all { s.phase == "ready",
    s' = s.with("durable", not(cancel)).with("canceled", cancel).with("phase", "reply")
      .with("wire", if (cancel and s.delivers) ["WARNING01000", "COMMIT"]
        else if (notice) ["NOTICE00000", "COMMIT"] else ["COMMIT"]) }
  // Premise: one actual Connection.poll_message owner; native FIFO returns a
  // Notice before routing subsequent COMMIT, even from one socket buffer.
  action poll = all { s.wire.length() > 0,
    val reordered = BROKEN == "outOfOrder" and s.wire.length() == 2
    val message = if (reordered) s.wire.nth(1) else s.wire.head()
    val warning = message == "WARNING01000"
    s' = s.with("wire", if (reordered) [s.wire.head()] else s.wire.tail())
      .with("warned", s.warned or warning)
      .with("flagged", s.flagged or (warning and BROKEN != "delayedPublication"))
      .with("pending", s.pending or (warning and BROKEN == "delayedPublication"))
      .with("notice", s.notice or message == "NOTICE00000")
      .with("ack", s.ack or message == "COMMIT") }
  action publish = all { s.pending,
    s' = s.with("pending", false).with("flagged", true) }
  action settle = all { s.phase == "reply", s.ack,
    s' = s.with("phase", "settled")
      .with("accepted", s.qualified and (not(s.flagged) or BROKEN == "ignoreWarning")) }
  action returnClient = all { s.phase == "settled", not(s.idle),
    s' = s.with("idle", true) }
  action idleWarning = all { s.idle, s.wire.length() == 0, not(s.warned),
    s' = s.with("wire", ["WARNING01000"]) }
  action checkout = all { s.idle, not(s.reused),
    not(s.flagged) or BROKEN == "resetFlag",
    s' = s.with("idle", false).with("reused", true).with("reusedWarned", s.warned)
      .with("flagged", if (BROKEN == "resetFlag") false else s.flagged) }
  action step = { nondet cancel = Set(true, false).oneOf()
    nondet notice = Set(true, false).oneOf()
    nondet delivers = Set(true, false).oneOf()
    any { configure(delivers), validate, serverReply(cancel, notice), poll,
      publish, settle, returnClient, idleWarning, checkout, s' = s } }
  val inv = (not(s.accepted) or s.durable) and
    (not(s.qualified) or s.delivers) and (not(s.warned) or s.flagged or s.pending) and
    not(s.reusedWarned)
  val normal = s.accepted and s.durable
  val weakReply = s.canceled and s.ack and not(s.accepted) and s.phase == "settled"
  val metadata = normal and s.notice
  val warningFirst = s.flagged and not(s.ack) and s.wire == ["COMMIT"]
  val idleFlag = s.idle and s.flagged and not(s.canceled)
  val suppressedRejected = s.phase == "rejected" and not(s.delivers)
  run normalTest = init.then(configure(true)).then(validate).then(serverReply(false, false))
    .then(poll).then(settle).expect(normal and inv)
  run warningTest = init.then(configure(true)).then(validate).then(serverReply(true, false))
    .then(poll).expect(warningFirst).then(poll).then(settle).expect(weakReply and inv)
  run noticeTest = init.then(configure(true)).then(validate).then(serverReply(false, true))
    .then(poll).then(poll).then(settle).expect(metadata and inv)
  run suppressionTest = init.then(configure(false)).then(validate).expect(suppressedRejected and inv)
  run idleTest = normalTest.then(returnClient).then(idleWarning).then(poll).expect(idleFlag and inv)
}
module postgres_commit_reply { import postgres_commit_reply_model(BROKEN = "none").* }
module postgres_commit_reply_ignoreWarning { import postgres_commit_reply_model(BROKEN = "ignoreWarning").* }
module postgres_commit_reply_delayedPublication { import postgres_commit_reply_model(BROKEN = "delayedPublication").* }
module postgres_commit_reply_skipValidation { import postgres_commit_reply_model(BROKEN = "skipValidation").* }
module postgres_commit_reply_resetFlag { import postgres_commit_reply_model(BROKEN = "resetFlag").* }
module postgres_commit_reply_outOfOrder { import postgres_commit_reply_model(BROKEN = "outOfOrder").* }
