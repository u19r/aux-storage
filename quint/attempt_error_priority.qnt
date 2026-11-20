/// Bounded settlement/error arbitration, not native SDK or atomics correctness.
module attempt_error_priority_model {
  const BROKEN: str
  type State = { calls: List[str], history: List[str], rejected: bool,
    rank: int, cause: str, cached: bool, done: bool, callback: str, result: str }
  var s: State
  pure val outcomes = Set("ok", "conflict", "unavailable", "capacity", "bytes", "permanent1", "permanent2", "unknown", "abandoned")
  pure def isPermanent(e: str): bool = Set("permanent1", "permanent2").contains(e)
  pure def strength(e: str): int = if (e == "unknown") 5 else if (e == "bytes") 4
    else if (e == "capacity") 3 else if (e == "unavailable") 2 else if (e == "ok") 0 else 1
  pure def ranked(r: int): str = if (r == 5) "unknown" else if (r == 4) "bytes"
    else if (r == 3) "capacity" else if (r == 2) "unavailable" else if (r == 1) "conflict" else "ok"
  pure def combine(callback: str, native: str): str = if (callback == "unknown") callback
    else if (Set("unknown", "permanent1", "permanent2", "bytes", "capacity", "unavailable").contains(native)) native
    else if (callback == "ok") native else callback
  pure def control(error: str, cached: bool): str = if (cached and error == "conflict") "fresh" else error
  action initMode(cached: bool): bool = s' = { calls: List(), history: List(), rejected: false,
    rank: 0, cause: "", cached: cached, done: false, callback: "", result: "" }
  action init = any { initMode(true), initMode(false) }
  action begin = all { not(s.done), s.rank == 0, s.calls.length() < 2,
    s' = s.with("calls", s.calls.append("pending")) }
  action complete(i: int, error: str): bool = {
    val cause = if (isPermanent(error) and BROKEN != "old" and (s.cause == "" or BROKEN == "overwrite")) error else s.cause
    all { not(s.done), s.calls.indices().contains(i), s.calls[i] == "pending", outcomes.contains(error),
      s' = s.with("calls", s.calls.replaceAt(i, error)).with("history", s.history.append(error))
        .with("rank", if (strength(error) > s.rank) strength(error) else s.rank).with("cause", cause) }
  }
  action reject = all { not(s.done), not(s.rejected),
    s' = s.with("rejected", true).with("rank", if (s.rank == 0) 1 else s.rank) }
  val nativeResult = if (s.rank == 5 and BROKEN != "unknown") "unknown"
    else if (s.cause != "") s.cause else ranked(s.rank)
  action finish(callback: str): bool = {
    val combined = if (BROKEN == "callback" and callback != "ok") callback else if (BROKEN == "callbackUnknown" and nativeResult != "ok") nativeResult else combine(callback, nativeResult)
    all { not(s.done), not(s.calls.indices().exists(i => s.calls[i] == "pending")),
      Set("ok", "conflict", "semantic", "permanent2", "unknown").contains(callback),
      s' = s.with("done", true).with("callback", callback).with("result", (if (BROKEN == "controlAll" and s.cached and combined != "ok") "fresh" else control(combined, s.cached))) }
  }
  action idle = all { s.done, s' = s }
  action step = { nondet i = Set(0, 1).oneOf(); nondet error = outcomes.oneOf()
    nondet callback = Set("ok", "conflict", "semantic", "permanent2", "unknown").oneOf()
    any { begin, complete(i, error), reject, finish(callback), idle } }
  // Independent append-only completion ledger is the specification.
  val permanentHistory = s.history.select(e => e == "permanent1" or e == "permanent2")
  val expectedNative = if (s.history.indices().exists(i => s.history[i] == "unknown")) "unknown"
    else if (permanentHistory.length() > 0) permanentHistory[0]
    else if (s.history.indices().exists(i => s.history[i] == "bytes")) "bytes" else if (s.history.indices().exists(i => s.history[i] == "capacity")) "capacity"
    else if (s.history.indices().exists(i => s.history[i] == "unavailable")) "unavailable"
    else if (s.rejected or s.history.indices().exists(i => s.history[i] != "ok")) "conflict" else "ok"
  // This specification does not call the implementation's combine/control.
  val expectedAnswer = if (s.callback == "unknown" or expectedNative == "unknown") "unknown"
    else if (permanentHistory.length() > 0) permanentHistory[0]
    else if (Set("bytes", "capacity", "unavailable").contains(expectedNative)) expectedNative
    else if (s.callback != "ok") s.callback else expectedNative
  val inv = not(s.done) or (not(s.calls.indices().exists(i => s.calls[i] == "pending")) and
    (if (expectedAnswer == "conflict" and s.cached) s.result == "fresh" else s.result == expectedAnswer))
  val unknownFirst = s.done and s.history == List("unknown", "permanent1") and s.result == "unknown"
  val unknownLast = s.done and s.history == List("permanent1", "unknown") and s.result == "unknown"
  val permanentConflict = s.done and s.history.indices().exists(i => s.history[i] == "permanent1") and s.callback == "conflict" and s.result == "permanent1"
  val firstCause = s.done and permanentHistory == List("permanent2", "permanent1") and s.result == "permanent2"
  val abandonedAbort = s.done and s.history == List("abandoned") and s.callback == "ok" and s.result == "fresh"
  val discardedSuccess = s.done and s.history.indices().exists(i => s.history[i] == "permanent1") and s.callback == "ok" and s.result == "permanent1"
  run permanentCallbackConflictTest = initMode(true).then(begin).then(complete(0, "permanent1"))
    .then(finish("conflict")).expect(permanentConflict and inv)
  run unknownAfterPermanentTest = initMode(true).then(begin).then(begin).then(complete(0, "permanent1"))
    .then(complete(1, "unknown")).then(finish("semantic")).expect(unknownLast and inv)
  run permanentAfterUnknownTest = initMode(true).then(begin).then(begin).then(complete(1, "unknown"))
    .then(complete(0, "permanent1")).then(finish("ok")).expect(unknownFirst and inv)
  run firstPermanentTest = initMode(true).then(begin).then(begin).then(complete(1, "permanent2"))
    .then(complete(0, "permanent1")).then(finish("ok")).expect(firstCause and inv)
  run abandonedCallTest = initMode(true).then(begin).then(complete(0, "abandoned"))
    .then(finish("ok")).expect(abandonedAbort and inv)
  run rejectedCallbackSuccessTest = initMode(false).then(begin).then(complete(0, "permanent1"))
    .then(finish("ok")).expect(discardedSuccess and inv)
}
module attempt_error_priority { import attempt_error_priority_model(BROKEN = "none").* }
module attempt_error_priority_old { import attempt_error_priority_model(BROKEN = "old").* }
module attempt_error_priority_callback { import attempt_error_priority_model(BROKEN = "callback").* }
module attempt_error_priority_unknown { import attempt_error_priority_model(BROKEN = "unknown").* }
module attempt_error_priority_callbackUnknown { import attempt_error_priority_model(BROKEN = "callbackUnknown").* }
module attempt_error_priority_controlAll { import attempt_error_priority_model(BROKEN = "controlAll").* }
module attempt_error_priority_overwrite { import attempt_error_priority_model(BROKEN = "overwrite").* }

/// Composition boundary: each factory invocation returns one abstract outcome.
/// The settlement module proves permanent-callback-conflict -> permanent.
/// Prep conflict is an ordinary factory error even when Cached was selected.
module attempt_error_retry_model {
  const BROKEN: str
  type Entry = { input: str, policy: str, error: str, continued: bool }
  type RetryState = { budget: int, attempt: int, used: bool, done: bool,
    history: List[Entry], result: str, published: bool }
  var r: RetryState
  pure val inputs = Set("ok", "native-conflict", "no-mutation", "prep-conflict", "permanent-callback-conflict", "unknown", "repeat-control")
  pure def nativeError(input: str, policy: str): str =
    if (input == "permanent-callback-conflict") "permanent"
    else if (input == "native-conflict") (if (policy == "Cached") "control" else "conflict")
    else if (input == "no-mutation") (if (policy == "Cached") "control" else "ok")
    else if (input == "prep-conflict") "conflict" else if (input == "repeat-control") "control" else input
  action initBudget(budget: int): bool = r' = { budget: budget, attempt: 1,
    used: false, done: false, history: List(), result: "", published: false }
  action init = any { initBudget(1), initBudget(2) }
  action execute(input: str): bool = {
    val policy = if (r.history.length() == 0 or BROKEN == "cached") "Cached" else "Fresh"
    val error = if (input == "permanent-callback-conflict" and BROKEN == "old")
      (if (policy == "Cached") "control" else "conflict")
      else if (input == "unknown" and BROKEN == "unknownReplay") "conflict"
      else if (input == "no-mutation" and policy == "Fresh" and BROKEN == "freshNoop") "conflict"
      else nativeError(input, policy)
    val controlAllowed = error == "control" and (not(r.used) or BROKEN == "repeat")
    val chargedControl = BROKEN == "charge" and error == "control"
    val again = (controlAllowed and (not(chargedControl) or r.attempt < r.budget))
      or (error == "conflict" and r.attempt < r.budget)
    all { not(r.done), r.history.length() < 4, inputs.contains(input),
      r' = r.with("history", r.history.append({ input: input, policy: policy, error: error, continued: again }))
        .with("used", r.used or error == "control")
        .with("attempt", r.attempt + (if (again and (error != "control" or chargedControl)) 1 else 0))
        .with("done", not(again)).with("published", error == "ok")
        .with("result", if (again) "" else if (error == "control" and r.used) "protocol-error" else error) }
  }
  action idle = all { r.done, r' = r }
  action step = { nondet input = inputs.oneOf(); any { execute(input), idle } }
  // Count independent returned errors before each invocation, not engine counters.
  val inv = r.history.indices().forall(i => {
    val entry = r.history[i]
    val earlier = r.history.slice(0, i)
    val controls = earlier.select(e => e.error == "control").length()
    val conflicts = earlier.select(e => e.error == "conflict").length()
    val expectedPolicy = if (i == 0) "Cached" else "Fresh"
    // Derive the requirement directly from recorded native input and position.
    // Do not call nativeError: that classifier is part of the implementation.
    val expectedError = if (entry.input == "unknown") "unknown"
      else if (entry.input == "permanent-callback-conflict") "permanent"
      else if (entry.input == "repeat-control") "control"
      else if (entry.input == "prep-conflict") "conflict"
      else if (i == 0 and Set("native-conflict", "no-mutation").contains(entry.input)) "control"
      else if (entry.input == "native-conflict") "conflict" else "ok"
    val expectedContinue = (expectedError == "control" and controls == 0)
      or (expectedError == "conflict" and conflicts + 1 < r.budget)
    entry.policy == expectedPolicy and entry.error == expectedError and entry.continued == expectedContinue
      and (i == 0 or r.history[i - 1].continued)
  }) and (not(r.published) or (r.done and r.result == "ok"))
    and (not(r.done) or {
      val last = r.history[r.history.length() - 1]
      val previousControls = r.history.slice(0, r.history.length() - 1).select(e => e.error == "control").length()
      not(last.continued) and r.result == (if (last.error == "control" and previousControls > 0) "protocol-error" else last.error)
    })
  val extraFreshSuccess = r.done and r.budget == 1 and r.history.length() == 2 and r.result == "ok"
    and r.history[0].error == "control"
  val freshConflictExhausted = r.done and r.budget == 1 and r.history.length() == 2 and r.result == "conflict"
  val threeExecutions = r.done and r.budget == 2 and r.history.length() == 3 and r.result == "conflict"
  val repeatStopped = r.done and r.result == "protocol-error" and r.history.length() == 2
  val prepConsumesBudget = r.done and r.budget == 1 and r.history.length() == 1 and r.history[0].input == "prep-conflict"
  val permanentStops = r.done and r.history.length() == 1 and r.result == "permanent"
  run oneExtraFreshTest = initBudget(1).then(execute("no-mutation")).then(execute("ok")).expect(extraFreshSuccess and inv)
  run cachedThenFreshNoMutationTest = initBudget(1).then(execute("no-mutation")).then(execute("no-mutation"))
    .expect(extraFreshSuccess and r.published and inv)
  run conflictThenFreshNoMutationTest = initBudget(1).then(execute("native-conflict")).then(execute("no-mutation"))
    .expect(extraFreshSuccess and r.published and inv)
  run freshConflictBudgetOneTest = initBudget(1).then(execute("native-conflict")).then(execute("native-conflict")).expect(freshConflictExhausted and inv)
  run freshConflictBudgetTwoTest = initBudget(2).then(execute("native-conflict")).then(execute("native-conflict"))
    .then(execute("native-conflict")).expect(threeExecutions and inv)
  run repeatedControlTest = initBudget(2).then(execute("repeat-control")).then(execute("repeat-control")).expect(repeatStopped and inv)
  run preparationConflictTest = initBudget(1).then(execute("prep-conflict")).expect(prepConsumesBudget and inv)
  run physicalPermanentStopsTest = initBudget(2).then(execute("permanent-callback-conflict")).expect(permanentStops and inv)
}
module attempt_error_retry { import attempt_error_retry_model(BROKEN = "none").* }
module attempt_error_retry_old { import attempt_error_retry_model(BROKEN = "old").* }
module attempt_error_retry_charge { import attempt_error_retry_model(BROKEN = "charge").* }
module attempt_error_retry_repeat { import attempt_error_retry_model(BROKEN = "repeat").* }
module attempt_error_retry_cached { import attempt_error_retry_model(BROKEN = "cached").* }
module attempt_error_retry_unknownReplay { import attempt_error_retry_model(BROKEN = "unknownReplay").* }

module attempt_error_retry_freshNoop { import attempt_error_retry_model(BROKEN = "freshNoop").* }
