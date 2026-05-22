/// Compact MBT surface for sync leader forwarding.
///
/// Protects the API-visible behavior found during the first three-process
/// runtime probe: followers must not convert OpenRaft ForwardToLeader into a
/// generic internal error. They must return a not-leader decision and preserve
/// any leader hint for the HTTP header/client retry path.
module sync_leader_forward_mbt {
  type ForwardInput = {
    localIsLeader: bool,
    leaderHintKnown: bool,
  }

  var lastInput: ForwardInput
  var lastDecision: str
  var lastHintForwarded: bool

  pure def expectedDecision(input) =
    if (input.localIsLeader) "serve" else "not_leader"

  pure def expectedHintForwarded(input) =
    not(input.localIsLeader) and input.leaderHintKnown

  action init = all {
    lastInput' = {
      localIsLeader: false,
      leaderHintKnown: false,
    },
    lastDecision' = "not_checked",
    lastHintForwarded' = false,
  }

  action Check = {
    nondet localIsLeader = Set(false, true).oneOf()
    nondet leaderHintKnown = Set(false, true).oneOf()
    val input = {
      localIsLeader: localIsLeader,
      leaderHintKnown: leaderHintKnown,
    }
    all {
      lastInput' = input,
      lastDecision' = expectedDecision(input),
      lastHintForwarded' = expectedHintForwarded(input),
    }
  }

  action step = Check

  val inv =
    (lastDecision == "serve" iff lastInput.localIsLeader) and
    (lastHintForwarded iff (lastDecision == "not_leader" and lastInput.leaderHintKnown))

  run followerWithLeaderHintForwardsHint =
    true.expect(expectedDecision({
      localIsLeader: false,
      leaderHintKnown: true,
    }) == "not_leader").expect(expectedHintForwarded({
      localIsLeader: false,
      leaderHintKnown: true,
    }))

  run followerWithoutLeaderHintStillReturnsNotLeader =
    true.expect(expectedDecision({
      localIsLeader: false,
      leaderHintKnown: false,
    }) == "not_leader").expect(not(expectedHintForwarded({
      localIsLeader: false,
      leaderHintKnown: false,
    })))

  run leaderServesWithoutForwardingHint =
    true.expect(expectedDecision({
      localIsLeader: true,
      leaderHintKnown: true,
    }) == "serve").expect(not(expectedHintForwarded({
      localIsLeader: true,
      leaderHintKnown: true,
    })))
}
