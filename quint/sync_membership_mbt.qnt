/// Compact MBT surface for sync membership activation.
///
/// Protects joint-consensus and quorum requirements before Rust membership
/// operations are bound to OpenRaft runtime calls.
module sync_membership_mbt {
  type MembershipGate = {
    oldConfigCommitted: bool,
    jointConfigCommitted: bool,
    newConfigCommitted: bool,
    leaderHasQuorum: bool,
  }

  var lastGate: MembershipGate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (
      gate.oldConfigCommitted and
      gate.jointConfigCommitted and
      gate.newConfigCommitted and
      gate.leaderHasQuorum
    ) "activate" else "block"

  action init = all {
    lastGate' = {
      oldConfigCommitted: true,
      jointConfigCommitted: false,
      newConfigCommitted: false,
      leaderHasQuorum: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet oldConfigCommitted = Set(false, true).oneOf()
    nondet jointConfigCommitted = Set(false, true).oneOf()
    nondet newConfigCommitted = Set(false, true).oneOf()
    nondet leaderHasQuorum = Set(false, true).oneOf()
    val gate = {
      oldConfigCommitted: oldConfigCommitted,
      jointConfigCommitted: jointConfigCommitted,
      newConfigCommitted: newConfigCommitted,
      leaderHasQuorum: leaderHasQuorum,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "activate" or (
      lastGate.oldConfigCommitted and
      lastGate.jointConfigCommitted and
      lastGate.newConfigCommitted and
      lastGate.leaderHasQuorum
    )

  run jointConfigRequired =
    true.expect(expectedDecision({
      oldConfigCommitted: true,
      jointConfigCommitted: false,
      newConfigCommitted: true,
      leaderHasQuorum: true,
    }) == "block")

  run quorumRequired =
    true.expect(expectedDecision({
      oldConfigCommitted: true,
      jointConfigCommitted: true,
      newConfigCommitted: true,
      leaderHasQuorum: false,
    }) == "block")

  run completeMembershipActivates =
    true.expect(expectedDecision({
      oldConfigCommitted: true,
      jointConfigCommitted: true,
      newConfigCommitted: true,
      leaderHasQuorum: true,
    }) == "activate")
}
