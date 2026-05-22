/// Compact MBT surface for sync-owned multi-region sender ownership.
///
/// Protects the rule that only the current sync Raft leader may run outbound
/// asynchronous multi-region replication.
module sync_multi_region_sender_ownership_mbt {
  var lastRole: str
  var lastDecision: str

  pure def expectedDecision(role) =
    if (role == "leader") {
      "owns_sender"
    } else {
      "standby"
    }

  action init = all {
    lastRole' = "disabled",
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet role = Set("disabled", "learner", "follower", "candidate", "leader").oneOf()
    all {
      lastRole' = role,
      lastDecision' = expectedDecision(role),
    }
  }

  action step = Check

  val inv =
    lastDecision != "owns_sender" or lastRole == "leader"

  run leaderOwnsSender =
    true.expect(expectedDecision("leader") == "owns_sender")

  run followerIsStandby =
    true.expect(expectedDecision("follower") == "standby")

  run candidateIsStandby =
    true.expect(expectedDecision("candidate") == "standby")
}
