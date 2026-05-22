/// Sync membership gate model.
///
/// Protects Invariant Inventory ID SYNC-2 from docs/quint-backfill.md.
module sync_membership {
  var oldConfigCommitted: bool
  var jointConfigCommitted: bool
  var newConfigCommitted: bool
  var leaderHasQuorum: bool
  var membershipActive: bool
  var lastOutcome: str

  pure def canActivateMembership(oldCommitted, jointCommitted, newCommitted, quorum) =
    oldCommitted and jointCommitted and newCommitted and quorum

  def membershipRequiresJointConsensus =
    not(membershipActive) or canActivateMembership(
      oldConfigCommitted,
      jointConfigCommitted,
      newConfigCommitted,
      leaderHasQuorum,
    )

  action init = all {
    oldConfigCommitted' = true,
    jointConfigCommitted' = false,
    newConfigCommitted' = false,
    leaderHasQuorum' = false,
    membershipActive' = false,
    lastOutcome' = "pending",
  }

  action CommitJointConfig = all {
    oldConfigCommitted' = oldConfigCommitted,
    jointConfigCommitted' = oldConfigCommitted,
    newConfigCommitted' = newConfigCommitted,
    leaderHasQuorum' = leaderHasQuorum,
    membershipActive' = membershipActive,
    lastOutcome' = if (oldConfigCommitted) "joint_committed" else "joint_blocked",
  }

  action CommitNewConfig = all {
    oldConfigCommitted' = oldConfigCommitted,
    jointConfigCommitted' = jointConfigCommitted,
    newConfigCommitted' = jointConfigCommitted,
    leaderHasQuorum' = leaderHasQuorum,
    membershipActive' = membershipActive,
    lastOutcome' = if (jointConfigCommitted) "new_config_committed" else "new_config_blocked",
  }

  action ConfirmQuorum = all {
    oldConfigCommitted' = oldConfigCommitted,
    jointConfigCommitted' = jointConfigCommitted,
    newConfigCommitted' = newConfigCommitted,
    leaderHasQuorum' = true,
    membershipActive' = membershipActive,
    lastOutcome' = "quorum_confirmed",
  }

  action TryActivateMembership = all {
    oldConfigCommitted' = oldConfigCommitted,
    jointConfigCommitted' = jointConfigCommitted,
    newConfigCommitted' = newConfigCommitted,
    leaderHasQuorum' = leaderHasQuorum,
    membershipActive' = canActivateMembership(oldConfigCommitted, jointConfigCommitted, newConfigCommitted, leaderHasQuorum),
    lastOutcome' = if (canActivateMembership(oldConfigCommitted, jointConfigCommitted, newConfigCommitted, leaderHasQuorum)) "membership_active" else "membership_blocked",
  }

  action step = any {
    CommitJointConfig,
    CommitNewConfig,
    ConfirmQuorum,
    TryActivateMembership,
  }

  val inv = membershipRequiresJointConsensus

  val membership_success_witness = membershipActive
  val membership_rejected_witness = lastOutcome == "membership_blocked"

  run membershipRequiresJointConfig =
    false.expect(canActivateMembership(true, false, true, true))

  run membershipSucceedsAfterJointAndNewConfig =
    true.expect(canActivateMembership(true, true, true, true))
}
