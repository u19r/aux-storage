/// Bug-hunting wrapper for sync membership gates.
///
/// Suggested invariant run:
/// quint run quint/sync_membership_bughunt.qnt --invariant=inv --max-samples=5000 --max-steps=12
module sync_membership_bughunt {
  import sync_membership.* from "./sync_membership"

  action UnsafeActivateWithoutJointConsensus = all {
    oldConfigCommitted' = oldConfigCommitted,
    jointConfigCommitted' = jointConfigCommitted,
    newConfigCommitted' = newConfigCommitted,
    leaderHasQuorum' = leaderHasQuorum,
    membershipActive' = true,
    lastOutcome' = "unsafe_membership_active",
  }

  action bughuntStep = any {
    step,
    UnsafeActivateWithoutJointConsensus,
  }

  val unsafe_membership_witness = lastOutcome == "unsafe_membership_active"
}
