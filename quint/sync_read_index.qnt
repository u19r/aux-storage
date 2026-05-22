/// Sync strong-read gate model.
///
/// Protects Invariant Inventory ID SYNC-2 from docs/quint-backfill.md.
module sync_read_index {
  var leaderHasQuorum: bool
  var readIndexApplied: bool
  var servedStrongRead: bool
  var lastOutcome: str

  pure def canServeStrongRead(quorum, applied) =
    quorum and applied

  def strongReadRequiresQuorumAndAppliedIndex =
    not(servedStrongRead) or canServeStrongRead(leaderHasQuorum, readIndexApplied)

  action init = all {
    leaderHasQuorum' = false,
    readIndexApplied' = false,
    servedStrongRead' = false,
    lastOutcome' = "pending",
  }

  action ConfirmQuorum = all {
    leaderHasQuorum' = true,
    readIndexApplied' = readIndexApplied,
    servedStrongRead' = servedStrongRead,
    lastOutcome' = "quorum_confirmed",
  }

  action ApplyReadIndex = all {
    leaderHasQuorum' = leaderHasQuorum,
    readIndexApplied' = true,
    servedStrongRead' = servedStrongRead,
    lastOutcome' = "read_index_applied",
  }

  action TryStrongRead = all {
    leaderHasQuorum' = leaderHasQuorum,
    readIndexApplied' = readIndexApplied,
    servedStrongRead' = canServeStrongRead(leaderHasQuorum, readIndexApplied),
    lastOutcome' = if (canServeStrongRead(leaderHasQuorum, readIndexApplied)) "strong_read_served" else "strong_read_blocked",
  }

  action step = any {
    ConfirmQuorum,
    ApplyReadIndex,
    TryStrongRead,
  }

  val inv = strongReadRequiresQuorumAndAppliedIndex

  val strong_read_success_witness = servedStrongRead
  val strong_read_rejected_witness = lastOutcome == "strong_read_blocked"

  run strongReadRequiresQuorum =
    false.expect(canServeStrongRead(false, true))

  run strongReadSucceedsWithQuorumAndIndex =
    true.expect(canServeStrongRead(true, true))
}
