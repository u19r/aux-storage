/// Compact MBT surface for the sync strong-read gate.
///
/// Protects stale leader and apply-before-serve behavior at the storage-sync
/// boundary before binding it to the OpenRaft runtime.
module sync_read_index_mbt {
  type ReadGate = {
    leaderHasQuorum: bool,
    readIndexApplied: bool,
  }

  var lastGate: ReadGate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (gate.leaderHasQuorum and gate.readIndexApplied) "serve" else "block"

  action init = all {
    lastGate' = {
      leaderHasQuorum: false,
      readIndexApplied: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet leaderHasQuorum = Set(false, true).oneOf()
    nondet readIndexApplied = Set(false, true).oneOf()
    val gate = {
      leaderHasQuorum: leaderHasQuorum,
      readIndexApplied: readIndexApplied,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "serve" or (lastGate.leaderHasQuorum and lastGate.readIndexApplied)

  run staleLeaderBlocksStrongRead =
    true.expect(expectedDecision({
      leaderHasQuorum: false,
      readIndexApplied: true,
    }) == "block")

  run unappliedReadIndexBlocksStrongRead =
    true.expect(expectedDecision({
      leaderHasQuorum: true,
      readIndexApplied: false,
    }) == "block")

  run quorumAndAppliedServesStrongRead =
    true.expect(expectedDecision({
      leaderHasQuorum: true,
      readIndexApplied: true,
    }) == "serve")
}
