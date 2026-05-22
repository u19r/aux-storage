/// Compact MBT surface for deterministic sync Raft transport fault decisions.
///
/// This protects the policy boundary that chaos probes depend on: only a
/// current-leader append delivered to a voter quorum may be acknowledged. Loss,
/// delay, duplicate append, replay after leader change, stale leader writes, and
/// one-way partitions must not become successful client acknowledgement.
module sync_transport_fault_mbt {
  type Gate = {
    sourceNode: int,
    leaderNode: int,
    currentTerm: int,
    messageTerm: int,
    deliveredToVoters: int,
    voterCount: int,
    faultMode: str,
  }

  var lastGate: Gate
  var lastDecision: str

  pure def hasQuorum(deliveredToVoters, voterCount) =
    deliveredToVoters * 2 > voterCount

  pure def expectedDecision(gate) =
    if (gate.sourceNode != gate.leaderNode or gate.messageTerm < gate.currentTerm or gate.faultMode == "stale_leader") {
      "reject_stale_leader"
    } else if (gate.faultMode == "delivered") {
      if (hasQuorum(gate.deliveredToVoters, gate.voterCount)) {
        "acknowledge"
      } else {
        "block_quorum"
      }
    } else if (gate.faultMode == "lost") {
      "block_quorum"
    } else if (gate.faultMode == "delayed") {
      "defer_delivery"
    } else if (gate.faultMode == "duplicated") {
      "ignore_duplicate"
    } else if (gate.faultMode == "replayed_after_leader_change") {
      "ignore_replay"
    } else {
      "unreachable_asymmetric"
    }

  action init = all {
    lastGate' = {
      sourceNode: 1,
      leaderNode: 1,
      currentTerm: 1,
      messageTerm: 1,
      deliveredToVoters: 2,
      voterCount: 3,
      faultMode: "delivered",
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet sourceNode = Set(1, 2).oneOf()
    nondet currentTerm = Set(3, 4).oneOf()
    nondet messageTerm = Set(3, 4).oneOf()
    nondet deliveredToVoters = Set(0, 1, 2, 3).oneOf()
    nondet voterCount = Set(3, 5).oneOf()
    nondet faultMode = Set(
      "delivered",
      "lost",
      "delayed",
      "duplicated",
      "replayed_after_leader_change",
      "stale_leader",
      "one_way_partition"
    ).oneOf()
    val gate = {
      sourceNode: sourceNode,
      leaderNode: 1,
      currentTerm: currentTerm,
      messageTerm: messageTerm,
      deliveredToVoters: deliveredToVoters,
      voterCount: voterCount,
      faultMode: faultMode,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "acknowledge"
      or all {
        lastGate.sourceNode == lastGate.leaderNode,
        lastGate.messageTerm >= lastGate.currentTerm,
        lastGate.faultMode == "delivered",
        hasQuorum(lastGate.deliveredToVoters, lastGate.voterCount),
      }

  run lostAppendDoesNotAcknowledge =
    true.expect(expectedDecision({
      sourceNode: 1,
      leaderNode: 1,
      currentTerm: 4,
      messageTerm: 4,
      deliveredToVoters: 0,
      voterCount: 3,
      faultMode: "lost",
    }) == "block_quorum")

  run oneWayPartitionIsAsymmetricUnreachable =
    true.expect(expectedDecision({
      sourceNode: 1,
      leaderNode: 1,
      currentTerm: 4,
      messageTerm: 4,
      deliveredToVoters: 2,
      voterCount: 3,
      faultMode: "one_way_partition",
    }) == "unreachable_asymmetric")

  run staleLeaderRejectsBeforeDelivery =
    true.expect(expectedDecision({
      sourceNode: 2,
      leaderNode: 1,
      currentTerm: 4,
      messageTerm: 4,
      deliveredToVoters: 3,
      voterCount: 3,
      faultMode: "delivered",
    }) == "reject_stale_leader")

  run currentLeaderQuorumAcknowledges =
    true.expect(expectedDecision({
      sourceNode: 1,
      leaderNode: 1,
      currentTerm: 4,
      messageTerm: 4,
      deliveredToVoters: 2,
      voterCount: 3,
      faultMode: "delivered",
    }) == "acknowledge")
}
