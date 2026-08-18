/// ReadSequence planner validation contract.
///
/// Each action is a named boundary case mirrored by the Rust Driver.  Keeping
/// the cases explicit makes a failing witness identify the planner rule rather
/// than hiding it behind an unconstrained request generator.
module read_sequence_planner {
  var lastScenario: str
  var lastOutcome: str
  var accepted: bool
  var nodeCount: int
  var waveCount: int

  action init = all {
    lastScenario' = "none",
    lastOutcome' = "not_checked",
    accepted' = false,
    nodeCount' = 0,
    waveCount' = 0,
  }

  action AcceptedLateParent = all {
    lastScenario' = "accepted_late_parent",
    lastOutcome' = "accepted",
    accepted' = true,
    nodeCount' = 2,
    waveCount' = 2,
  }

  action AcceptedTransactionalPoint = all {
    lastScenario' = "accepted_transactional_point",
    lastOutcome' = "accepted",
    accepted' = true,
    nodeCount' = 1,
    waveCount' = 1,
  }

  action RejectedEmptySequence = all {
    lastScenario' = "rejected_empty_sequence",
    lastOutcome' = "rejected_empty_sequence",
    accepted' = false,
    nodeCount' = 0,
    waveCount' = 0,
  }

  action RejectedCycle = all {
    lastScenario' = "rejected_cycle",
    lastOutcome' = "rejected_cycle",
    accepted' = false,
    nodeCount' = 2,
    waveCount' = 0,
  }

  action RejectedUnknownDependency = all {
    lastScenario' = "rejected_unknown_dependency",
    lastOutcome' = "rejected_unknown_dependency",
    accepted' = false,
    nodeCount' = 1,
    waveCount' = 0,
  }

  action RejectedUnknownInput = all {
    lastScenario' = "rejected_unknown_input",
    lastOutcome' = "rejected_unknown_input",
    accepted' = false,
    nodeCount' = 1,
    waveCount' = 0,
  }

  action RejectedStrongGsi = all {
    lastScenario' = "rejected_strong_gsi",
    lastOutcome' = "rejected_strong_gsi",
    accepted' = false,
    nodeCount' = 1,
    waveCount' = 0,
  }

  action RejectedTransactionalGsi = all {
    lastScenario' = "rejected_transactional_gsi",
    lastOutcome' = "rejected_transactional_gsi",
    accepted' = false,
    nodeCount' = 1,
    waveCount' = 0,
  }

  action RejectedNodeLimit = all {
    lastScenario' = "rejected_node_limit",
    lastOutcome' = "rejected_node_limit",
    accepted' = false,
    nodeCount' = 9,
    waveCount' = 0,
  }

  action RejectedFanoutCap = all {
    lastScenario' = "rejected_fanout_cap",
    lastOutcome' = "rejected_fanout_cap",
    accepted' = false,
    nodeCount' = 1,
    waveCount' = 0,
  }

  action step = any {
    AcceptedLateParent,
    AcceptedTransactionalPoint,
    RejectedEmptySequence,
    RejectedCycle,
    RejectedUnknownDependency,
    RejectedUnknownInput,
    RejectedStrongGsi,
    RejectedTransactionalGsi,
    RejectedNodeLimit,
    RejectedFanoutCap,
  }

  val inv =
    (accepted and lastOutcome == "accepted")
      or (not(accepted) and lastOutcome != "accepted")
}
