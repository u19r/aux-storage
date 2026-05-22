/// Compact MBT surface for sync proposal admission.
///
/// Protects operation-count, byte-count, and queue-depth rejection ordering
/// before runtime proposal batching/backpressure is accepted as model-backed.
module sync_proposal_admission_mbt {
  type Limits = {
    maxOperations: int,
    maxBytes: int,
    maxQueueDepth: int,
  }

  type Gate = {
    operationCount: int,
    byteCount: int,
    inFlight: int,
  }

  var lastLimits: Limits
  var lastGate: Gate
  var lastDecision: str

  pure def expectedDecision(limits, gate) =
    if (gate.operationCount > limits.maxOperations) {
      "reject_operation_count"
    } else if (gate.byteCount > limits.maxBytes) {
      "reject_byte_count"
    } else if (gate.inFlight >= limits.maxQueueDepth) {
      "reject_queue_full"
    } else {
      "admit"
    }

  action init = all {
    lastLimits' = {
      maxOperations: 1,
      maxBytes: 1,
      maxQueueDepth: 1,
    },
    lastGate' = {
      operationCount: 1,
      byteCount: 1,
      inFlight: 0,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet maxOperations = Set(1, 2, 3).oneOf()
    nondet maxBytes = Set(1, 2, 3).oneOf()
    nondet maxQueueDepth = Set(1, 2, 3).oneOf()
    nondet operationCount = Set(1, 2, 3, 4).oneOf()
    nondet byteCount = Set(1, 2, 3, 4).oneOf()
    nondet inFlight = Set(0, 1, 2, 3).oneOf()
    val limits = {
      maxOperations: maxOperations,
      maxBytes: maxBytes,
      maxQueueDepth: maxQueueDepth,
    }
    val gate = {
      operationCount: operationCount,
      byteCount: byteCount,
      inFlight: inFlight,
    }
    all {
      lastLimits' = limits,
      lastGate' = gate,
      lastDecision' = expectedDecision(limits, gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "admit"
      or all {
        lastGate.operationCount <= lastLimits.maxOperations,
        lastGate.byteCount <= lastLimits.maxBytes,
        lastGate.inFlight < lastLimits.maxQueueDepth,
      }

  run operationLimitRejectsBeforeByteLimit =
    true.expect(expectedDecision(
      { maxOperations: 1, maxBytes: 1, maxQueueDepth: 1 },
      { operationCount: 2, byteCount: 2, inFlight: 1 }
    ) == "reject_operation_count")

  run byteLimitRejectsBeforeQueueLimit =
    true.expect(expectedDecision(
      { maxOperations: 2, maxBytes: 1, maxQueueDepth: 1 },
      { operationCount: 1, byteCount: 2, inFlight: 1 }
    ) == "reject_byte_count")

  run queueLimitRejectsAtDepth =
    true.expect(expectedDecision(
      { maxOperations: 2, maxBytes: 2, maxQueueDepth: 1 },
      { operationCount: 1, byteCount: 1, inFlight: 1 }
    ) == "reject_queue_full")

  run admitWhenWithinLimits =
    true.expect(expectedDecision(
      { maxOperations: 2, maxBytes: 2, maxQueueDepth: 2 },
      { operationCount: 1, byteCount: 1, inFlight: 1 }
    ) == "admit")
}
