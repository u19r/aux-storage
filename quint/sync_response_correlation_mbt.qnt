/// Compact MBT surface for sync proposal response correlation.
///
/// Protects index/default/required behavior before public API handlers decode
/// responses returned from committed sync proposals.
module sync_response_correlation_mbt {
  type Gate = {
    responseCount: int,
    index: int,
    payloadPresent: bool,
    required: bool,
  }

  var lastGate: Gate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (gate.index >= gate.responseCount) {
      if (gate.required) "missing_entry" else "use_default"
    } else if (gate.payloadPresent) {
      "decode_payload"
    } else if (gate.required) {
      "missing_payload"
    } else {
      "use_default"
    }

  action init = all {
    lastGate' = {
      responseCount: 0,
      index: 0,
      payloadPresent: false,
      required: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet responseCount = Set(0, 1, 2).oneOf()
    nondet index = Set(0, 1, 2).oneOf()
    nondet payloadPresent = Set(false, true).oneOf()
    nondet required = Set(false, true).oneOf()
    val gate = {
      responseCount: responseCount,
      index: index,
      payloadPresent: payloadPresent,
      required: required,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "decode_payload" or lastGate.index < lastGate.responseCount,
      lastDecision != "missing_entry" or lastGate.required,
      lastDecision != "missing_payload" or lastGate.required,
    }

  run optionalMissingEntryUsesDefault =
    true.expect(expectedDecision({
      responseCount: 0,
      index: 0,
      payloadPresent: false,
      required: false,
    }) == "use_default")

  run requiredMissingEntryRejects =
    true.expect(expectedDecision({
      responseCount: 0,
      index: 0,
      payloadPresent: false,
      required: true,
    }) == "missing_entry")

  run requiredMissingPayloadRejects =
    true.expect(expectedDecision({
      responseCount: 1,
      index: 0,
      payloadPresent: false,
      required: true,
    }) == "missing_payload")

  run payloadDecodes =
    true.expect(expectedDecision({
      responseCount: 1,
      index: 0,
      payloadPresent: true,
      required: true,
    }) == "decode_payload")
}
