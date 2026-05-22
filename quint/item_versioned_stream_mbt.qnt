/// Compact MBT surface for item-versioned stream Rust/spec comparison.
///
/// Protects Invariant Inventory IDs STREAM-1, STREAM-2, STREAM-3, and STREAM-4.
module item_versioned_stream_mbt {
  pure val NONE_VERSION = 0
  pure val OLD_FORMAT_VERSION = -1
  pure val KEYS = Set("a", "b")

  type StreamCase = {
    key: str,
    operation: str,
    pointerVersion: int,
  }

  var lastCase: StreamCase
  var lastOutcome: str
  var lastVersion: int

  pure def initialCase = {
    key: "a",
    operation: "none",
    pointerVersion: NONE_VERSION,
  }

  pure def expectedOutcome(streamCase, currentVersion) =
    if (streamCase.pointerVersion == OLD_FORMAT_VERSION)
      "rejected_old_format"
    else if (streamCase.operation == "put" or streamCase.operation == "delete")
      "accepted_versioned_mutation"
    else
      "accepted_pointer"

  pure def expectedVersion(streamCase, currentVersion) =
    if (streamCase.pointerVersion == OLD_FORMAT_VERSION)
      currentVersion
    else if (streamCase.operation == "put" or streamCase.operation == "delete")
      currentVersion + 1
    else
      currentVersion

  action init = all {
    lastCase' = initialCase,
    lastOutcome' = "not_checked",
    lastVersion' = NONE_VERSION,
  }

  action Check = {
    nondet key = KEYS.oneOf()
    nondet operation = Set("put", "delete", "read_pointer").oneOf()
    nondet pointerVersion = Set(OLD_FORMAT_VERSION, 0, 1).oneOf()
    val streamCase = { key: key, operation: operation, pointerVersion: pointerVersion }
    all {
      lastCase' = streamCase,
      lastOutcome' = expectedOutcome(streamCase, lastVersion),
      lastVersion' = expectedVersion(streamCase, lastVersion),
    }
  }

  action step = Check

  val inv =
    lastOutcome != "accepted_pointer" or lastCase.pointerVersion != OLD_FORMAT_VERSION

  run acceptsVersionedMutation =
    true.expect(expectedOutcome({ key: "a", operation: "put", pointerVersion: 1 }, 0) == "accepted_versioned_mutation")

  run rejectsOldFormatPointer =
    true.expect(expectedOutcome({ key: "a", operation: "read_pointer", pointerVersion: OLD_FORMAT_VERSION }, 0) == "rejected_old_format")
}
