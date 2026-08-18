/// Compact MBT contract for the ReadSequence continuation-token codec.
///
/// The route-level ReadSequence model spans planner, executor, and token
/// ownership boundaries. This owner-local model keeps the codec comparison
/// small: equivalent frontiers canonicalize to the same decoded sets and a
/// tampered payload is rejected by the Rust integrity check.
module read_sequence_token_mbt {
  pure val FRONTIER = Set(0, 1, 2, 3)

  type TokenCase = {
    completedNodes: Set[int],
    continuationNodes: Set[int],
    tampered: bool,
  }

  var lastCase: TokenCase
  var lastOutcome: str
  var decodedCompletedNodes: Set[int]
  var decodedContinuationNodes: Set[int]

  pure def initialCase = {
    completedNodes: Set(),
    continuationNodes: Set(),
    tampered: false,
  }

  action init = all {
    lastCase' = initialCase,
    lastOutcome' = "not_checked",
    decodedCompletedNodes' = Set(),
    decodedContinuationNodes' = Set(),
  }

  action Check = {
    nondet completedNodes = FRONTIER.powerset().oneOf()
    nondet continuationNodes = FRONTIER.powerset().oneOf()
    nondet tampered = Set(true, false).oneOf()
    val tokenCase = {
      completedNodes: completedNodes,
      continuationNodes: continuationNodes,
      tampered: tampered,
    }
    all {
      lastCase' = tokenCase,
      lastOutcome' = if (tampered) "rejected" else "accepted",
      decodedCompletedNodes' = if (tampered) Set() else completedNodes,
      decodedContinuationNodes' = if (tampered) Set() else continuationNodes,
    }
  }

  action step = Check

  val inv =
    lastOutcome != "accepted"
      or decodedCompletedNodes == lastCase.completedNodes
        and decodedContinuationNodes == lastCase.continuationNodes

  pure def exampleFrontier = {
    completedNodes: Set(0, 2),
    continuationNodes: Set(1, 3),
    tampered: false,
  }

  run canonicalFrontierExample = true.expect(
    exampleFrontier.completedNodes == Set(0, 2)
      and exampleFrontier.continuationNodes == Set(1, 3)
      and not(exampleFrontier.tampered)
  )
}
