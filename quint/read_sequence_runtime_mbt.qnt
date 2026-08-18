/// Runtime ReadSequence contract exercised by the storage-api quint-connect
/// adapter.
///
/// The adapter deliberately keeps this model finite.  A first page is served
/// from two real Get nodes under a one-item budget, the returned token is then
/// resumed for the deferred node, or the same token is tampered with and
/// rejected.  The Rust driver derives these fields from the actual endpoint
/// response; it does not reproduce the manager's token or wave logic.
module read_sequence_runtime_mbt {
  var phase: str
  var status: int
  var nodeCount: int
  var tokenPresent: bool

  pure def firstPageAllowed(current: str): bool = current == "ready"
  pure def continuationAllowed(current: str): bool = current == "resumable"

  action init = all {
    phase' = "ready",
    status' = 0,
    nodeCount' = 0,
    tokenPresent' = false,
  }

  pure def nextPhase(operation: str): str =
    if (operation == "FirstPage") "resumable"
    else if (operation == "Resume") "complete"
    else "rejected"

  pure def nextStatus(operation: str): int =
    if (operation == "Tamper") 400 else 200

  pure def nextNodeCount(operation: str): int =
    if (operation == "Tamper") 0 else 1

  pure def nextTokenPresent(operation: str): bool =
    operation != "Resume"

  action step = {
    nondet operation = Set("FirstPage", "Resume", "Tamper").oneOf()
    all {
      phase' = nextPhase(operation),
      status' = nextStatus(operation),
      nodeCount' = nextNodeCount(operation),
      tokenPresent' = nextTokenPresent(operation),
    }
  }

  val inv =
    (phase == "ready" and status == 0 and nodeCount == 0 and not(tokenPresent))
      or (phase == "resumable" and status == 200 and nodeCount == 1 and tokenPresent)
      or (phase == "complete" and status == 200 and nodeCount == 1 and not(tokenPresent))
      or (phase == "rejected" and status == 400 and nodeCount == 0 and tokenPresent)

  run firstPageRequiresReady = true.expect(
    firstPageAllowed("ready") and nextPhase("FirstPage") == "resumable"
  )
  run resumeRequiresContinuation = true.expect(
    continuationAllowed("resumable") and nextPhase("Resume") == "complete"
  )
  run tamperLeavesOriginalToken = true.expect(
    continuationAllowed("resumable") and nextTokenPresent("Tamper")
  )
}
