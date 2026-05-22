/// Compact MBT surface for sync Raft peer HTTP status classification.
///
/// Protects peer-auth failures from being treated as ordinary peer errors.
module sync_peer_auth_transport_mbt {
  var lastStatus: int
  var lastDecision: str
  var lastMessage: str

  pure def expectedDecision(status) =
    if (status == 401 or status == 403) {
      "authentication_failed"
    } else {
      "peer_returned_error"
    }

  pure def expectedMessage(decision) =
    if (decision == "authentication_failed") {
      "sync raft peer authentication failed"
    } else {
      "sync raft peer returned error"
    }

  action init = all {
    lastStatus' = 500,
    lastDecision' = "not_checked",
    lastMessage' = "not_checked",
  }

  action Check = {
    nondet status = Set(400, 401, 403, 404, 409, 429, 500, 503).oneOf()
    all {
      lastStatus' = status,
      lastDecision' = expectedDecision(status),
      lastMessage' = expectedMessage(expectedDecision(status)),
    }
  }

  action step = Check

  val inv =
    all {
      (lastDecision == "authentication_failed") == (lastStatus == 401 or lastStatus == 403),
      lastMessage == expectedMessage(lastDecision),
    }

  run unauthorizedIsAuthFailure =
    true.expect(expectedDecision(401) == "authentication_failed")

  run forbiddenIsAuthFailure =
    true.expect(expectedDecision(403) == "authentication_failed")

  run serviceUnavailableIsPeerError =
    true.expect(expectedDecision(503) == "peer_returned_error")
}
