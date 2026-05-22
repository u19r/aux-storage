/// Compact MBT surface for sync backend-pair admission.
///
/// Protects the support matrix so mixed-backend catchup cannot be enabled
/// accidentally by permissive fallback logic.
module sync_backend_pair_policy_mbt {
  type BackendPair = {
    source: str,
    destination: str,
  }

  var lastPair: BackendPair
  var lastDecision: str
  var lastReason: str

  pure def nonRemoteBackends =
    Set("sqlite", "postgres", "turso", "rocksdb", "foundationdb")

  pure def isNonRemoteBackend(backend) =
    nonRemoteBackends.contains(backend)

  pure def expectedDecision(pair) =
    if (pair.source == "" or pair.destination == "") {
      "rejected"
    } else if (pair.source == "remote" or pair.destination == "remote") {
      "rejected"
    } else if (not(isNonRemoteBackend(pair.source)) or not(isNonRemoteBackend(pair.destination))) {
      "rejected"
    } else if (pair.source == pair.destination) {
      "production_supported"
    } else {
      "validation_only"
    }

  pure def expectedReason(pair) =
    if (pair.source == "" or pair.destination == "") {
      "empty_backend_name"
    } else if (pair.source == "remote" or pair.destination == "remote") {
      "remote_backend"
    } else if (not(isNonRemoteBackend(pair.source)) or not(isNonRemoteBackend(pair.destination))) {
      "unknown_backend"
    } else if (pair.source == pair.destination) {
      "homogeneous_non_remote_backend"
    } else {
      "heterogeneous_non_remote_backend"
    }

  action init = all {
    lastPair' = { source: "", destination: "" },
    lastDecision' = "not_checked",
    lastReason' = "not_checked",
  }

  action Check = {
    nondet source = Set("", "sqlite", "postgres", "turso", "rocksdb", "foundationdb", "remote", "unknown").oneOf()
    nondet destination = Set("", "sqlite", "postgres", "turso", "rocksdb", "foundationdb", "remote", "unknown").oneOf()
    val pair = { source: source, destination: destination }
    all {
      lastPair' = pair,
      lastDecision' = expectedDecision(pair),
      lastReason' = expectedReason(pair),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "production_supported"
        or (
          lastPair.source == lastPair.destination
            and isNonRemoteBackend(lastPair.source)
        ),
      lastDecision != "validation_only"
        or (
          lastPair.source != lastPair.destination
            and isNonRemoteBackend(lastPair.source)
            and isNonRemoteBackend(lastPair.destination)
        ),
      lastReason == expectedReason(lastPair) or lastReason == "not_checked",
    }

  run homogeneousNonRemoteProductionSupported =
    true.expect(expectedDecision({ source: "sqlite", destination: "sqlite" }) == "production_supported")
      and true.expect(expectedDecision({ source: "postgres", destination: "postgres" }) == "production_supported")
      and true.expect(expectedDecision({ source: "turso", destination: "turso" }) == "production_supported")
      and true.expect(expectedDecision({ source: "rocksdb", destination: "rocksdb" }) == "production_supported")
      and true.expect(expectedDecision({ source: "foundationdb", destination: "foundationdb" }) == "production_supported")

  run everyOrderedHeterogeneousNonRemotePairIsValidationOnly =
    nonRemoteBackends.forall(source =>
      nonRemoteBackends.forall(destination =>
        source == destination
          or true.expect(expectedDecision({ source: source, destination: destination }) == "validation_only")
      )
    )

  run everyOrderedHeterogeneousNonRemotePairHasReason =
    nonRemoteBackends.forall(source =>
      nonRemoteBackends.forall(destination =>
        source == destination
          or true.expect(expectedReason({ source: source, destination: destination }) == "heterogeneous_non_remote_backend")
      )
    )

  run unknownRejected =
    true.expect(expectedDecision({ source: "unknown", destination: "sqlite" }) == "rejected")
      and true.expect(expectedDecision({ source: "sqlite", destination: "unknown" }) == "rejected")

  run remoteRejected =
    true.expect(expectedDecision({ source: "remote", destination: "remote" }) == "rejected")
}
