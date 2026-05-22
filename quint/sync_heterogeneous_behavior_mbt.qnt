/// Composite MBT surface for heterogeneous sync-replication behavior gates.
module sync_heterogeneous_behavior_mbt {
  type HeterogeneousCase = {
    sourceBackend: str,
    destinationBackend: str,
    logicalSnapshotDomainsComplete: bool,
    resolvedApplyComplete: bool,
    learnerPromoted: bool,
    bootstrapDestinationEmpty: bool,
    bootstrapPreflightMarkerPresent: bool,
    syncRole: str,
    currentVersion: int,
    incomingVersion: int,
  }

  val nonRemoteBackends = Set("sqlite", "postgres", "turso", "rocksdb", "foundationdb")

  var lastCase: HeterogeneousCase
  var lastBackendPairDecision: str
  var lastLogicalSnapshotDecision: str
  var lastResolvedApplyDecision: str
  var lastPromotedSurfaceDecision: str
  var lastBootstrapPreflightDecision: str
  var lastOutboundDecision: str
  var lastInboundDecision: str

  pure def backendPairDecision(source, destination) =
    if (source == "" or destination == "") {
      "rejected"
    } else if (source == "remote" or destination == "remote") {
      "rejected"
    } else if (not(nonRemoteBackends.contains(source)) or not(nonRemoteBackends.contains(destination))) {
      "rejected"
    } else if (source == destination) {
      "production_supported"
    } else {
      "validation_only"
    }

  pure def logicalSnapshotDecision(pairDecision, domainsComplete) =
    if (pairDecision == "rejected") {
      "block"
    } else if (not(domainsComplete)) {
      "block"
    } else {
      "allow"
    }

  pure def resolvedApplyDecision(pairDecision, destination, complete) =
    if (pairDecision == "rejected") {
      "block"
    } else if (not(Set("postgres", "turso", "rocksdb", "foundationdb").contains(destination))) {
      "block"
    } else if (not(complete)) {
      "block"
    } else {
      "allow"
    }

  pure def promotedSurfaceDecision(pairDecision, domainsComplete, resolvedComplete, promoted) =
    if (pairDecision == "rejected") {
      "block"
    } else if (not(promoted)) {
      "block"
    } else if (not(domainsComplete)) {
      "block"
    } else if (not(resolvedComplete)) {
      "block"
    } else {
      "allow"
    }

  pure def bootstrapPreflightDecision(destinationEmpty, markerPresent) =
    if (markerPresent) {
      "allow_retry_after_preflight"
    } else if (destinationEmpty) {
      "allow_empty_destination"
    } else {
      "reject_non_empty_destination"
    }

  pure def outboundDecision(role) =
    if (role == "leader") "owns_sender" else "standby"

  pure def inboundDecision(currentVersion, incomingVersion) =
    if (incomingVersion > currentVersion) "apply" else "skip_stale"

  action init = all {
    lastCase' = {
      sourceBackend: "sqlite",
      destinationBackend: "sqlite",
      logicalSnapshotDomainsComplete: true,
      resolvedApplyComplete: false,
      learnerPromoted: false,
      bootstrapDestinationEmpty: true,
      bootstrapPreflightMarkerPresent: false,
      syncRole: "disabled",
      currentVersion: 0,
      incomingVersion: 0,
    },
    lastBackendPairDecision' = "not_checked",
    lastLogicalSnapshotDecision' = "not_checked",
    lastResolvedApplyDecision' = "not_checked",
    lastPromotedSurfaceDecision' = "not_checked",
    lastBootstrapPreflightDecision' = "not_checked",
    lastOutboundDecision' = "not_checked",
    lastInboundDecision' = "not_checked",
  }

  action Check = {
    nondet sourceBackend = Set("sqlite", "postgres", "turso", "rocksdb", "foundationdb", "remote", "unknown", "").oneOf()
    nondet destinationBackend = Set("sqlite", "postgres", "turso", "rocksdb", "foundationdb", "remote", "unknown", "").oneOf()
    nondet logicalSnapshotDomainsComplete = Set(true, false).oneOf()
    nondet resolvedApplyComplete = Set(true, false).oneOf()
    nondet learnerPromoted = Set(true, false).oneOf()
    nondet bootstrapDestinationEmpty = Set(true, false).oneOf()
    nondet bootstrapPreflightMarkerPresent = Set(true, false).oneOf()
    nondet syncRole = Set("disabled", "learner", "follower", "candidate", "leader").oneOf()
    nondet currentVersion = Set(0, 1, 2).oneOf()
    nondet incomingVersion = Set(0, 1, 2, 3).oneOf()
    pure val pairDecision = backendPairDecision(sourceBackend, destinationBackend)
    all {
      lastCase' = {
        sourceBackend: sourceBackend,
        destinationBackend: destinationBackend,
        logicalSnapshotDomainsComplete: logicalSnapshotDomainsComplete,
        resolvedApplyComplete: resolvedApplyComplete,
        learnerPromoted: learnerPromoted,
        bootstrapDestinationEmpty: bootstrapDestinationEmpty,
        bootstrapPreflightMarkerPresent: bootstrapPreflightMarkerPresent,
        syncRole: syncRole,
        currentVersion: currentVersion,
        incomingVersion: incomingVersion,
      },
      lastBackendPairDecision' = pairDecision,
      lastLogicalSnapshotDecision' = logicalSnapshotDecision(pairDecision, logicalSnapshotDomainsComplete),
      lastResolvedApplyDecision' = resolvedApplyDecision(pairDecision, destinationBackend, resolvedApplyComplete),
      lastPromotedSurfaceDecision' = promotedSurfaceDecision(
        pairDecision,
        logicalSnapshotDomainsComplete,
        resolvedApplyComplete,
        learnerPromoted
      ),
      lastBootstrapPreflightDecision' = bootstrapPreflightDecision(
        bootstrapDestinationEmpty,
        bootstrapPreflightMarkerPresent
      ),
      lastOutboundDecision' = outboundDecision(syncRole),
      lastInboundDecision' = inboundDecision(currentVersion, incomingVersion),
    }
  }

  action step = Check

  val inv = all {
    lastLogicalSnapshotDecision != "allow" or lastBackendPairDecision != "rejected",
    lastResolvedApplyDecision != "allow" or lastBackendPairDecision != "rejected",
    lastPromotedSurfaceDecision != "allow" or lastLogicalSnapshotDecision == "allow",
    lastPromotedSurfaceDecision != "allow" or lastResolvedApplyDecision == "allow",
    lastBootstrapPreflightDecision != "reject_non_empty_destination"
      or not(lastCase.bootstrapDestinationEmpty)
      and not(lastCase.bootstrapPreflightMarkerPresent),
    lastOutboundDecision != "owns_sender" or lastCase.syncRole == "leader",
    lastInboundDecision != "apply" or lastCase.incomingVersion > lastCase.currentVersion,
  }

  run mixedLogicalSnapshotInstallAllowed =
    true.expect(logicalSnapshotDecision(backendPairDecision("sqlite", "postgres"), true) == "allow")

  run mixedResolvedApplyReplayAllowed =
    true.expect(resolvedApplyDecision(backendPairDecision("sqlite", "foundationdb"), "foundationdb", true) == "allow")

  run mixedPromotionSurfaceAllowed =
    true.expect(promotedSurfaceDecision(backendPairDecision("sqlite", "turso"), true, true, true) == "allow")

  run mixedBootstrapActivationAllowedAfterPreflight =
    true.expect(bootstrapPreflightDecision(false, true) == "allow_retry_after_preflight")

  run unsupportedRemoteCombinationRejected =
    true.expect(backendPairDecision("sqlite", "remote") == "rejected")

  run missingResolvedApplyBlocksPromotion =
    true.expect(promotedSurfaceDecision(backendPairDecision("sqlite", "postgres"), true, false, true) == "block")
}
