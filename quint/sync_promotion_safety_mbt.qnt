/// Compact MBT surface for sync learner auto-promotion safety.
///
/// Protects the operator-free promotion gates used by SyncAutoPromotionController.
module sync_promotion_safety_mbt {
  type PromotionGate = {
    learnerAlreadyVoter: bool,
    importComplete: bool,
    checksumValidated: bool,
    pendingRequiredDomains: int,
    protectedStreamDrained: bool,
    tombstoneCleanupReady: bool,
    appliedIndex: int,
    promotionDecisionIndex: int,
    peerConnectivityHealthy: bool,
    membershipContainsLearner: bool,
    conflictingClusterIdentity: bool,
  }

  var lastGate: PromotionGate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (not(gate.importComplete)) {
      "import_incomplete"
    } else if (not(gate.checksumValidated)) {
      "checksum_not_validated"
    } else if (gate.pendingRequiredDomains != 0) {
      "required_domains_pending"
    } else if (not(gate.protectedStreamDrained)) {
      "protected_stream_not_drained"
    } else if (not(gate.tombstoneCleanupReady)) {
      "tombstone_cleanup_not_ready"
    } else if (gate.appliedIndex < gate.promotionDecisionIndex) {
      "applied_index_behind_promotion_decision"
    } else if (not(gate.peerConnectivityHealthy)) {
      "peer_connectivity_unhealthy"
    } else if (not(gate.membershipContainsLearner)) {
      "learner_missing_from_membership"
    } else if (gate.conflictingClusterIdentity) {
      "conflicting_cluster_identity"
    } else if (gate.learnerAlreadyVoter) {
      "already_promoted"
    } else {
      "allow"
    }

  action init = all {
    lastGate' = {
      importComplete: false,
      checksumValidated: false,
      pendingRequiredDomains: 1,
      protectedStreamDrained: false,
      tombstoneCleanupReady: false,
      appliedIndex: 0,
      promotionDecisionIndex: 1,
      peerConnectivityHealthy: false,
      membershipContainsLearner: false,
      conflictingClusterIdentity: true,
      learnerAlreadyVoter: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet importComplete = Set(false, true).oneOf()
    nondet checksumValidated = Set(false, true).oneOf()
    nondet pendingRequiredDomains = Set(0, 1, 2).oneOf()
    nondet protectedStreamDrained = Set(false, true).oneOf()
    nondet tombstoneCleanupReady = Set(false, true).oneOf()
    nondet appliedIndex = Set(0, 1, 2).oneOf()
    nondet promotionDecisionIndex = Set(0, 1, 2).oneOf()
    nondet peerConnectivityHealthy = Set(false, true).oneOf()
    nondet membershipContainsLearner = Set(false, true).oneOf()
    nondet conflictingClusterIdentity = Set(false, true).oneOf()
    nondet learnerAlreadyVoter = Set(false, true).oneOf()
    val gate = {
      learnerAlreadyVoter: learnerAlreadyVoter,
      importComplete: importComplete,
      checksumValidated: checksumValidated,
      pendingRequiredDomains: pendingRequiredDomains,
      protectedStreamDrained: protectedStreamDrained,
      tombstoneCleanupReady: tombstoneCleanupReady,
      appliedIndex: appliedIndex,
      promotionDecisionIndex: promotionDecisionIndex,
      peerConnectivityHealthy: peerConnectivityHealthy,
      membershipContainsLearner: membershipContainsLearner,
      conflictingClusterIdentity: conflictingClusterIdentity,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "allow"
      or all {
        lastGate.importComplete,
        lastGate.checksumValidated,
        lastGate.pendingRequiredDomains == 0,
        lastGate.protectedStreamDrained,
        lastGate.tombstoneCleanupReady,
        lastGate.appliedIndex >= lastGate.promotionDecisionIndex,
        lastGate.peerConnectivityHealthy,
        lastGate.membershipContainsLearner,
        not(lastGate.conflictingClusterIdentity),
      }

  run importRequired =
    true.expect(expectedDecision({
      importComplete: false,
      checksumValidated: true,
      pendingRequiredDomains: 0,
      protectedStreamDrained: true,
      tombstoneCleanupReady: true,
      appliedIndex: 2,
      promotionDecisionIndex: 1,
      peerConnectivityHealthy: true,
      membershipContainsLearner: true,
      conflictingClusterIdentity: false,
      learnerAlreadyVoter: false,
    }) == "import_incomplete")

  run appliedIndexRequired =
    true.expect(expectedDecision({
      importComplete: true,
      checksumValidated: true,
      pendingRequiredDomains: 0,
      protectedStreamDrained: true,
      tombstoneCleanupReady: true,
      appliedIndex: 0,
      promotionDecisionIndex: 1,
      peerConnectivityHealthy: true,
      membershipContainsLearner: true,
      conflictingClusterIdentity: false,
      learnerAlreadyVoter: false,
    }) == "applied_index_behind_promotion_decision")

  run allGatesAllow =
    true.expect(expectedDecision({
      importComplete: true,
      checksumValidated: true,
      pendingRequiredDomains: 0,
      protectedStreamDrained: true,
      tombstoneCleanupReady: true,
      appliedIndex: 2,
      promotionDecisionIndex: 1,
      peerConnectivityHealthy: true,
      membershipContainsLearner: true,
      conflictingClusterIdentity: false,
      learnerAlreadyVoter: false,
    }) == "allow")

  run alreadyPromotedIsIdempotent =
    true.expect(expectedDecision({
      learnerAlreadyVoter: true,
      importComplete: true,
      checksumValidated: true,
      pendingRequiredDomains: 0,
      protectedStreamDrained: true,
      tombstoneCleanupReady: true,
      appliedIndex: 2,
      promotionDecisionIndex: 1,
      peerConnectivityHealthy: true,
      membershipContainsLearner: true,
      conflictingClusterIdentity: false,
    }) == "already_promoted")
}
