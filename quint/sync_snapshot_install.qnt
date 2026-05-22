/// Sync snapshot install gate model.
///
/// Protects Invariant Inventory ID SYNC-2 from docs/quint-backfill.md.
module sync_snapshot_install {
  var snapshotVerified: bool
  var logPrefixCovered: bool
  var installCommitted: bool
  var postBoundaryLogBuffered: bool
  var postBoundaryLogApplied: bool
  var learnerActive: bool
  var lastOutcome: str

  pure def canActivateFromSnapshot(verified, covered, committed) =
    verified and covered and committed

  def snapshotActivationRequiresVerifiedCommit =
    not(learnerActive) or canActivateFromSnapshot(snapshotVerified, logPrefixCovered, installCommitted)

  def postBoundaryLogRequiresInstall =
    not(postBoundaryLogApplied) or installCommitted

  action init = all {
    snapshotVerified' = false,
    logPrefixCovered' = false,
    installCommitted' = false,
    postBoundaryLogBuffered' = false,
    postBoundaryLogApplied' = false,
    learnerActive' = false,
    lastOutcome' = "pending",
  }

  action VerifySnapshot = all {
    snapshotVerified' = true,
    logPrefixCovered' = logPrefixCovered,
    installCommitted' = installCommitted,
    postBoundaryLogBuffered' = postBoundaryLogBuffered,
    postBoundaryLogApplied' = postBoundaryLogApplied,
    learnerActive' = learnerActive,
    lastOutcome' = "snapshot_verified",
  }

  action CoverLogPrefix = all {
    snapshotVerified' = snapshotVerified,
    logPrefixCovered' = true,
    installCommitted' = installCommitted,
    postBoundaryLogBuffered' = postBoundaryLogBuffered,
    postBoundaryLogApplied' = postBoundaryLogApplied,
    learnerActive' = learnerActive,
    lastOutcome' = "log_prefix_covered",
  }

  action CommitInstall = all {
    snapshotVerified' = snapshotVerified,
    logPrefixCovered' = logPrefixCovered,
    installCommitted' = snapshotVerified and logPrefixCovered,
    postBoundaryLogBuffered' = postBoundaryLogBuffered,
    postBoundaryLogApplied' = postBoundaryLogApplied,
    learnerActive' = learnerActive,
    lastOutcome' = if (snapshotVerified and logPrefixCovered) "install_committed" else "install_blocked",
  }

  action BufferPostBoundaryLog = all {
    snapshotVerified' = snapshotVerified,
    logPrefixCovered' = logPrefixCovered,
    installCommitted' = installCommitted,
    postBoundaryLogBuffered' = true,
    postBoundaryLogApplied' = postBoundaryLogApplied,
    learnerActive' = learnerActive,
    lastOutcome' = "post_boundary_log_buffered",
  }

  action ApplyPostBoundaryLog = all {
    postBoundaryLogBuffered,
    installCommitted,
    snapshotVerified' = snapshotVerified,
    logPrefixCovered' = logPrefixCovered,
    installCommitted' = installCommitted,
    postBoundaryLogBuffered' = postBoundaryLogBuffered,
    postBoundaryLogApplied' = true,
    learnerActive' = learnerActive,
    lastOutcome' = "post_boundary_log_applied",
  }

  action RejectEarlyPostBoundaryLog = all {
    snapshotVerified' = snapshotVerified,
    logPrefixCovered' = logPrefixCovered,
    installCommitted' = installCommitted,
    postBoundaryLogBuffered' = postBoundaryLogBuffered,
    postBoundaryLogApplied' = postBoundaryLogApplied,
    learnerActive' = learnerActive,
    lastOutcome' = if (installCommitted) lastOutcome else "post_boundary_log_blocked",
  }

  action TryActivateLearner = all {
    snapshotVerified' = snapshotVerified,
    logPrefixCovered' = logPrefixCovered,
    installCommitted' = installCommitted,
    postBoundaryLogBuffered' = postBoundaryLogBuffered,
    postBoundaryLogApplied' = postBoundaryLogApplied,
    learnerActive' = canActivateFromSnapshot(snapshotVerified, logPrefixCovered, installCommitted),
    lastOutcome' = if (canActivateFromSnapshot(snapshotVerified, logPrefixCovered, installCommitted)) "learner_active" else "activation_blocked",
  }

  action step = any {
    VerifySnapshot,
    CoverLogPrefix,
    CommitInstall,
    BufferPostBoundaryLog,
    ApplyPostBoundaryLog,
    RejectEarlyPostBoundaryLog,
    TryActivateLearner,
  }

  val inv = snapshotActivationRequiresVerifiedCommit and postBoundaryLogRequiresInstall

  val snapshot_activation_success_witness = learnerActive
  val snapshot_activation_rejected_witness = lastOutcome == "activation_blocked"
  val post_boundary_log_blocked_witness = lastOutcome == "post_boundary_log_blocked"
  val post_boundary_log_applied_witness = postBoundaryLogApplied

  run snapshotActivationRequiresCommit =
    false.expect(canActivateFromSnapshot(true, true, false))

  run snapshotActivationSucceedsAfterCommit =
    true.expect(canActivateFromSnapshot(true, true, true))
}
