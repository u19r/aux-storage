/// Multi-region replica activation gate model.
///
/// Protects Invariant Inventory ID MULTI-2 from docs/quint-backfill.md.
/// Logical backfill completion is intentionally abstract in this model.
module multi_region_bootstrap_gate {
  var bootstrapComplete: bool
  var protectedBoundaryHeld: bool
  var streamHistoryAvailable: bool
  var replicaActivated: bool
  var lastOutcome: str

  pure def canActivate(bootstrap, boundary, _history) =
    bootstrap and boundary

  def activationRequiresBackfill =
    not(replicaActivated) or canActivate(bootstrapComplete, protectedBoundaryHeld, streamHistoryAvailable)

  action init = all {
    bootstrapComplete' = false,
    protectedBoundaryHeld' = false,
    streamHistoryAvailable' = true,
    replicaActivated' = false,
    lastOutcome' = "pending",
  }

  action CompleteBootstrap = all {
    bootstrapComplete' = true,
    protectedBoundaryHeld' = protectedBoundaryHeld,
    streamHistoryAvailable' = streamHistoryAvailable,
    replicaActivated' = replicaActivated,
    lastOutcome' = "bootstrap_complete",
  }

  action ProtectBoundary = all {
    bootstrapComplete' = bootstrapComplete,
    protectedBoundaryHeld' = true,
    streamHistoryAvailable' = streamHistoryAvailable,
    replicaActivated' = replicaActivated,
    lastOutcome' = "boundary_protected",
  }

  action LoseHistory = all {
    bootstrapComplete' = bootstrapComplete,
    protectedBoundaryHeld' = protectedBoundaryHeld,
    streamHistoryAvailable' = false,
    replicaActivated' = replicaActivated,
    lastOutcome' = "history_unavailable",
  }

  action TryActivate = all {
    bootstrapComplete' = bootstrapComplete,
    protectedBoundaryHeld' = protectedBoundaryHeld,
    streamHistoryAvailable' = streamHistoryAvailable,
    replicaActivated' = canActivate(bootstrapComplete, protectedBoundaryHeld, streamHistoryAvailable),
    lastOutcome' = if (canActivate(bootstrapComplete, protectedBoundaryHeld, streamHistoryAvailable)) "activated" else "activation_blocked",
  }

  action step = any {
    CompleteBootstrap,
    ProtectBoundary,
    LoseHistory,
    TryActivate,
  }

  val inv = activationRequiresBackfill

  val activation_success_witness = replicaActivated
  val activation_rejected_witness = lastOutcome == "activation_blocked"
  val activation_after_unretained_history_witness = replicaActivated and not(streamHistoryAvailable)

  run activationRequiresAbstractBackfill =
    false.expect(canActivate(false, true, true))

  run activationSucceedsAfterBackfillBoundaryAndHistory =
    true.expect(canActivate(true, true, true))

  run activationSucceedsAfterLogicalBackfillWithoutFullStreamHistory =
    true.expect(canActivate(true, true, false))
}
