/// Sync learner catchup bughunt model.
///
/// Exercises restart/failover during chunked catchup and forbids promotion
/// before a complete imported manifest and checkpoint are available.
module sync_learner_catchup_bughunt {
  var manifestStarted: bool
  var importedChunks: int
  var requiredChunks: int
  var checkpointPersisted: bool
  var sourceGeneration: int
  var learnerRestarted: bool
  var promoted: bool
  var lastOutcome: str

  pure def importComplete(imported, required) = imported >= required

  pure def canPromote(started, imported, required, checkpoint) =
    started and checkpoint and importComplete(imported, required)

  action init = all {
    manifestStarted' = false,
    importedChunks' = 0,
    requiredChunks' = 2,
    checkpointPersisted' = false,
    sourceGeneration' = 0,
    learnerRestarted' = false,
    promoted' = false,
    lastOutcome' = "pending",
  }

  action StartManifest = all {
    manifestStarted' = true,
    importedChunks' = importedChunks,
    requiredChunks' = requiredChunks,
    checkpointPersisted' = checkpointPersisted,
    sourceGeneration' = sourceGeneration,
    learnerRestarted' = learnerRestarted,
    promoted' = promoted,
    lastOutcome' = "manifest_started",
  }

  action ImportChunk = all {
    manifestStarted,
    importedChunks < requiredChunks,
    manifestStarted' = manifestStarted,
    importedChunks' = importedChunks + 1,
    requiredChunks' = requiredChunks,
    checkpointPersisted' = importedChunks + 1 >= requiredChunks,
    sourceGeneration' = sourceGeneration,
    learnerRestarted' = learnerRestarted,
    promoted' = promoted,
    lastOutcome' = "chunk_imported",
  }

  action RestartLearner = all {
    manifestStarted' = manifestStarted,
    importedChunks' = importedChunks,
    requiredChunks' = requiredChunks,
    checkpointPersisted' = checkpointPersisted,
    sourceGeneration' = sourceGeneration,
    learnerRestarted' = true,
    promoted' = promoted,
    lastOutcome' = "learner_restarted",
  }

  action FailoverSource = all {
    manifestStarted' = manifestStarted,
    importedChunks' = importedChunks,
    requiredChunks' = requiredChunks,
    checkpointPersisted' = checkpointPersisted,
    sourceGeneration' = sourceGeneration + 1,
    learnerRestarted' = learnerRestarted,
    promoted' = promoted,
    lastOutcome' = "source_failed_over",
  }

  action TryPromote = all {
    manifestStarted' = manifestStarted,
    importedChunks' = importedChunks,
    requiredChunks' = requiredChunks,
    checkpointPersisted' = checkpointPersisted,
    sourceGeneration' = sourceGeneration,
    learnerRestarted' = learnerRestarted,
    promoted' = canPromote(manifestStarted, importedChunks, requiredChunks, checkpointPersisted),
    lastOutcome' = if (canPromote(manifestStarted, importedChunks, requiredChunks, checkpointPersisted)) "promoted" else "promotion_blocked",
  }

  action step = any {
    StartManifest,
    ImportChunk,
    RestartLearner,
    FailoverSource,
    TryPromote,
  }

  val inv =
    not(promoted)
      or canPromote(manifestStarted, importedChunks, requiredChunks, checkpointPersisted)

  val restart_witness = learnerRestarted and importComplete(importedChunks, requiredChunks)
  val failover_witness = sourceGeneration > 0 and importComplete(importedChunks, requiredChunks)
  val promotion_blocked_witness = lastOutcome == "promotion_blocked"

  run promotionRequiresCompleteCatchup =
    false.expect(canPromote(true, 1, 2, false))

  run promotionAllowedAfterCompleteCatchup =
    true.expect(canPromote(true, 2, 2, true))
}
