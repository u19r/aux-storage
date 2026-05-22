/// Sync resolved mutation application model.
///
/// Protects Invariant Inventory ID SYNC-2 from docs/quint-backfill.md.
module sync_resolved_mutation {
  var leaderEvaluatedCondition: bool
  var followerReevaluatedCondition: bool
  var localMetadataAllocated: bool
  var appliedResolvedMutation: bool
  var currentBaseVersion: int
  var resolvedBaseVersion: int
  var operationCount: int
  var appliedOperationCount: int
  var batchOrderPreserved: bool
  var resolvedOutcome: str
  var lastOutcome: str

  pure def canApplyResolvedMutation(leaderEvaluated, followerReevaluated, metadataAllocated, currentVersion, resolvedVersion) =
    leaderEvaluated
      and not(followerReevaluated)
      and not(metadataAllocated)
      and currentVersion == resolvedVersion

  pure def successfulOutcome(outcome) =
    outcome == "success"

  pure def replayableNoopOutcome(outcome) =
    outcome == "conditional_failure" or outcome == "transaction_cancelled"

  def followersApplyResolvedDecision =
    not(appliedResolvedMutation) or canApplyResolvedMutation(
      leaderEvaluatedCondition,
      followerReevaluatedCondition,
      localMetadataAllocated,
      currentBaseVersion,
      resolvedBaseVersion,
    )

  def transactionAppliesAtomically =
    not(successfulOutcome(resolvedOutcome))
      or appliedOperationCount == 0
      or appliedOperationCount == operationCount

  def proposalBatchPreservesOrder =
    not(appliedResolvedMutation) or batchOrderPreserved

  def noopOutcomesDoNotApplyStorageEffects =
    not(replayableNoopOutcome(resolvedOutcome)) or appliedOperationCount == 0

  def baseMismatchFailsClosed =
    currentBaseVersion == resolvedBaseVersion
      or (not(appliedResolvedMutation) and lastOutcome == "rejected_base_version_mismatch")

  action init = all {
    leaderEvaluatedCondition' = false,
    followerReevaluatedCondition' = false,
    localMetadataAllocated' = false,
    appliedResolvedMutation' = false,
    currentBaseVersion' = 0,
    resolvedBaseVersion' = 0,
    operationCount' = 1,
    appliedOperationCount' = 0,
    batchOrderPreserved' = true,
    resolvedOutcome' = "pending",
    lastOutcome' = "pending",
  }

  action LeaderResolvesSuccess = all {
    leaderEvaluatedCondition' = true,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = appliedResolvedMutation,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = currentBaseVersion,
    operationCount' = 1,
    appliedOperationCount' = appliedOperationCount,
    batchOrderPreserved' = true,
    resolvedOutcome' = "success",
    lastOutcome' = "leader_resolved_success",
  }

  action LeaderResolvesConditionalFailure = all {
    leaderEvaluatedCondition' = true,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = appliedResolvedMutation,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = currentBaseVersion,
    operationCount' = 1,
    appliedOperationCount' = 0,
    batchOrderPreserved' = true,
    resolvedOutcome' = "conditional_failure",
    lastOutcome' = "resolved_conditional_failure",
  }

  action LeaderResolvesTransactionCancellation = all {
    leaderEvaluatedCondition' = true,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = appliedResolvedMutation,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = currentBaseVersion,
    operationCount' = 2,
    appliedOperationCount' = 0,
    batchOrderPreserved' = true,
    resolvedOutcome' = "transaction_cancelled",
    lastOutcome' = "resolved_transaction_cancelled",
  }

  action LeaderResolvesProposalBatch = all {
    leaderEvaluatedCondition' = true,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = appliedResolvedMutation,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = currentBaseVersion,
    operationCount' = 2,
    appliedOperationCount' = appliedOperationCount,
    batchOrderPreserved' = true,
    resolvedOutcome' = "success",
    lastOutcome' = "leader_resolved_batch",
  }

  action AdvanceCurrentVersion = all {
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = false,
    currentBaseVersion' = currentBaseVersion + 1,
    resolvedBaseVersion' = resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = 0,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = "current_version_advanced",
  }

  action FollowerAppliesResolvedMutation = all {
    leaderEvaluatedCondition,
    currentBaseVersion == resolvedBaseVersion,
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = false,
    localMetadataAllocated' = false,
    appliedResolvedMutation' = true,
    currentBaseVersion' = if (successfulOutcome(resolvedOutcome)) currentBaseVersion + operationCount else currentBaseVersion,
    resolvedBaseVersion' = if (successfulOutcome(resolvedOutcome)) currentBaseVersion + operationCount else resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = if (successfulOutcome(resolvedOutcome)) operationCount else 0,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = if (successfulOutcome(resolvedOutcome)) "applied_resolved_mutation" else "applied_resolved_noop",
  }

  action RejectBaseVersionMismatch = all {
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = false,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = 0,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = if (currentBaseVersion == resolvedBaseVersion) lastOutcome else "rejected_base_version_mismatch",
  }

  action RejectFollowerReevaluation = all {
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = false,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = appliedResolvedMutation,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = appliedOperationCount,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = "rejected_follower_reevaluation",
  }

  action RejectLocalMetadataAllocation = all {
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = false,
    appliedResolvedMutation' = appliedResolvedMutation,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = appliedOperationCount,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = "rejected_local_metadata",
  }

  action step = any {
    LeaderResolvesSuccess,
    LeaderResolvesConditionalFailure,
    LeaderResolvesTransactionCancellation,
    LeaderResolvesProposalBatch,
    AdvanceCurrentVersion,
    FollowerAppliesResolvedMutation,
    RejectBaseVersionMismatch,
    RejectFollowerReevaluation,
    RejectLocalMetadataAllocation,
  }

  val inv =
    followersApplyResolvedDecision
      and transactionAppliesAtomically
      and proposalBatchPreservesOrder
      and noopOutcomesDoNotApplyStorageEffects
      and baseMismatchFailsClosed

  val applied_resolved_mutation_witness = appliedResolvedMutation
  val reevaluation_rejected_witness = lastOutcome == "rejected_follower_reevaluation"
  val conditional_failure_witness = lastOutcome == "applied_resolved_noop" and resolvedOutcome == "conditional_failure"
  val transaction_cancelled_witness = lastOutcome == "applied_resolved_noop" and resolvedOutcome == "transaction_cancelled"
  val base_mismatch_rejected_witness = lastOutcome == "rejected_base_version_mismatch"
  val proposal_batch_witness = lastOutcome == "applied_resolved_mutation" and operationCount == 2

  run followerApplyDoesNotReevaluate =
    true.expect(canApplyResolvedMutation(true, false, false, 1, 1))

  run resolvedMutationRequiresLeaderDecision =
    false.expect(canApplyResolvedMutation(false, false, false, 1, 1))

  run resolvedMutationRejectsLocalMetadataAllocation =
    false.expect(canApplyResolvedMutation(true, false, true, 1, 1))

  run resolvedMutationRejectsBaseMismatch =
    false.expect(canApplyResolvedMutation(true, false, false, 2, 1))

  run resolvedNoopOutcomesAreReplayable =
    true.expect(replayableNoopOutcome("conditional_failure") and replayableNoopOutcome("transaction_cancelled"))
}
