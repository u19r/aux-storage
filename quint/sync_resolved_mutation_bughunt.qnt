/// Bug-hunting wrapper for sync resolved mutation application.
///
/// Suggested invariant run:
/// quint run quint/sync_resolved_mutation_bughunt.qnt --invariant=inv --max-samples=5000 --max-steps=12
module sync_resolved_mutation_bughunt {
  import sync_resolved_mutation.* from "./sync_resolved_mutation"

  action UnsafeFollowerReevaluates = all {
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = true,
    localMetadataAllocated' = localMetadataAllocated,
    appliedResolvedMutation' = true,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = appliedOperationCount,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = "unsafe_follower_reevaluated",
  }

  action UnsafeAllocatesLocalMetadata = all {
    leaderEvaluatedCondition' = leaderEvaluatedCondition,
    followerReevaluatedCondition' = followerReevaluatedCondition,
    localMetadataAllocated' = true,
    appliedResolvedMutation' = true,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = resolvedBaseVersion,
    operationCount' = operationCount,
    appliedOperationCount' = appliedOperationCount,
    batchOrderPreserved' = batchOrderPreserved,
    resolvedOutcome' = resolvedOutcome,
    lastOutcome' = "unsafe_local_metadata",
  }

  action UnsafePartialTransactionApply = all {
    leaderEvaluatedCondition' = true,
    followerReevaluatedCondition' = false,
    localMetadataAllocated' = false,
    appliedResolvedMutation' = true,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = currentBaseVersion,
    operationCount' = 2,
    appliedOperationCount' = 1,
    batchOrderPreserved' = true,
    resolvedOutcome' = "success",
    lastOutcome' = "unsafe_partial_transaction",
  }

  action UnsafeBatchReordered = all {
    leaderEvaluatedCondition' = true,
    followerReevaluatedCondition' = false,
    localMetadataAllocated' = false,
    appliedResolvedMutation' = true,
    currentBaseVersion' = currentBaseVersion,
    resolvedBaseVersion' = currentBaseVersion,
    operationCount' = 2,
    appliedOperationCount' = 2,
    batchOrderPreserved' = false,
    resolvedOutcome' = "success",
    lastOutcome' = "unsafe_batch_reordered",
  }

  action bughuntStep = any {
    step,
    UnsafeFollowerReevaluates,
    UnsafeAllocatesLocalMetadata,
    UnsafePartialTransactionApply,
    UnsafeBatchReordered,
  }

  val unsafe_reevaluation_witness = lastOutcome == "unsafe_follower_reevaluated"
}
