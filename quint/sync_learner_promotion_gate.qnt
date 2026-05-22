/// Sync learner promotion gate model.
///
/// Protects Invariant Inventory ID SYNC-1 from docs/quint-backfill.md.
/// Logical backfill completion is intentionally abstract in this model.
module sync_learner_promotion_gate {
  var backfillComplete: bool
  var appliedPromotionIndex: bool
  var membershipJointConsensus: bool
  var promoted: bool
  var lastOutcome: str

  pure def canPromote(backfill, indexApplied, membershipReady) =
    backfill and indexApplied and membershipReady

  def promotionRequiresAllGates =
    not(promoted) or canPromote(backfillComplete, appliedPromotionIndex, membershipJointConsensus)

  action init = all {
    backfillComplete' = false,
    appliedPromotionIndex' = false,
    membershipJointConsensus' = false,
    promoted' = false,
    lastOutcome' = "pending",
  }

  action CompleteBackfill = all {
    backfillComplete' = true,
    appliedPromotionIndex' = appliedPromotionIndex,
    membershipJointConsensus' = membershipJointConsensus,
    promoted' = promoted,
    lastOutcome' = "backfill_complete",
  }

  action ApplyPromotionIndex = all {
    backfillComplete' = backfillComplete,
    appliedPromotionIndex' = true,
    membershipJointConsensus' = membershipJointConsensus,
    promoted' = promoted,
    lastOutcome' = "promotion_index_applied",
  }

  action CompleteMembershipGate = all {
    backfillComplete' = backfillComplete,
    appliedPromotionIndex' = appliedPromotionIndex,
    membershipJointConsensus' = true,
    promoted' = promoted,
    lastOutcome' = "membership_ready",
  }

  action TryPromote = all {
    backfillComplete' = backfillComplete,
    appliedPromotionIndex' = appliedPromotionIndex,
    membershipJointConsensus' = membershipJointConsensus,
    promoted' = canPromote(backfillComplete, appliedPromotionIndex, membershipJointConsensus),
    lastOutcome' = if (canPromote(backfillComplete, appliedPromotionIndex, membershipJointConsensus)) "promoted" else "promotion_blocked",
  }

  action step = any {
    CompleteBackfill,
    ApplyPromotionIndex,
    CompleteMembershipGate,
    TryPromote,
  }

  val inv = promotionRequiresAllGates

  val promotion_success_witness = promoted
  val promotion_rejected_witness = lastOutcome == "promotion_blocked"

  run promotionRequiresBackfill =
    false.expect(canPromote(false, true, true))

  run promotionSucceedsAfterAllGates =
    true.expect(canPromote(true, true, true))
}
