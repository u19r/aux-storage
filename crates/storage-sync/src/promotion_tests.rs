use std::sync::Mutex;

use storage_types::StorageResult;

use crate::{
    SyncAutoPromotionController, SyncAutoPromotionOutcome, SyncLearnerPromoter,
    SyncLearnerPromotionReport, SyncNodeId, SyncPromotionBlockReason, SyncPromotionSafetyDecision,
    SyncPromotionSafetyGate, plan_sync_promotion_safety,
};

#[test]
fn sync_promotion_safety_requires_all_operator_free_gates() {
    assert_eq!(
        plan_sync_promotion_safety(SyncPromotionSafetyGate {
            import_complete: false,
            ..allow_gate()
        }),
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::ImportIncomplete)
    );
    assert_eq!(
        plan_sync_promotion_safety(SyncPromotionSafetyGate {
            applied_index: 10,
            promotion_decision_index: 11,
            ..allow_gate()
        }),
        SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::AppliedIndexBehindPromotionDecision
        )
    );
    assert_eq!(
        plan_sync_promotion_safety(SyncPromotionSafetyGate {
            membership_contains_learner: false,
            ..allow_gate()
        }),
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::LearnerMissingFromMembership)
    );
    assert_eq!(
        plan_sync_promotion_safety(allow_gate()),
        SyncPromotionSafetyDecision::Allow
    );
    assert_eq!(
        plan_sync_promotion_safety(SyncPromotionSafetyGate {
            learner_already_voter: true,
            ..allow_gate()
        }),
        SyncPromotionSafetyDecision::AlreadyPromoted
    );
}

fn allow_gate() -> SyncPromotionSafetyGate {
    SyncPromotionSafetyGate {
        learner_already_voter: false,
        import_complete: true,
        checksum_validated: true,
        pending_required_domains: 0,
        protected_stream_drained: true,
        tombstone_cleanup_ready: true,
        applied_index: 12,
        promotion_decision_index: 11,
        peer_connectivity_healthy: true,
        membership_contains_learner: true,
        conflicting_cluster_identity: false,
    }
}

#[tokio::test]
async fn auto_promotion_controller_promotes_only_when_safety_allows() {
    let promoter = RecordingPromoter::default();
    let controller = SyncAutoPromotionController;

    let blocked = controller
        .promote_if_safe(
            &promoter,
            7,
            SyncPromotionSafetyGate {
                checksum_validated: false,
                ..allow_gate()
            },
        )
        .await
        .expect("blocked decision");

    assert_eq!(
        blocked,
        SyncAutoPromotionOutcome::Blocked(SyncPromotionBlockReason::ChecksumNotValidated)
    );
    assert_eq!(promoter.promoted_nodes(), Vec::<SyncNodeId>::new());

    let promoted = controller
        .promote_if_safe(&promoter, 7, allow_gate())
        .await
        .expect("promoted decision");

    assert_eq!(
        promoted,
        SyncAutoPromotionOutcome::Promoted(SyncLearnerPromotionReport {
            node_id: 7,
            log_index: 1007,
        })
    );
    assert_eq!(promoter.promoted_nodes(), vec![7]);
}

#[tokio::test]
async fn auto_promotion_controller_is_idempotent_after_leader_failover_sees_existing_voter() {
    let promoter = RecordingPromoter::default();
    let controller = SyncAutoPromotionController;

    let promoted = controller
        .promote_if_safe(&promoter, 7, allow_gate())
        .await
        .expect("first leader promotes");
    let after_failover = controller
        .promote_if_safe(
            &promoter,
            7,
            SyncPromotionSafetyGate {
                learner_already_voter: true,
                ..allow_gate()
            },
        )
        .await
        .expect("new leader observes committed voter");

    assert!(matches!(promoted, SyncAutoPromotionOutcome::Promoted(_)));
    assert_eq!(
        after_failover,
        SyncAutoPromotionOutcome::AlreadyPromoted { node_id: 7 }
    );
    assert_eq!(promoter.promoted_nodes(), vec![7]);
}

#[tokio::test]
async fn auto_promotion_controller_promotes_after_failover_when_membership_not_committed() {
    let promoter = RecordingPromoter::default();
    let controller = SyncAutoPromotionController;

    let after_failover = controller
        .promote_if_safe(&promoter, 9, allow_gate())
        .await
        .expect("new leader promotes");

    assert!(matches!(
        after_failover,
        SyncAutoPromotionOutcome::Promoted(SyncLearnerPromotionReport {
            node_id: 9,
            log_index: 1009,
        })
    ));
    assert_eq!(promoter.promoted_nodes(), vec![9]);
}

#[derive(Default)]
struct RecordingPromoter {
    promoted: Mutex<Vec<SyncNodeId>>,
}

impl RecordingPromoter {
    fn promoted_nodes(&self) -> Vec<SyncNodeId> {
        self.promoted.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl SyncLearnerPromoter for RecordingPromoter {
    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> StorageResult<SyncLearnerPromotionReport> {
        self.promoted.lock().unwrap().push(node_id);
        Ok(SyncLearnerPromotionReport {
            node_id,
            log_index: 1000 + node_id,
        })
    }
}
