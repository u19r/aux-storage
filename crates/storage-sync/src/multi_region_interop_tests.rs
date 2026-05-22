use crate::{
    SyncMultiRegionInboundApplyDecision, SyncMultiRegionSenderOwnershipDecision, SyncRaftRole,
    plan_sync_multi_region_interop,
};

#[test]
fn sync_follower_standby_still_accepts_fresh_inbound_multi_region_apply() {
    let decision = plan_sync_multi_region_interop(&SyncRaftRole::Follower, 1, 2);

    assert_eq!(
        decision.outbound,
        SyncMultiRegionSenderOwnershipDecision::Standby
    );
    assert_eq!(decision.inbound, SyncMultiRegionInboundApplyDecision::Apply);
    assert_eq!(decision.stored_version, 2);
}

#[test]
fn stale_inbound_multi_region_apply_is_skipped_without_regressing_state() {
    let decision = plan_sync_multi_region_interop(&SyncRaftRole::Leader, 2, 1);

    assert_eq!(
        decision.outbound,
        SyncMultiRegionSenderOwnershipDecision::OwnsSender
    );
    assert_eq!(
        decision.inbound,
        SyncMultiRegionInboundApplyDecision::SkipStale
    );
    assert_eq!(decision.stored_version, 2);
}
