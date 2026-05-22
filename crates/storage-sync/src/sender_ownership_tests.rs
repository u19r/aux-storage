use crate::{
    SyncMultiRegionSenderOwnershipDecision, SyncRaftRole, plan_multi_region_sender_ownership,
};

#[test]
fn multi_region_sender_ownership_is_limited_to_sync_leader() {
    assert_eq!(
        plan_multi_region_sender_ownership(&SyncRaftRole::Leader),
        SyncMultiRegionSenderOwnershipDecision::OwnsSender
    );
    for role in [
        SyncRaftRole::Disabled,
        SyncRaftRole::Learner,
        SyncRaftRole::Follower,
        SyncRaftRole::Candidate,
    ] {
        assert_eq!(
            plan_multi_region_sender_ownership(&role),
            SyncMultiRegionSenderOwnershipDecision::Standby
        );
    }
}
