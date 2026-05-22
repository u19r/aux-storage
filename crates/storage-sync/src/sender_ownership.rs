use crate::SyncRaftRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMultiRegionSenderOwnershipDecision {
    OwnsSender,
    Standby,
}

#[must_use]
pub fn plan_multi_region_sender_ownership(
    role: &SyncRaftRole,
) -> SyncMultiRegionSenderOwnershipDecision {
    if matches!(role, SyncRaftRole::Leader) {
        SyncMultiRegionSenderOwnershipDecision::OwnsSender
    } else {
        SyncMultiRegionSenderOwnershipDecision::Standby
    }
}
