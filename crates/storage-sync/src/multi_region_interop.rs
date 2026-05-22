use crate::{
    SyncMultiRegionSenderOwnershipDecision, SyncRaftRole, plan_multi_region_sender_ownership,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMultiRegionInboundApplyDecision {
    Apply,
    SkipStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncMultiRegionInteropDecision {
    pub outbound: SyncMultiRegionSenderOwnershipDecision,
    pub inbound: SyncMultiRegionInboundApplyDecision,
    pub stored_version: u64,
}

#[must_use]
pub fn plan_sync_multi_region_interop(
    sync_role: &SyncRaftRole,
    current_version: u64,
    incoming_version: u64,
) -> SyncMultiRegionInteropDecision {
    let inbound = if incoming_version > current_version {
        SyncMultiRegionInboundApplyDecision::Apply
    } else {
        SyncMultiRegionInboundApplyDecision::SkipStale
    };
    let stored_version = match inbound {
        SyncMultiRegionInboundApplyDecision::Apply => incoming_version,
        SyncMultiRegionInboundApplyDecision::SkipStale => current_version,
    };
    SyncMultiRegionInteropDecision {
        outbound: plan_multi_region_sender_ownership(sync_role),
        inbound,
        stored_version,
    }
}
