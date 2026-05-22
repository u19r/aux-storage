#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTransportFaultMode {
    Delivered,
    Lost,
    Delayed,
    Duplicated,
    ReplayedAfterLeaderChange,
    StaleLeader,
    OneWayPartition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncTransportFaultGate {
    pub source_node: u64,
    pub leader_node: u64,
    pub current_term: u64,
    pub message_term: u64,
    pub delivered_to_voters: usize,
    pub voter_count: usize,
    pub fault_mode: SyncTransportFaultMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTransportFaultDecision {
    Acknowledge,
    BlockQuorum,
    DeferDelivery,
    IgnoreDuplicate,
    IgnoreReplay,
    RejectStaleLeader,
    UnreachableAsymmetric,
}

#[must_use]
pub fn plan_transport_fault_delivery(gate: SyncTransportFaultGate) -> SyncTransportFaultDecision {
    if gate.source_node != gate.leader_node
        || gate.message_term < gate.current_term
        || matches!(gate.fault_mode, SyncTransportFaultMode::StaleLeader)
    {
        return SyncTransportFaultDecision::RejectStaleLeader;
    }
    match gate.fault_mode {
        SyncTransportFaultMode::Delivered => {
            if has_quorum(gate.delivered_to_voters, gate.voter_count) {
                SyncTransportFaultDecision::Acknowledge
            } else {
                SyncTransportFaultDecision::BlockQuorum
            }
        }
        SyncTransportFaultMode::Lost => SyncTransportFaultDecision::BlockQuorum,
        SyncTransportFaultMode::Delayed => SyncTransportFaultDecision::DeferDelivery,
        SyncTransportFaultMode::Duplicated => SyncTransportFaultDecision::IgnoreDuplicate,
        SyncTransportFaultMode::ReplayedAfterLeaderChange => {
            SyncTransportFaultDecision::IgnoreReplay
        }
        SyncTransportFaultMode::StaleLeader => SyncTransportFaultDecision::RejectStaleLeader,
        SyncTransportFaultMode::OneWayPartition => {
            SyncTransportFaultDecision::UnreachableAsymmetric
        }
    }
}

fn has_quorum(delivered_to_voters: usize, voter_count: usize) -> bool {
    delivered_to_voters.saturating_mul(2) > voter_count
}
