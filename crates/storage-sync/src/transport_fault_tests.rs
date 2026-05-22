use crate::{
    SyncTransportFaultDecision, SyncTransportFaultGate, SyncTransportFaultMode,
    plan_transport_fault_delivery,
};

#[test]
fn delivered_current_leader_requires_quorum_before_acknowledgement() {
    assert_eq!(
        plan_transport_fault_delivery(gate(SyncTransportFaultMode::Delivered, 2)),
        SyncTransportFaultDecision::Acknowledge
    );
    assert_eq!(
        plan_transport_fault_delivery(gate(SyncTransportFaultMode::Delivered, 1)),
        SyncTransportFaultDecision::BlockQuorum
    );
}

#[test]
fn non_delivered_faults_do_not_acknowledge_writes() {
    for (mode, decision) in [
        (
            SyncTransportFaultMode::Lost,
            SyncTransportFaultDecision::BlockQuorum,
        ),
        (
            SyncTransportFaultMode::Delayed,
            SyncTransportFaultDecision::DeferDelivery,
        ),
        (
            SyncTransportFaultMode::Duplicated,
            SyncTransportFaultDecision::IgnoreDuplicate,
        ),
        (
            SyncTransportFaultMode::ReplayedAfterLeaderChange,
            SyncTransportFaultDecision::IgnoreReplay,
        ),
        (
            SyncTransportFaultMode::OneWayPartition,
            SyncTransportFaultDecision::UnreachableAsymmetric,
        ),
    ] {
        assert_eq!(plan_transport_fault_delivery(gate(mode, 3)), decision);
    }
}

#[test]
fn stale_leader_and_stale_term_are_rejected_before_fault_handling() {
    let mut stale_source = gate(SyncTransportFaultMode::Delivered, 3);
    stale_source.source_node = 2;
    assert_eq!(
        plan_transport_fault_delivery(stale_source),
        SyncTransportFaultDecision::RejectStaleLeader
    );

    let mut stale_term = gate(SyncTransportFaultMode::Delivered, 3);
    stale_term.message_term = 3;
    stale_term.current_term = 4;
    assert_eq!(
        plan_transport_fault_delivery(stale_term),
        SyncTransportFaultDecision::RejectStaleLeader
    );
}

fn gate(fault_mode: SyncTransportFaultMode, delivered_to_voters: usize) -> SyncTransportFaultGate {
    SyncTransportFaultGate {
        source_node: 1,
        leader_node: 1,
        current_term: 4,
        message_term: 4,
        delivered_to_voters,
        voter_count: 3,
        fault_mode,
    }
}
