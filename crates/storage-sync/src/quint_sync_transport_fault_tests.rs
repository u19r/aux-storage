#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncTransportFaultDecision, SyncTransportFaultGate, SyncTransportFaultMode,
    plan_transport_fault_delivery,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtTransportFaultGate {
    #[serde(rename = "sourceNode")]
    source_node: u64,
    #[serde(rename = "leaderNode")]
    leader_node: u64,
    #[serde(rename = "currentTerm")]
    current_term: u64,
    #[serde(rename = "messageTerm")]
    message_term: u64,
    #[serde(rename = "deliveredToVoters")]
    delivered_to_voters: usize,
    #[serde(rename = "voterCount")]
    voter_count: usize,
    #[serde(rename = "faultMode")]
    fault_mode: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncTransportFaultState {
    #[serde(rename = "lastGate")]
    last_gate: MbtTransportFaultGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncTransportFaultDriver> for SyncTransportFaultState {
    fn from_driver(driver: &SyncTransportFaultDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncTransportFaultDriver {
    last_gate: MbtTransportFaultGate,
    last_decision: String,
}

impl Default for SyncTransportFaultDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtTransportFaultGate {
                source_node: 1,
                leader_node: 1,
                current_term: 1,
                message_term: 1,
                delivered_to_voters: 2,
                voter_count: 3,
                fault_mode: "delivered".to_string(),
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncTransportFaultDriver {
    type State = SyncTransportFaultState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                sourceNode: u64,
                currentTerm: u64,
                messageTerm: u64,
                deliveredToVoters: usize,
                voterCount: usize,
                faultMode: String,
            ) => {
                self.check(MbtTransportFaultGate {
                    source_node: sourceNode,
                    leader_node: 1,
                    current_term: currentTerm,
                    message_term: messageTerm,
                    delivered_to_voters: deliveredToVoters,
                    voter_count: voterCount,
                    fault_mode: faultMode,
                });
            },
            step(
                sourceNode: u64?,
                currentTerm: u64?,
                messageTerm: u64?,
                deliveredToVoters: usize?,
                voterCount: usize?,
                faultMode: String?,
            ) => {
                if let (
                    Some(source_node),
                    Some(current_term),
                    Some(message_term),
                    Some(delivered_to_voters),
                    Some(voter_count),
                    Some(fault_mode),
                ) = (
                    sourceNode,
                    currentTerm,
                    messageTerm,
                    deliveredToVoters,
                    voterCount,
                    faultMode,
                ) {
                    self.check(MbtTransportFaultGate {
                        source_node,
                        leader_node: 1,
                        current_term,
                        message_term,
                        delivered_to_voters,
                        voter_count,
                        fault_mode,
                    });
                }
            },
        })
    }
}

impl SyncTransportFaultDriver {
    fn check(&mut self, gate: MbtTransportFaultGate) {
        let decision = plan_transport_fault_delivery(SyncTransportFaultGate {
            source_node: gate.source_node,
            leader_node: gate.leader_node,
            current_term: gate.current_term,
            message_term: gate.message_term,
            delivered_to_voters: gate.delivered_to_voters,
            voter_count: gate.voter_count,
            fault_mode: fault_mode(&gate.fault_mode),
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn fault_mode(name: &str) -> SyncTransportFaultMode {
    match name {
        "delivered" => SyncTransportFaultMode::Delivered,
        "lost" => SyncTransportFaultMode::Lost,
        "delayed" => SyncTransportFaultMode::Delayed,
        "duplicated" => SyncTransportFaultMode::Duplicated,
        "replayed_after_leader_change" => SyncTransportFaultMode::ReplayedAfterLeaderChange,
        "stale_leader" => SyncTransportFaultMode::StaleLeader,
        "one_way_partition" => SyncTransportFaultMode::OneWayPartition,
        _ => SyncTransportFaultMode::Lost,
    }
}

fn decision_name(decision: SyncTransportFaultDecision) -> &'static str {
    match decision {
        SyncTransportFaultDecision::Acknowledge => "acknowledge",
        SyncTransportFaultDecision::BlockQuorum => "block_quorum",
        SyncTransportFaultDecision::DeferDelivery => "defer_delivery",
        SyncTransportFaultDecision::IgnoreDuplicate => "ignore_duplicate",
        SyncTransportFaultDecision::IgnoreReplay => "ignore_replay",
        SyncTransportFaultDecision::RejectStaleLeader => "reject_stale_leader",
        SyncTransportFaultDecision::UnreachableAsymmetric => "unreachable_asymmetric",
    }
}

#[quint_run(
    spec = "../../quint/sync_transport_fault_mbt.qnt",
    max_samples = 128,
    max_steps = 8,
    seed = "0x7fa417"
)]
fn sync_transport_fault_mbt_matches_rust_boundary() -> impl Driver {
    SyncTransportFaultDriver::default()
}
