#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncProposalAdmissionDecision, SyncProposalAdmissionGate, SyncProposalPipelineLimits,
    SyncProposalShape, plan_proposal_admission,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtLimits {
    #[serde(rename = "maxOperations")]
    max_operations: usize,
    #[serde(rename = "maxBytes")]
    max_bytes: usize,
    #[serde(rename = "maxQueueDepth")]
    max_queue_depth: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtGate {
    #[serde(rename = "operationCount")]
    operation_count: usize,
    #[serde(rename = "byteCount")]
    byte_count: usize,
    #[serde(rename = "inFlight")]
    in_flight: usize,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncProposalAdmissionState {
    #[serde(rename = "lastLimits")]
    last_limits: MbtLimits,
    #[serde(rename = "lastGate")]
    last_gate: MbtGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncProposalAdmissionDriver> for SyncProposalAdmissionState {
    fn from_driver(driver: &SyncProposalAdmissionDriver) -> Result<Self> {
        Ok(Self {
            last_limits: driver.last_limits,
            last_gate: driver.last_gate,
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncProposalAdmissionDriver {
    last_limits: MbtLimits,
    last_gate: MbtGate,
    last_decision: String,
}

impl Default for SyncProposalAdmissionDriver {
    fn default() -> Self {
        Self {
            last_limits: MbtLimits {
                max_operations: 1,
                max_bytes: 1,
                max_queue_depth: 1,
            },
            last_gate: MbtGate {
                operation_count: 1,
                byte_count: 1,
                in_flight: 0,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncProposalAdmissionDriver {
    type State = SyncProposalAdmissionState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                maxOperations: usize,
                maxBytes: usize,
                maxQueueDepth: usize,
                operationCount: usize,
                byteCount: usize,
                inFlight: usize,
            ) => {
                self.check(
                    MbtLimits {
                        max_operations: maxOperations,
                        max_bytes: maxBytes,
                        max_queue_depth: maxQueueDepth,
                    },
                    MbtGate {
                        operation_count: operationCount,
                        byte_count: byteCount,
                        in_flight: inFlight,
                    },
                );
            },
            step(
                maxOperations: usize?,
                maxBytes: usize?,
                maxQueueDepth: usize?,
                operationCount: usize?,
                byteCount: usize?,
                inFlight: usize?,
            ) => {
                if let (
                    Some(max_operations),
                    Some(max_bytes),
                    Some(max_queue_depth),
                    Some(operation_count),
                    Some(byte_count),
                    Some(in_flight),
                ) = (
                    maxOperations,
                    maxBytes,
                    maxQueueDepth,
                    operationCount,
                    byteCount,
                    inFlight,
                ) {
                    self.check(
                        MbtLimits {
                            max_operations,
                            max_bytes,
                            max_queue_depth,
                        },
                        MbtGate {
                            operation_count,
                            byte_count,
                            in_flight,
                        },
                    );
                }
            },
        })
    }
}

impl SyncProposalAdmissionDriver {
    fn check(&mut self, limits: MbtLimits, gate: MbtGate) {
        let decision = plan_proposal_admission(
            SyncProposalPipelineLimits {
                max_batch_operations: limits.max_operations,
                max_batch_bytes: limits.max_bytes,
                max_queue_depth: limits.max_queue_depth,
                max_proposal_latency_ms: 250,
            },
            SyncProposalAdmissionGate {
                shape: SyncProposalShape {
                    operation_count: gate.operation_count,
                    byte_count: gate.byte_count,
                },
                in_flight: gate.in_flight,
            },
        );
        self.last_decision = decision_name(decision).to_string();
        self.last_limits = limits;
        self.last_gate = gate;
    }
}

fn decision_name(decision: SyncProposalAdmissionDecision) -> &'static str {
    match decision {
        SyncProposalAdmissionDecision::Admit => "admit",
        SyncProposalAdmissionDecision::RejectOperationCount => "reject_operation_count",
        SyncProposalAdmissionDecision::RejectByteCount => "reject_byte_count",
        SyncProposalAdmissionDecision::RejectQueueFull => "reject_queue_full",
    }
}

#[quint_run(
    spec = "../../quint/sync_proposal_admission_mbt.qnt",
    max_samples = 96,
    max_steps = 8,
    seed = "0xadd501"
)]
fn sync_proposal_admission_mbt_matches_rust_boundary() -> impl Driver {
    SyncProposalAdmissionDriver::default()
}
