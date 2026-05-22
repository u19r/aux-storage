#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::sync_response_correlation::{
    SyncResponseCorrelationDecision, SyncResponseCorrelationGate, plan_sync_response_correlation,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtGate {
    #[serde(rename = "responseCount")]
    response_count: usize,
    index: usize,
    #[serde(rename = "payloadPresent")]
    payload_present: bool,
    required: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncResponseCorrelationState {
    #[serde(rename = "lastGate")]
    last_gate: MbtGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncResponseCorrelationDriver> for SyncResponseCorrelationState {
    fn from_driver(driver: &SyncResponseCorrelationDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate,
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncResponseCorrelationDriver {
    last_gate: MbtGate,
    last_decision: String,
}

impl Default for SyncResponseCorrelationDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtGate {
                response_count: 0,
                index: 0,
                payload_present: false,
                required: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncResponseCorrelationDriver {
    type State = SyncResponseCorrelationState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(responseCount: usize, index: usize, payloadPresent: bool, required: bool) => {
                self.check(MbtGate {
                    response_count: responseCount,
                    index,
                    payload_present: payloadPresent,
                    required,
                });
            },
            step(responseCount: usize?, index: usize?, payloadPresent: bool?, required: bool?) => {
                if let (
                    Some(response_count),
                    Some(index),
                    Some(payload_present),
                    Some(required),
                ) = (responseCount, index, payloadPresent, required)
                {
                    self.check(MbtGate {
                        response_count,
                        index,
                        payload_present,
                        required,
                    });
                }
            },
        })
    }
}

impl SyncResponseCorrelationDriver {
    fn check(&mut self, gate: MbtGate) {
        let decision = plan_sync_response_correlation(SyncResponseCorrelationGate {
            response_count: gate.response_count,
            index: gate.index,
            payload_present: gate.payload_present,
            required: gate.required,
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn decision_name(decision: SyncResponseCorrelationDecision) -> &'static str {
    match decision {
        SyncResponseCorrelationDecision::UseDefault => "use_default",
        SyncResponseCorrelationDecision::DecodePayload => "decode_payload",
        SyncResponseCorrelationDecision::MissingEntry => "missing_entry",
        SyncResponseCorrelationDecision::MissingPayload => "missing_payload",
    }
}

#[quint_run(
    spec = "../../quint/sync_response_correlation_mbt.qnt",
    max_samples = 64,
    max_steps = 8,
    seed = "0xc011e1a7e"
)]
fn sync_response_correlation_mbt_matches_rust_boundary() -> impl Driver {
    SyncResponseCorrelationDriver::default()
}
