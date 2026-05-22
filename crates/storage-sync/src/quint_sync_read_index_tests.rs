#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{SyncStrongReadDecision, SyncStrongReadGate, plan_strong_read_gate};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtReadGate {
    #[serde(rename = "leaderHasQuorum")]
    leader_has_quorum: bool,
    #[serde(rename = "readIndexApplied")]
    read_index_applied: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncReadIndexState {
    #[serde(rename = "lastGate")]
    last_gate: MbtReadGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncReadIndexDriver> for SyncReadIndexState {
    fn from_driver(driver: &SyncReadIndexDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncReadIndexDriver {
    last_gate: MbtReadGate,
    last_decision: String,
}

impl Default for SyncReadIndexDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtReadGate {
                leader_has_quorum: false,
                read_index_applied: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncReadIndexDriver {
    type State = SyncReadIndexState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(leaderHasQuorum: bool, readIndexApplied: bool) => {
                self.check(MbtReadGate {
                    leader_has_quorum: leaderHasQuorum,
                    read_index_applied: readIndexApplied,
                });
            },
            step(leaderHasQuorum: bool?, readIndexApplied: bool?) => {
                if let (Some(leader_has_quorum), Some(read_index_applied)) =
                    (leaderHasQuorum, readIndexApplied)
                {
                    self.check(MbtReadGate {
                        leader_has_quorum,
                        read_index_applied,
                    });
                }
            },
        })
    }
}

impl SyncReadIndexDriver {
    fn check(&mut self, gate: MbtReadGate) {
        let decision = plan_strong_read_gate(SyncStrongReadGate {
            leader_has_quorum: gate.leader_has_quorum,
            read_index_applied: gate.read_index_applied,
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn decision_name(decision: SyncStrongReadDecision) -> &'static str {
    match decision {
        SyncStrongReadDecision::Serve => "serve",
        SyncStrongReadDecision::Block => "block",
    }
}

#[quint_run(
    spec = "../../quint/sync_read_index_mbt.qnt",
    max_samples = 32,
    max_steps = 8,
    seed = "0x51ead"
)]
fn sync_read_index_mbt_matches_rust_boundary() -> impl Driver {
    SyncReadIndexDriver::default()
}
