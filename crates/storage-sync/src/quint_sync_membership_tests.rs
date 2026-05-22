#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{SyncMembershipDecision, SyncMembershipGate, plan_membership_activation};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtMembershipGate {
    #[serde(rename = "oldConfigCommitted")]
    old_config_committed: bool,
    #[serde(rename = "jointConfigCommitted")]
    joint_config_committed: bool,
    #[serde(rename = "newConfigCommitted")]
    new_config_committed: bool,
    #[serde(rename = "leaderHasQuorum")]
    leader_has_quorum: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncMembershipState {
    #[serde(rename = "lastGate")]
    last_gate: MbtMembershipGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncMembershipDriver> for SyncMembershipState {
    fn from_driver(driver: &SyncMembershipDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncMembershipDriver {
    last_gate: MbtMembershipGate,
    last_decision: String,
}

impl Default for SyncMembershipDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtMembershipGate {
                old_config_committed: true,
                joint_config_committed: false,
                new_config_committed: false,
                leader_has_quorum: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncMembershipDriver {
    type State = SyncMembershipState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                oldConfigCommitted: bool,
                jointConfigCommitted: bool,
                newConfigCommitted: bool,
                leaderHasQuorum: bool,
            ) => {
                self.check(MbtMembershipGate {
                    old_config_committed: oldConfigCommitted,
                    joint_config_committed: jointConfigCommitted,
                    new_config_committed: newConfigCommitted,
                    leader_has_quorum: leaderHasQuorum,
                });
            },
            step(
                oldConfigCommitted: bool?,
                jointConfigCommitted: bool?,
                newConfigCommitted: bool?,
                leaderHasQuorum: bool?,
            ) => {
                if let (
                    Some(old_config_committed),
                    Some(joint_config_committed),
                    Some(new_config_committed),
                    Some(leader_has_quorum),
                ) = (
                    oldConfigCommitted,
                    jointConfigCommitted,
                    newConfigCommitted,
                    leaderHasQuorum,
                )
                {
                    self.check(MbtMembershipGate {
                        old_config_committed,
                        joint_config_committed,
                        new_config_committed,
                        leader_has_quorum,
                    });
                }
            },
        })
    }
}

impl SyncMembershipDriver {
    fn check(&mut self, gate: MbtMembershipGate) {
        let decision = plan_membership_activation(SyncMembershipGate {
            old_config_committed: gate.old_config_committed,
            joint_config_committed: gate.joint_config_committed,
            new_config_committed: gate.new_config_committed,
            leader_has_quorum: gate.leader_has_quorum,
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn decision_name(decision: SyncMembershipDecision) -> &'static str {
    match decision {
        SyncMembershipDecision::Activate => "activate",
        SyncMembershipDecision::Block => "block",
    }
}

#[quint_run(
    spec = "../../quint/sync_membership_mbt.qnt",
    max_samples = 64,
    max_steps = 8,
    seed = "0x5e7be2"
)]
fn sync_membership_mbt_matches_rust_boundary() -> impl Driver {
    SyncMembershipDriver::default()
}
