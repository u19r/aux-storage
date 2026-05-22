#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncMultiRegionSenderOwnershipDecision, SyncRaftRole, plan_multi_region_sender_ownership,
};

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncMultiRegionSenderOwnershipState {
    #[serde(rename = "lastRole")]
    last_role: String,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncMultiRegionSenderOwnershipDriver> for SyncMultiRegionSenderOwnershipState {
    fn from_driver(driver: &SyncMultiRegionSenderOwnershipDriver) -> Result<Self> {
        Ok(Self {
            last_role: driver.last_role.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncMultiRegionSenderOwnershipDriver {
    last_role: String,
    last_decision: String,
}

impl Default for SyncMultiRegionSenderOwnershipDriver {
    fn default() -> Self {
        Self {
            last_role: "disabled".to_string(),
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncMultiRegionSenderOwnershipDriver {
    type State = SyncMultiRegionSenderOwnershipState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(role: String) => {
                self.check(role);
            },
            step(role: String?) => {
                if let Some(role) = role {
                    self.check(role);
                }
            },
        })
    }
}

impl SyncMultiRegionSenderOwnershipDriver {
    fn check(&mut self, role: String) {
        self.last_decision =
            decision_name(plan_multi_region_sender_ownership(&role_from_name(&role))).to_string();
        self.last_role = role;
    }
}

fn role_from_name(role: &str) -> SyncRaftRole {
    match role {
        "learner" => SyncRaftRole::Learner,
        "follower" => SyncRaftRole::Follower,
        "candidate" => SyncRaftRole::Candidate,
        "leader" => SyncRaftRole::Leader,
        _ => SyncRaftRole::Disabled,
    }
}

fn decision_name(decision: SyncMultiRegionSenderOwnershipDecision) -> &'static str {
    match decision {
        SyncMultiRegionSenderOwnershipDecision::OwnsSender => "owns_sender",
        SyncMultiRegionSenderOwnershipDecision::Standby => "standby",
    }
}

#[quint_run(
    spec = "../../quint/sync_multi_region_sender_ownership_mbt.qnt",
    max_samples = 64,
    max_steps = 8,
    seed = "0xadd509"
)]
fn sync_multi_region_sender_ownership_mbt_matches_rust_boundary() -> impl Driver {
    SyncMultiRegionSenderOwnershipDriver::default()
}
