#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::sync_raft_peer_status::{SyncRaftPeerStatusDecision, classify_sync_raft_peer_status};

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncPeerAuthTransportState {
    #[serde(rename = "lastStatus")]
    last_status: u16,
    #[serde(rename = "lastDecision")]
    last_decision: String,
    #[serde(rename = "lastMessage")]
    last_message: String,
}

impl State<SyncPeerAuthTransportDriver> for SyncPeerAuthTransportState {
    fn from_driver(driver: &SyncPeerAuthTransportDriver) -> Result<Self> {
        Ok(Self {
            last_status: driver.last_status,
            last_decision: driver.last_decision.clone(),
            last_message: driver.last_message.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncPeerAuthTransportDriver {
    last_status: u16,
    last_decision: String,
    last_message: String,
}

impl Default for SyncPeerAuthTransportDriver {
    fn default() -> Self {
        Self {
            last_status: 500,
            last_decision: "not_checked".to_string(),
            last_message: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncPeerAuthTransportDriver {
    type State = SyncPeerAuthTransportState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(status: u16) => {
                self.check(status);
            },
            step(status: u16?) => {
                if let Some(status) = status {
                    self.check(status);
                }
            },
        })
    }
}

impl SyncPeerAuthTransportDriver {
    fn check(&mut self, status: u16) {
        self.last_status = status;
        let decision = classify_sync_raft_peer_status(status);
        self.last_decision = decision_name(decision).to_string();
        self.last_message = decision.message().to_string();
    }
}

fn decision_name(decision: SyncRaftPeerStatusDecision) -> &'static str {
    match decision {
        SyncRaftPeerStatusDecision::AuthenticationFailed => "authentication_failed",
        SyncRaftPeerStatusDecision::PeerReturnedError => "peer_returned_error",
    }
}

#[quint_run(
    spec = "../../quint/sync_peer_auth_transport_mbt.qnt",
    max_samples = 32,
    max_steps = 8,
    seed = "0xa474"
)]
fn sync_peer_auth_transport_mbt_matches_rust_boundary() -> impl Driver {
    SyncPeerAuthTransportDriver::default()
}
