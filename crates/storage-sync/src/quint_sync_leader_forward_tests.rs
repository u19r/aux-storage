#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{SyncLeaderForward, SyncLeaderForwardDecision, plan_leader_forward};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct MbtForwardInput {
    #[serde(rename = "localIsLeader")]
    local_is_leader: bool,
    #[serde(rename = "leaderHintKnown")]
    leader_hint_known: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncLeaderForwardState {
    #[serde(rename = "lastInput")]
    last_input: MbtForwardInput,
    #[serde(rename = "lastDecision")]
    last_decision: String,
    #[serde(rename = "lastHintForwarded")]
    last_hint_forwarded: bool,
}

impl State<SyncLeaderForwardDriver> for SyncLeaderForwardState {
    fn from_driver(driver: &SyncLeaderForwardDriver) -> Result<Self> {
        Ok(Self {
            last_input: driver.last_input.clone(),
            last_decision: driver.last_decision.clone(),
            last_hint_forwarded: driver.last_hint_forwarded,
        })
    }
}

#[derive(Debug)]
struct SyncLeaderForwardDriver {
    last_input: MbtForwardInput,
    last_decision: String,
    last_hint_forwarded: bool,
}

impl Default for SyncLeaderForwardDriver {
    fn default() -> Self {
        Self {
            last_input: MbtForwardInput {
                local_is_leader: false,
                leader_hint_known: false,
            },
            last_decision: "not_checked".to_string(),
            last_hint_forwarded: false,
        }
    }
}

impl Driver for SyncLeaderForwardDriver {
    type State = SyncLeaderForwardState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(localIsLeader: bool, leaderHintKnown: bool) => {
                self.check(MbtForwardInput {
                    local_is_leader: localIsLeader,
                    leader_hint_known: leaderHintKnown,
                });
            },
            step(localIsLeader: bool?, leaderHintKnown: bool?) => {
                if let (Some(local_is_leader), Some(leader_hint_known)) =
                    (localIsLeader, leaderHintKnown)
                {
                    self.check(MbtForwardInput {
                        local_is_leader,
                        leader_hint_known,
                    });
                }
            },
        })
    }
}

impl SyncLeaderForwardDriver {
    fn check(&mut self, input: MbtForwardInput) {
        let leader_hint = input
            .leader_hint_known
            .then(|| "http://leader.test/storage".to_string());
        let decision = plan_leader_forward(SyncLeaderForward {
            local_is_leader: input.local_is_leader,
            leader_hint,
        });
        self.last_decision = decision_name(&decision).to_string();
        self.last_hint_forwarded = matches!(
            decision,
            SyncLeaderForwardDecision::NotLeader {
                leader_hint: Some(_),
            }
        );
        self.last_input = input;
    }
}

fn decision_name(decision: &SyncLeaderForwardDecision) -> &'static str {
    match decision {
        SyncLeaderForwardDecision::Serve => "serve",
        SyncLeaderForwardDecision::NotLeader { .. } => "not_leader",
    }
}

#[quint_run(
    spec = "../../quint/sync_leader_forward_mbt.qnt",
    max_samples = 32,
    max_steps = 8,
    seed = "0x1eadf0d"
)]
fn sync_leader_forward_mbt_matches_rust_boundary() -> impl Driver {
    SyncLeaderForwardDriver::default()
}
