#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncBackendPairDecision, SyncBackendPairReason, plan_sync_backend_pair,
    plan_sync_backend_pair_detailed,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct BackendPair {
    source: String,
    destination: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncBackendPairPolicyState {
    #[serde(rename = "lastPair")]
    last_pair: BackendPair,
    #[serde(rename = "lastDecision")]
    last_decision: String,
    #[serde(rename = "lastReason")]
    last_reason: String,
}

impl State<SyncBackendPairPolicyDriver> for SyncBackendPairPolicyState {
    fn from_driver(driver: &SyncBackendPairPolicyDriver) -> Result<Self> {
        Ok(Self {
            last_pair: driver.last_pair.clone(),
            last_decision: driver.last_decision.clone(),
            last_reason: driver.last_reason.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncBackendPairPolicyDriver {
    last_pair: BackendPair,
    last_decision: String,
    last_reason: String,
}

impl Default for SyncBackendPairPolicyDriver {
    fn default() -> Self {
        Self {
            last_pair: BackendPair {
                source: String::new(),
                destination: String::new(),
            },
            last_decision: "not_checked".to_string(),
            last_reason: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncBackendPairPolicyDriver {
    type State = SyncBackendPairPolicyState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(source: String, destination: String) => {
                self.check(source, destination);
            },
            step(source: String?, destination: String?) => {
                if let (Some(source), Some(destination)) = (source, destination) {
                    self.check(source, destination);
                }
            },
        })
    }
}

impl SyncBackendPairPolicyDriver {
    fn check(&mut self, source: String, destination: String) {
        let plan = plan_sync_backend_pair_detailed(&source, &destination);
        self.last_decision = decision_name(plan.decision).to_string();
        self.last_reason = reason_name(plan.reason).to_string();
        self.last_pair = BackendPair {
            source,
            destination,
        };
    }
}

fn reason_name(reason: SyncBackendPairReason) -> &'static str {
    reason.as_str()
}

fn decision_name(decision: SyncBackendPairDecision) -> &'static str {
    match decision {
        SyncBackendPairDecision::ProductionSupported => "production_supported",
        SyncBackendPairDecision::ValidationOnly => "validation_only",
        SyncBackendPairDecision::Rejected => "rejected",
    }
}

#[quint_run(
    spec = "../../quint/sync_backend_pair_policy_mbt.qnt",
    max_samples = 96,
    max_steps = 8,
    seed = "0xadd508"
)]
fn sync_backend_pair_policy_mbt_matches_rust_boundary() -> impl Driver {
    SyncBackendPairPolicyDriver::default()
}

#[test]
fn sync_backend_pair_compatibility_wrapper_returns_decision_only() {
    assert_eq!(
        plan_sync_backend_pair("sqlite", "sqlite"),
        SyncBackendPairDecision::ProductionSupported
    );
    assert_eq!(
        plan_sync_backend_pair("sqlite", "rocksdb"),
        SyncBackendPairDecision::ValidationOnly
    );
    assert_eq!(
        plan_sync_backend_pair("remote", "sqlite"),
        SyncBackendPairDecision::Rejected
    );
}
