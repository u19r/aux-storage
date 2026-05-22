#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncMultiRegionInboundApplyDecision, SyncMultiRegionSenderOwnershipDecision, SyncRaftRole,
    plan_sync_multi_region_interop,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct InteropCase {
    #[serde(rename = "syncRole")]
    sync_role: SyncRoleName,
    #[serde(rename = "currentVersion")]
    current_version: u64,
    #[serde(rename = "incomingVersion")]
    incoming_version: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SyncRoleName {
    Disabled,
    Learner,
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncMultiRegionInteropState {
    #[serde(rename = "lastCase")]
    last_case: InteropCase,
    #[serde(rename = "lastOutboundDecision")]
    last_outbound_decision: String,
    #[serde(rename = "lastInboundDecision")]
    last_inbound_decision: String,
    #[serde(rename = "lastStoredVersion")]
    last_stored_version: u64,
}

impl State<SyncMultiRegionInteropDriver> for SyncMultiRegionInteropState {
    fn from_driver(driver: &SyncMultiRegionInteropDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_outbound_decision: driver.last_outbound_decision.clone(),
            last_inbound_decision: driver.last_inbound_decision.clone(),
            last_stored_version: driver.last_stored_version,
        })
    }
}

#[derive(Debug)]
struct SyncMultiRegionInteropDriver {
    last_case: InteropCase,
    last_outbound_decision: String,
    last_inbound_decision: String,
    last_stored_version: u64,
}

impl Default for SyncMultiRegionInteropDriver {
    fn default() -> Self {
        Self {
            last_case: InteropCase {
                sync_role: SyncRoleName::Disabled,
                current_version: 0,
                incoming_version: 0,
            },
            last_outbound_decision: "not_checked".to_string(),
            last_inbound_decision: "not_checked".to_string(),
            last_stored_version: 0,
        }
    }
}

impl Driver for SyncMultiRegionInteropDriver {
    type State = SyncMultiRegionInteropState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(syncRole: SyncRoleName, currentVersion: u64, incomingVersion: u64) => {
                self.check(InteropCase {
                    sync_role: syncRole,
                    current_version: currentVersion,
                    incoming_version: incomingVersion,
                });
            },
            step(syncRole: SyncRoleName?, currentVersion: u64?, incomingVersion: u64?) => {
                if let (Some(sync_role), Some(current_version), Some(incoming_version)) =
                    (syncRole, currentVersion, incomingVersion)
                {
                    self.check(InteropCase {
                        sync_role,
                        current_version,
                        incoming_version,
                    });
                }
            },
        })
    }
}

impl SyncMultiRegionInteropDriver {
    fn check(&mut self, case: InteropCase) {
        let decision = plan_sync_multi_region_interop(
            &sync_role(case.sync_role),
            case.current_version,
            case.incoming_version,
        );
        self.last_outbound_decision = outbound_decision_name(decision.outbound).to_string();
        self.last_inbound_decision = inbound_decision_name(decision.inbound).to_string();
        self.last_stored_version = decision.stored_version;
        self.last_case = case;
    }
}

fn sync_role(role: SyncRoleName) -> SyncRaftRole {
    match role {
        SyncRoleName::Disabled => SyncRaftRole::Disabled,
        SyncRoleName::Learner => SyncRaftRole::Learner,
        SyncRoleName::Follower => SyncRaftRole::Follower,
        SyncRoleName::Candidate => SyncRaftRole::Candidate,
        SyncRoleName::Leader => SyncRaftRole::Leader,
    }
}

fn outbound_decision_name(decision: SyncMultiRegionSenderOwnershipDecision) -> &'static str {
    match decision {
        SyncMultiRegionSenderOwnershipDecision::OwnsSender => "owns_sender",
        SyncMultiRegionSenderOwnershipDecision::Standby => "standby",
    }
}

fn inbound_decision_name(decision: SyncMultiRegionInboundApplyDecision) -> &'static str {
    match decision {
        SyncMultiRegionInboundApplyDecision::Apply => "apply",
        SyncMultiRegionInboundApplyDecision::SkipStale => "skip_stale",
    }
}

#[quint_run(
    spec = "../../quint/sync_multi_region_interop_mbt.qnt",
    max_samples = 96,
    max_steps = 8,
    seed = "0xadd510"
)]
fn sync_multi_region_interop_mbt_matches_rust_boundary() -> impl Driver {
    SyncMultiRegionInteropDriver::default()
}
