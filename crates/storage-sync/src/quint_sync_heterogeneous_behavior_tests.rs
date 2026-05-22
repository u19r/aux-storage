#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_backfill::LogicalBootstrapPreflightDecision;

use crate::{
    SyncBackendPairDecision, SyncHeterogeneousBehaviorGate,
    SyncHeterogeneousLogicalSnapshotDecision, SyncMultiRegionInboundApplyDecision,
    SyncMultiRegionSenderOwnershipDecision, SyncNonSqlResolvedApplyDecision,
    SyncPromotedLearnerSurfaceDecision, SyncRaftRole, plan_sync_heterogeneous_behavior,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct HeterogeneousCase {
    #[serde(rename = "sourceBackend")]
    source_backend: String,
    #[serde(rename = "destinationBackend")]
    destination_backend: String,
    #[serde(rename = "logicalSnapshotDomainsComplete")]
    logical_snapshot_domains_complete: bool,
    #[serde(rename = "resolvedApplyComplete")]
    resolved_apply_complete: bool,
    #[serde(rename = "learnerPromoted")]
    learner_promoted: bool,
    #[serde(rename = "bootstrapDestinationEmpty")]
    bootstrap_destination_empty: bool,
    #[serde(rename = "bootstrapPreflightMarkerPresent")]
    bootstrap_preflight_marker_present: bool,
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
struct HeterogeneousBehaviorState {
    #[serde(rename = "lastCase")]
    last_case: HeterogeneousCase,
    #[serde(rename = "lastBackendPairDecision")]
    last_backend_pair_decision: String,
    #[serde(rename = "lastLogicalSnapshotDecision")]
    last_logical_snapshot_decision: String,
    #[serde(rename = "lastResolvedApplyDecision")]
    last_resolved_apply_decision: String,
    #[serde(rename = "lastPromotedSurfaceDecision")]
    last_promoted_surface_decision: String,
    #[serde(rename = "lastBootstrapPreflightDecision")]
    last_bootstrap_preflight_decision: String,
    #[serde(rename = "lastOutboundDecision")]
    last_outbound_decision: String,
    #[serde(rename = "lastInboundDecision")]
    last_inbound_decision: String,
}

impl State<HeterogeneousBehaviorDriver> for HeterogeneousBehaviorState {
    fn from_driver(driver: &HeterogeneousBehaviorDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_backend_pair_decision: driver.last_backend_pair_decision.clone(),
            last_logical_snapshot_decision: driver.last_logical_snapshot_decision.clone(),
            last_resolved_apply_decision: driver.last_resolved_apply_decision.clone(),
            last_promoted_surface_decision: driver.last_promoted_surface_decision.clone(),
            last_bootstrap_preflight_decision: driver.last_bootstrap_preflight_decision.clone(),
            last_outbound_decision: driver.last_outbound_decision.clone(),
            last_inbound_decision: driver.last_inbound_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct HeterogeneousBehaviorDriver {
    last_case: HeterogeneousCase,
    last_backend_pair_decision: String,
    last_logical_snapshot_decision: String,
    last_resolved_apply_decision: String,
    last_promoted_surface_decision: String,
    last_bootstrap_preflight_decision: String,
    last_outbound_decision: String,
    last_inbound_decision: String,
}

impl Default for HeterogeneousBehaviorDriver {
    fn default() -> Self {
        Self {
            last_case: HeterogeneousCase {
                source_backend: "sqlite".to_string(),
                destination_backend: "sqlite".to_string(),
                logical_snapshot_domains_complete: true,
                resolved_apply_complete: false,
                learner_promoted: false,
                bootstrap_destination_empty: true,
                bootstrap_preflight_marker_present: false,
                sync_role: SyncRoleName::Disabled,
                current_version: 0,
                incoming_version: 0,
            },
            last_backend_pair_decision: "not_checked".to_string(),
            last_logical_snapshot_decision: "not_checked".to_string(),
            last_resolved_apply_decision: "not_checked".to_string(),
            last_promoted_surface_decision: "not_checked".to_string(),
            last_bootstrap_preflight_decision: "not_checked".to_string(),
            last_outbound_decision: "not_checked".to_string(),
            last_inbound_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for HeterogeneousBehaviorDriver {
    type State = HeterogeneousBehaviorState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                sourceBackend: String,
                destinationBackend: String,
                logicalSnapshotDomainsComplete: bool,
                resolvedApplyComplete: bool,
                learnerPromoted: bool,
                bootstrapDestinationEmpty: bool,
                bootstrapPreflightMarkerPresent: bool,
                syncRole: SyncRoleName,
                currentVersion: u64,
                incomingVersion: u64,
            ) => {
                self.check(HeterogeneousCase {
                    source_backend: sourceBackend,
                    destination_backend: destinationBackend,
                    logical_snapshot_domains_complete: logicalSnapshotDomainsComplete,
                    resolved_apply_complete: resolvedApplyComplete,
                    learner_promoted: learnerPromoted,
                    bootstrap_destination_empty: bootstrapDestinationEmpty,
                    bootstrap_preflight_marker_present: bootstrapPreflightMarkerPresent,
                    sync_role: syncRole,
                    current_version: currentVersion,
                    incoming_version: incomingVersion,
                });
            },
            step(
                sourceBackend: String?,
                destinationBackend: String?,
                logicalSnapshotDomainsComplete: bool?,
                resolvedApplyComplete: bool?,
                learnerPromoted: bool?,
                bootstrapDestinationEmpty: bool?,
                bootstrapPreflightMarkerPresent: bool?,
                syncRole: SyncRoleName?,
                currentVersion: u64?,
                incomingVersion: u64?,
            ) => {
                if let (
                    Some(source_backend),
                    Some(destination_backend),
                    Some(logical_snapshot_domains_complete),
                    Some(resolved_apply_complete),
                    Some(learner_promoted),
                    Some(bootstrap_destination_empty),
                    Some(bootstrap_preflight_marker_present),
                    Some(sync_role),
                    Some(current_version),
                    Some(incoming_version),
                ) = (
                    sourceBackend,
                    destinationBackend,
                    logicalSnapshotDomainsComplete,
                    resolvedApplyComplete,
                    learnerPromoted,
                    bootstrapDestinationEmpty,
                    bootstrapPreflightMarkerPresent,
                    syncRole,
                    currentVersion,
                    incomingVersion,
                ) {
                    self.check(HeterogeneousCase {
                        source_backend,
                        destination_backend,
                        logical_snapshot_domains_complete,
                        resolved_apply_complete,
                        learner_promoted,
                        bootstrap_destination_empty,
                        bootstrap_preflight_marker_present,
                        sync_role,
                        current_version,
                        incoming_version,
                    });
                }
            },
        })
    }
}

impl HeterogeneousBehaviorDriver {
    fn check(&mut self, case: HeterogeneousCase) {
        let plan = plan_sync_heterogeneous_behavior(SyncHeterogeneousBehaviorGate {
            source_backend: &case.source_backend,
            destination_backend: &case.destination_backend,
            logical_snapshot_domains_complete: case.logical_snapshot_domains_complete,
            resolved_apply_complete: case.resolved_apply_complete,
            learner_promoted: case.learner_promoted,
            bootstrap_destination_empty: case.bootstrap_destination_empty,
            bootstrap_preflight_marker_present: case.bootstrap_preflight_marker_present,
            sync_role: sync_role(case.sync_role),
            current_version: case.current_version,
            incoming_version: case.incoming_version,
        });

        self.last_backend_pair_decision = backend_pair_decision_name(plan.backend_pair).to_string();
        self.last_logical_snapshot_decision =
            logical_snapshot_decision_name(plan.logical_snapshot).to_string();
        self.last_resolved_apply_decision =
            resolved_apply_decision_name(plan.resolved_apply).to_string();
        self.last_promoted_surface_decision =
            promoted_surface_decision_name(plan.promoted_surface).to_string();
        self.last_bootstrap_preflight_decision =
            bootstrap_preflight_decision_name(plan.bootstrap_preflight).to_string();
        self.last_outbound_decision =
            outbound_decision_name(plan.multi_region_interop.outbound).to_string();
        self.last_inbound_decision =
            inbound_decision_name(plan.multi_region_interop.inbound).to_string();
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

fn backend_pair_decision_name(decision: SyncBackendPairDecision) -> &'static str {
    match decision {
        SyncBackendPairDecision::ProductionSupported => "production_supported",
        SyncBackendPairDecision::ValidationOnly => "validation_only",
        SyncBackendPairDecision::Rejected => "rejected",
    }
}

fn logical_snapshot_decision_name(
    decision: SyncHeterogeneousLogicalSnapshotDecision,
) -> &'static str {
    match decision {
        SyncHeterogeneousLogicalSnapshotDecision::Allow => "allow",
        SyncHeterogeneousLogicalSnapshotDecision::Block(_) => "block",
    }
}

fn resolved_apply_decision_name(decision: SyncNonSqlResolvedApplyDecision) -> &'static str {
    match decision {
        SyncNonSqlResolvedApplyDecision::Allow => "allow",
        SyncNonSqlResolvedApplyDecision::Block(_) => "block",
    }
}

fn promoted_surface_decision_name(decision: SyncPromotedLearnerSurfaceDecision) -> &'static str {
    match decision {
        SyncPromotedLearnerSurfaceDecision::Allow => "allow",
        SyncPromotedLearnerSurfaceDecision::Block(_) => "block",
    }
}

fn bootstrap_preflight_decision_name(decision: LogicalBootstrapPreflightDecision) -> &'static str {
    match decision {
        LogicalBootstrapPreflightDecision::AllowEmptyDestination => "allow_empty_destination",
        LogicalBootstrapPreflightDecision::AllowRetryAfterPreflight => {
            "allow_retry_after_preflight"
        }
        LogicalBootstrapPreflightDecision::RejectNonEmptyDestination => {
            "reject_non_empty_destination"
        }
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
    spec = "../../quint/sync_heterogeneous_behavior_mbt.qnt",
    max_samples = 96,
    max_steps = 8,
    seed = "0x5e9be7"
)]
fn sync_heterogeneous_behavior_mbt_matches_rust_boundary() -> impl Driver {
    HeterogeneousBehaviorDriver::default()
}
