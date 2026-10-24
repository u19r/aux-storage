use std::sync::Arc;

use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};

use super::{
    error::{ImportNack, TransportResult},
    message::WireIdentity,
};
use crate::control::{ControlDecision, ControlRecord, RetiredSealEvidence};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum ControlCommand {
    #[serde(rename = "PrepareConfiguration")]
    Prepare { proposal: Arc<ControlProposal> },
    #[serde(rename = "ObserveConfiguration")]
    Observe { proposal: Arc<ControlProposal> },
    #[serde(rename = "InstallConfiguration")]
    Install {
        proposal: Arc<ControlProposal>,
        bootstrap: Option<CanonicalDigest>,
        retirement: Vec<RetiredSealEvidence>,
    },
    #[serde(rename = "ReleaseConfiguration")]
    Release {
        proposal: Arc<ControlProposal>,
        bootstrap: Option<CanonicalDigest>,
        retirement: Vec<RetiredSealEvidence>,
    },
    #[serde(rename = "ReleaseCancelledConfiguration")]
    ReleaseCancelled {
        proposal: Arc<ControlProposal>,
        bootstrap: Option<CanonicalDigest>,
    },
}

impl ControlCommand {
    pub(super) fn proposal(&self) -> &Arc<ControlProposal> {
        match self {
            Self::Prepare { proposal }
            | Self::Observe { proposal }
            | Self::Install { proposal, .. }
            | Self::Release { proposal, .. }
            | Self::ReleaseCancelled { proposal, .. } => proposal,
        }
    }

    pub(super) fn validate(&self) -> TransportResult<()> {
        self.proposal().validate()?;
        match self {
            Self::Install {
                proposal,
                bootstrap,
                retirement,
            } => ControlRecord::require_evidence(
                proposal,
                ControlDecision::Committed,
                *bootstrap,
                retirement,
            )?,
            Self::Release {
                proposal,
                bootstrap,
                retirement,
            } => ControlRecord::require_evidence(
                proposal,
                ControlDecision::Completed,
                *bootstrap,
                retirement,
            )?,
            Self::ReleaseCancelled {
                proposal,
                bootstrap,
            } => ControlRecord::require_evidence(
                proposal,
                ControlDecision::Cancelled,
                *bootstrap,
                &[],
            )?,
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlMessage {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub command: ControlCommand,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlReply {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub result: Result<ControlRecord, ImportNack>,
}
