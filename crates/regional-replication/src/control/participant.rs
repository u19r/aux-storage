use replication_types::{configuration::proposal::ControlProposal, identity::replica::RegionId};

use super::{ControlDecision, DurableGrant, DurableInstallation};
use crate::transport::RemoteControlReceipt;

/// A configured peer declaration stays distinct from local native evidence.
pub enum ParticipantEvidence {
    LocalGrant(DurableGrant),
    LocalInstallation(DurableInstallation),
    Remote(RemoteControlReceipt),
}

impl ParticipantEvidence {
    pub(crate) fn retirement(&self) -> &[super::RetiredSealEvidence] {
        match self {
            Self::LocalGrant(_) => &[],
            Self::LocalInstallation(receipt) => &receipt.retirement,
            Self::Remote(receipt) => receipt.retirement(),
        }
    }

    pub(crate) fn bootstrap(&self) -> Option<replication_types::protocol::CanonicalDigest> {
        match self {
            Self::LocalGrant(grant) => grant.bootstrap,
            Self::LocalInstallation(receipt) => receipt.bootstrap,
            Self::Remote(receipt) => receipt.bootstrap(),
        }
    }

    pub(crate) fn matches(
        &self,
        proposal: &ControlProposal,
        expected: ControlDecision,
        region: &RegionId,
    ) -> bool {
        match self {
            Self::LocalGrant(grant) => {
                expected == ControlDecision::Prepared
                    && &grant.proposal == proposal
                    && &grant.region == region
            }
            Self::LocalInstallation(receipt) => {
                expected == ControlDecision::Installed
                    && &receipt.proposal == proposal
                    && &receipt.region == region
            }
            Self::Remote(receipt) => receipt.matches(proposal, expected, region),
        }
    }
}
