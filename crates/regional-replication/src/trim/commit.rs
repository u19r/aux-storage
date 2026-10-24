//! Complete commit evidence is descriptive until its native owner validates
//! custody.
use replication_types::{identity::replica::RegionId, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult};

use super::{
    grant_turn::{TrimGrantTurn, TrimParticipantGrant},
    proposal::PublicTrimProposal,
};
use crate::{
    history::source_floor::SourcePublicationFloors, native_record::corrupt_record,
    transport::RemoteTrimReceipt,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrimCommitCertificate {
    pub(crate) proposal: PublicTrimProposal,
    grants: Vec<TrimParticipantGrant>,
}

impl TrimCommitCertificate {
    pub(crate) fn require_grant_turn(
        &self,
        turn: &TrimGrantTurn,
        region: &RegionId,
    ) -> StorageResult<()> {
        self.require_proposal(&turn.proposal)?;
        turn.validate()?;
        if turn.target()? != region
            || self.grants.get(..turn.predecessors.len()) != Some(turn.predecessors.as_slice())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
    pub(crate) fn try_from_grants(
        proposal: &PublicTrimProposal,
        primary: &RegionId,
        local_grant: CanonicalDigest,
        receipts: &[RemoteTrimReceipt],
    ) -> StorageResult<Self> {
        proposal.validate().map_err(|_| corrupt_record())?;
        if *primary != proposal.primary
            || proposal.participants.len().checked_sub(1) != Some(receipts.len())
        {
            return Err(corrupt_record());
        }
        let mut remote = receipts.iter();
        let mut grants = Vec::with_capacity(proposal.participants.len());
        for participant in &proposal.participants {
            let grant = if participant.region == *primary {
                local_grant
            } else {
                remote
                    .next()
                    .ok_or_else(corrupt_record)?
                    .require_grant(proposal, &participant.region)?
                    .ok_or_else(|| {
                        StorageError::unsupported("Trim participant grant is unresolved")
                    })?
            };
            grants.push(TrimParticipantGrant {
                region: participant.region.clone(),
                grant,
            });
        }
        Ok(Self {
            proposal: proposal.clone(),
            grants,
        })
    }

    pub(crate) fn require_proposal(&self, proposal: &PublicTrimProposal) -> StorageResult<()> {
        if self.proposal != *proposal {
            return Err(corrupt_record());
        }
        self.require_integrity()
    }

    pub(crate) fn require_integrity(&self) -> StorageResult<()> {
        self.proposal.validate().map_err(|_| corrupt_record())?;
        if self.grants.len() != self.proposal.participants.len()
            || self
                .grants
                .iter()
                .zip(&self.proposal.participants)
                .any(|(grant, participant)| grant.region != participant.region)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn require_installed_floors(
        &self,
        floors: &SourcePublicationFloors,
    ) -> StorageResult<()> {
        self.require_integrity()?;
        floors.require_installation(&self.proposal.candidate)?;
        for participant in &self.proposal.participants {
            floors.require_sources(&participant.state)?;
        }
        Ok(())
    }

    pub(crate) fn grant_for(&self, region: &RegionId) -> StorageResult<CanonicalDigest> {
        self.grants
            .iter()
            .find(|grant| grant.region == *region)
            .map(|grant| grant.grant)
            .ok_or_else(corrupt_record)
    }
}
