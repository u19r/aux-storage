//! Every operation acquires journal reservations in the same region order.
use replication_types::{identity::replica::RegionId, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{
    proposal::PublicTrimProposal,
    record::{TrimDecision, TrimObservation},
};
use crate::{native_record::corrupt_record, transport::RemoteTrimReceipt};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrimParticipantGrant {
    pub(crate) region: RegionId,
    pub(crate) grant: CanonicalDigest,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrimGrantTurn {
    pub(crate) proposal: PublicTrimProposal,
    pub(crate) predecessors: Vec<TrimParticipantGrant>,
}

impl TrimGrantTurn {
    pub(crate) fn try_next(
        record: &TrimObservation,
        receipts: &[RemoteTrimReceipt],
    ) -> StorageResult<Option<Self>> {
        let proposal = &record.proposal;
        proposal.validate()?;
        if proposal.participants.len().checked_sub(1) != Some(receipts.len()) {
            return Err(corrupt_record());
        }
        let mut remote = receipts.iter();
        let grants = proposal
            .participants
            .iter()
            .map(|participant| {
                if participant.region == proposal.primary {
                    return require_local_grant(record);
                }
                remote
                    .next()
                    .ok_or_else(corrupt_record)?
                    .require_grant(proposal, &participant.region)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let Some(next) = grants.iter().position(Option::is_none) else {
            return Ok(None);
        };
        if grants[next..].iter().any(Option::is_some) {
            return Err(corrupt_record());
        }
        let predecessors = proposal
            .participants
            .iter()
            .zip(&grants)
            .take(next)
            .map(|(participant, grant)| {
                Ok(TrimParticipantGrant {
                    region: participant.region.clone(),
                    grant: grant.ok_or_else(corrupt_record)?,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let turn = Self {
            proposal: proposal.clone(),
            predecessors,
        };
        turn.validate()?;
        Ok(Some(turn))
    }

    pub(crate) fn target(&self) -> StorageResult<&RegionId> {
        self.proposal
            .participants
            .get(self.predecessors.len())
            .map(|participant| &participant.region)
            .ok_or_else(corrupt_record)
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        self.proposal.validate()?;
        self.target()?;
        if self
            .predecessors
            .iter()
            .zip(&self.proposal.participants)
            .any(|(grant, participant)| grant.region != participant.region)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

/// Configured-primary intent; decoding a turn alone does not authorize a grant.
pub struct PrimaryTrimGrantTurn {
    pub(crate) turn: TrimGrantTurn,
}

impl PrimaryTrimGrantTurn {
    pub(crate) fn try_from_primary(turn: TrimGrantTurn, primary: &RegionId) -> StorageResult<Self> {
        turn.validate()?;
        if turn.proposal.primary != *primary {
            return Err(corrupt_record());
        }
        Ok(Self { turn })
    }
}

fn require_local_grant(record: &TrimObservation) -> StorageResult<Option<CanonicalDigest>> {
    match record.decision {
        TrimDecision::Preparing => Ok(None),
        TrimDecision::Granted(grant) => Ok(Some(grant)),
        _ => Err(corrupt_record()),
    }
}
