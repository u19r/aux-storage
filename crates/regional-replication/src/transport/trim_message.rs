//! Participant commands carry immutable intent and actual primary cancellation.
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{
    error::{TransportError, TransportResult},
    message::WireIdentity,
};
use crate::trim::{
    proposal::PublicTrimProposal,
    record::{TrimDecision, TrimObservation, TrimOutcome},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum TrimCommand {
    Authorize(crate::trim::grant_turn::TrimGrantTurn),
    Install(crate::trim::commit::TrimCommitCertificate),
    Finish(crate::trim::completion::TrimCompletionCertificate),
    Prepare(Arc<PublicTrimProposal>),
    Observe(Arc<PublicTrimProposal>),
    Cancel(TrimObservation),
}

impl TrimCommand {
    pub(super) fn proposal(&self) -> &PublicTrimProposal {
        match self {
            Self::Authorize(turn) => &turn.proposal,
            Self::Finish(completion) => &completion.certificate.proposal,
            Self::Install(certificate) => &certificate.proposal,
            Self::Prepare(proposal) | Self::Observe(proposal) => proposal,
            Self::Cancel(record) => &record.proposal,
        }
    }

    pub(super) fn validate(&self) -> TransportResult<()> {
        self.proposal().validate()?;
        if let Self::Authorize(turn) = self {
            turn.validate()?;
        }
        if let Self::Finish(completion) = self {
            completion.certificate.require_integrity()?;
        }
        if let Self::Install(certificate) = self {
            certificate.require_integrity()?;
        }
        if matches!(self, Self::Cancel(record) if record.decision != TrimDecision::Cancelled) {
            return Err(TransportError::Peer);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrimMessage {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub command: TrimCommand,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrimReply {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub proposal: PublicTrimProposal,
    pub result: Result<TrimOutcome, super::error::ImportNack>,
}

pub(super) fn require_trim_participant(
    proposal: &PublicTrimProposal,
    identity: &WireIdentity,
) -> TransportResult<()> {
    let participant = proposal.participant(&identity.region)?;
    if !participant.state.sources.iter().any(|source| {
        source.region == identity.region
            && source.database_group == identity.group
            && source.generation == identity.generation
            && source.table == participant.state.configuration.table
            && source.config == participant.state.configuration.config
    }) {
        return Err(TransportError::Peer);
    }
    Ok(())
}
