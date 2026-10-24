//! Durable operation phases are observations, never grant capabilities.
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::proposal::PublicTrimProposal;

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum TrimDecision {
    Preparing,
    Granted(replication_types::protocol::CanonicalDigest),
    Committed(replication_types::protocol::CanonicalDigest),
    Installed(replication_types::protocol::CanonicalDigest),
    Completed(replication_types::protocol::CanonicalDigest),
    Cancelled,
}

impl TrimDecision {
    pub(crate) fn satisfies_observation(self, expected: Self) -> bool {
        self == expected || (expected == Self::Preparing && matches!(self, Self::Granted(_)))
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrimObservation {
    pub proposal: PublicTrimProposal,
    pub decision: TrimDecision,
    pub(crate) released_grant: Option<replication_types::protocol::CanonicalDigest>,
}

impl TrimObservation {
    pub(crate) fn require_proposal(&self, proposal: &PublicTrimProposal) -> StorageResult<()> {
        if &self.proposal != proposal
            || (self.decision != TrimDecision::Cancelled && self.released_grant.is_some())
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

/// Participant release is issued only by the native primary or its validated
/// configured transport boundary; an arbitrary public observation is not a
/// capability.
pub struct PrimaryTrimCancellation {
    pub(crate) proposal: PublicTrimProposal,
}

impl PrimaryTrimCancellation {
    pub(crate) fn try_from_primary(
        record: TrimObservation,
        primary: &replication_types::identity::replica::RegionId,
    ) -> StorageResult<Self> {
        record.proposal.validate()?;
        if record.decision != TrimDecision::Cancelled || &record.proposal.primary != primary {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self {
            proposal: record.proposal,
        })
    }
}

/// Scheduler readiness never authorizes retention or a phase transition.
#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum TrimWorkState {
    Ready,
    Waiting,
    AwaitingQualification,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrimOutcome {
    pub observation: Option<TrimObservation>,
    #[serde(skip)]
    pub(crate) certificate: Option<super::commit::TrimCommitCertificate>,
    pub installation: Option<super::completion::TrimInstalledParticipant>,
    #[serde(skip)]
    pub(crate) completion: Option<super::completion::TrimCompletionCertificate>,
    pub completion_released: bool,
    pub work: TrimWorkState,
    pub participants_prepared: bool,
    pub cancellation_released: bool,
    #[serde(skip)]
    pub(crate) prefix: Option<crate::journal::public_trim_scan::CompleteTrimPrefix>,
    #[serde(skip)]
    pub(crate) grant_turn: Option<super::grant_turn::TrimGrantTurn>,
}
