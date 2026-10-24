//! Immutable trim intent fixes publication coverage and configured
//! incarnations.
use std::num::NonZeroU64;

use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalTablePhase},
        table::RegionalMode,
    },
    identity::{replica::RegionId, resource::ControlOperationId},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult};

use crate::history::{table_cut::CanonicalTableCut, trim_authority::TrimRevision};

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrimPolicy {
    pub minimum_retention_ms: NonZeroU64,
    pub clock_policy: CanonicalDigest,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrimParticipantBinding {
    pub region: RegionId,
    pub state: InstalledTableState,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicTrimProposal {
    pub operation: ControlOperationId,
    pub primary: RegionId,
    pub expected: TrimRevision,
    pub next: TrimRevision,
    pub candidate: CanonicalTableCut,
    pub participants: Vec<TrimParticipantBinding>,
    pub policy: TrimPolicy,
}

impl PublicTrimProposal {
    pub(crate) fn validate(&self) -> StorageResult<()> {
        self.candidate.require_inventory()?;
        if self.expected != self.candidate.trim
            || self.next != self.expected.checked_next()?
            || !self
                .candidate
                .sources
                .iter()
                .any(|cut| cut.through > cut.after)
            || self.participants.is_empty()
            || self
                .participants
                .windows(2)
                .any(|pair| pair[0].region >= pair[1].region)
        {
            return Err(StorageError::validation("Invalid public trim proposal"));
        }
        for participant in &self.participants {
            self.require_participant_scope(participant)?;
        }
        self.participant(&self.primary)?;
        Ok(())
    }

    pub(crate) fn participant(&self, region: &RegionId) -> StorageResult<&TrimParticipantBinding> {
        self.participants
            .iter()
            .find(|participant| &participant.region == region)
            .ok_or_else(|| StorageError::validation("Trim participant is absent"))
    }

    fn require_participant_scope(&self, participant: &TrimParticipantBinding) -> StorageResult<()> {
        let state = &participant.state;
        state
            .validate()
            .map_err(|_| crate::native_record::corrupt_record())?;
        let RegionalMode::Global { primary, members } = &state.configuration.regional else {
            return Err(StorageError::validation(
                "Public trim requires a global table",
            ));
        };
        if state.phase != RegionalTablePhase::Active
            || primary != &self.primary
            || state.configuration.lineage != self.candidate.lineage
            || state.membership != self.candidate.membership
            || members.len() != self.participants.len()
            || members
                .iter()
                .zip(&self.participants)
                .any(|(region, binding)| region != &binding.region)
            || !state.sources.iter().any(|source| {
                source.region == participant.region
                    && source.table == state.configuration.table
                    && source.config == state.configuration.config
            })
        {
            return Err(StorageError::validation("Trim participant scope differs"));
        }
        Ok(())
    }
}
