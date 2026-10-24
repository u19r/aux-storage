//! Bounded whole-prefix absence requires exact original and component progress.
use replication_types::{
    event::identity::CanonicalEventId, identity::origin::OriginPosition, protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::proposal::PublicTrimProposal;
use crate::{history::source_cut::CanonicalSourceCut, native_record::corrupt_record};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OriginalPrefixWitness {
    proposal: CanonicalDigest,
    pub(crate) identity: CanonicalEventId,
    pub(crate) digest: CanonicalDigest,
    pub(crate) component: u32,
    pub(crate) after: OriginPosition,
}

impl OriginalPrefixWitness {
    pub(crate) fn try_for_proposal(
        proposal: &PublicTrimProposal,
        original: (CanonicalEventId, CanonicalDigest),
    ) -> StorageResult<Self> {
        let first = proposal
            .candidate
            .sources
            .first()
            .ok_or_else(corrupt_record)?;
        let witness = Self {
            proposal: CanonicalDigest::of_json(proposal).map_err(|_| corrupt_record())?,
            identity: original.0,
            digest: original.1,
            component: 0,
            after: first.after,
        };
        witness.require_component(proposal)?;
        Ok(witness)
    }

    pub(crate) fn require_component<'proposal>(
        &self,
        proposal: &'proposal PublicTrimProposal,
    ) -> StorageResult<&'proposal CanonicalSourceCut> {
        let cut = proposal
            .candidate
            .sources
            .get(usize::try_from(self.component).map_err(|_| corrupt_record())?)
            .ok_or_else(corrupt_record)?;
        if self.proposal != CanonicalDigest::of_json(proposal).map_err(|_| corrupt_record())?
            || self.after < cut.after
            || self.after > cut.through
            || !self.identity.is_bound_to(proposal.candidate.lineage)
        {
            return Err(corrupt_record());
        }
        Ok(cut)
    }

    pub(crate) fn advance_component(
        &mut self,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<bool> {
        if self.after != self.require_component(proposal)?.through {
            return Ok(false);
        }
        let component = self.component.checked_add(1).ok_or_else(corrupt_record)?;
        let Some(next) = proposal
            .candidate
            .sources
            .get(usize::try_from(component).map_err(|_| corrupt_record())?)
        else {
            return Ok(true);
        };
        self.component = component;
        self.after = next.after;
        Ok(false)
    }
}
