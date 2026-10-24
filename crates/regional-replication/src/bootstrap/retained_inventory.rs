//! One frozen inventory commitment for installed custody and ordered history
//! declarations.
use replication_types::{
    configuration::{control::ConfigurationChange, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{ReenableHistoryDefinition, ReenableHistoryPage};
use crate::native_record::corrupt_record;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedHistoryInventory {
    pub(crate) proposal: ControlProposal,
    pub(crate) bootstrap: CanonicalDigest,
    pub(crate) cuts: u64,
    pub(crate) inventory: CanonicalDigest,
}

impl TryFrom<&ReenableHistoryDefinition> for RetainedHistoryInventory {
    type Error = storage_types::StorageError;
    fn try_from(definition: &ReenableHistoryDefinition) -> StorageResult<Self> {
        definition.validate()?;
        Ok(Self {
            proposal: definition.initial().proposal().clone(),
            bootstrap: definition.content(),
            cuts: definition.cuts(),
            inventory: definition.inventory(),
        })
    }
}

impl RetainedHistoryInventory {
    pub(crate) fn validate(&self) -> StorageResult<()> {
        self.proposal.validate().map_err(|_| corrupt_record())?;
        if self.cuts == 0 {
            return Err(corrupt_record());
        }
        match self.proposal.operation().change {
            ConfigurationChange::EnableGlobal { .. } => {
                replication_provider::control::source_capture_state(&self.proposal)
                    .map_err(|_| corrupt_record())?;
            }
            ConfigurationChange::AddRegion { .. } => {
                replication_provider::control::validate_addition(&self.proposal)
                    .map_err(|_| corrupt_record())?
            }
            _ => return Err(corrupt_record()),
        }
        self.inventory_seed()?;
        Ok(())
    }
    pub(crate) fn inventory_seed(&self) -> StorageResult<CanonicalDigest> {
        let before = self
            .proposal
            .participant(&self.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        CanonicalDigest::of_json(&(self.proposal.operation().content, before.physical))
            .map_err(|_| corrupt_record())
    }
    pub(crate) fn require_page(
        &self,
        page: &ReenableHistoryPage,
        expected: (u64, CanonicalDigest),
    ) -> StorageResult<CanonicalDigest> {
        self.validate()?;
        page.cut.require_scope(&self.proposal)?;
        let next = expected.0.checked_add(1).ok_or_else(corrupt_record)?;
        if (expected.0 == 0 && expected.1 != self.inventory_seed()?)
            || expected.0 >= self.cuts
            || page.cut.ordinal != expected.0
            || page.definition != self.bootstrap
            || page.next_ordinal != (next < self.cuts).then_some(next)
        {
            return Err(corrupt_record());
        }
        let vector = page.cut.fold_inventory(expected.1)?;
        if page.next_ordinal.is_none() && vector != self.inventory {
            return Err(corrupt_record());
        }
        Ok(vector)
    }
    pub(crate) fn require_native_inventory(&self, native: &Self) -> StorageResult<()> {
        if CanonicalDigest::of_json(self).map_err(|_| corrupt_record())?
            != CanonicalDigest::of_json(native).map_err(|_| corrupt_record())?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
