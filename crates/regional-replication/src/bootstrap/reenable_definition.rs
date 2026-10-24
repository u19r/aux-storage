//! Retained-history transfer input; native target verification still owns
//! activation.
use replication_types::{
    configuration::{installed::InstalledTableState, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::SourceCaptureDefinition;
use crate::{history::source_cut::CanonicalSourceCut, native_record::corrupt_record};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableHistoryDefinition {
    initial: SourceCaptureDefinition,
    disable: ControlProposal,
    inventory: CanonicalDigest,
    cuts: u64,
    content: CanonicalDigest,
}

impl ReenableHistoryDefinition {
    pub(crate) fn from_native(
        initial: SourceCaptureDefinition,
        disable: ControlProposal,
        inventory: (CanonicalDigest, u64),
    ) -> StorageResult<Self> {
        let content = CanonicalDigest::of_json(&(initial.content(), &disable, inventory))
            .map_err(|_| corrupt_record())?;
        let definition = Self {
            initial,
            disable,
            inventory: inventory.0,
            cuts: inventory.1,
            content,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        self.initial.validate()?;
        self.disable.validate().map_err(|_| corrupt_record())?;
        let proposal = self.initial.proposal();
        let home = &proposal.operation().home_region;
        let before = proposal
            .participant(home)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        let survivor = &self
            .disable
            .participant(home)
            .map_err(|_| corrupt_record())?
            .proposed;
        if self.cuts == 0
            || u64::from(before.configuration.config) == 0
            || before != survivor
            || !matches!(
                self.disable.operation().change,
                replication_types::configuration::control::ConfigurationChange::DisableGlobal
            )
            || self.disable.operation().lineage != before.configuration.lineage
            || proposal.operation().lineage == before.configuration.lineage
            || self.content
                != CanonicalDigest::of_json(&(
                    self.initial.content(),
                    &self.disable,
                    (self.inventory, self.cuts),
                ))
                .map_err(|_| corrupt_record())?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    /// Validate ordered transfer input; this is not proof of native target
    /// coverage.
    pub fn require_page(
        &self,
        page: &ReenableHistoryPage,
        expected: (u64, CanonicalDigest),
    ) -> StorageResult<CanonicalDigest> {
        super::RetainedHistoryInventory::try_from(self)?.require_page(page, expected)
    }

    pub fn initial(&self) -> &SourceCaptureDefinition {
        &self.initial
    }
    pub fn content(&self) -> CanonicalDigest {
        self.content
    }
    pub fn inventory(&self) -> CanonicalDigest {
        self.inventory
    }
    pub fn cuts(&self) -> u64 {
        self.cuts
    }

    pub fn inventory_seed(&self) -> StorageResult<CanonicalDigest> {
        let proposal = self.initial.proposal();
        let before = proposal
            .participant(&proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        CanonicalDigest::of_json(&(proposal.operation().content, before.physical))
            .map_err(|_| corrupt_record())
    }
}

/// Original scope and terminal native authority accompany every ordered cut.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableHistoryCut {
    pub ordinal: u64,
    pub custody: Option<super::ReenableHistoryCustody>,
    pub disable: ControlProposal,
    pub survivor: InstalledTableState,
    pub cut: CanonicalSourceCut,
    pub visibility: crate::history::trim_authority::HistoricalTrimSnapshot,
    pub baseline: super::ReenableBaselineDefinition,
}

impl ReenableHistoryCut {
    pub(crate) fn require_scope(&self, proposal: &ControlProposal) -> StorageResult<()> {
        self.disable.validate().map_err(|_| corrupt_record())?;
        let survivor = &self
            .disable
            .participant(&self.disable.operation().home_region)
            .map_err(|_| corrupt_record())?
            .proposed;
        let before = proposal
            .participant(&proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        self.baseline.require_lineage(survivor)?;
        if let Some(custody) = &self.custody {
            custody.require_custodian(proposal)?;
        } else if !super::ReenableHistoryCustody::is_same_incarnation(survivor, before) {
            return Err(corrupt_record());
        }
        self.require_original_survivor(survivor, before)?;
        if self.cut.lineage == proposal.operation().lineage {
            return Err(corrupt_record());
        }
        self.visibility.require_cut(&self.cut)
    }

    fn require_original_survivor(
        &self,
        survivor: &InstalledTableState,
        before: &InstalledTableState,
    ) -> StorageResult<()> {
        if !matches!(
            self.disable.operation().change,
            replication_types::configuration::control::ConfigurationChange::DisableGlobal
        ) || survivor != &self.survivor
            || self.disable.operation().lineage != self.cut.lineage
            || survivor.configuration.lineage != self.cut.lineage
            || self.cut.membership != survivor.membership
            || survivor.configuration.tenant != before.configuration.tenant
            || survivor.name != before.name
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn fold_inventory(
        &self,
        previous: CanonicalDigest,
    ) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(previous, self)).map_err(|_| corrupt_record())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableHistoryPage {
    pub definition: CanonicalDigest,
    pub cut: ReenableHistoryCut,
    pub next_ordinal: Option<u64>,
}

/// A page reference, never peer authority to select another historical scope.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableOriginalRequest {
    pub proposal: ControlProposal,
    pub definition: CanonicalDigest,
    pub ordinal: u64,
    pub after: replication_types::identity::origin::OriginPosition,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetRetainedHistoryObservation {
    pub definition: CanonicalDigest,
    pub next_ordinal: u64,
    pub vector: CanonicalDigest,
}
