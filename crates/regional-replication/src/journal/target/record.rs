use replication_types::{configuration::installed::InstalledTableState, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};

use crate::bootstrap::{HiddenTargetRequest, HiddenTargetReservation};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetRecord {
    pub request: HiddenTargetRequest,
    pub content: CanonicalDigest,
    pub state: InstalledTableState,
    pub definition: Option<CanonicalDigest>,
    pub source_vector: Option<CanonicalDigest>,
    pub next_page: u64,
    pub baselines_complete: bool,
    pub last_baseline: Option<CanonicalDigest>,
    pub cancelled: bool,
    pub adoption: Option<TargetAdoptionRecord>,
    pub reenable: Option<TargetRetainedHistory>,
    pub verification: Option<crate::bootstrap::VerificationProgress>,
}

impl TargetRecord {
    pub(super) fn require_joining_import<'a>(
        &self,
        import: &'a replication_types::regional_import::RegionalImport,
        region: &replication_types::identity::replica::RegionId,
    ) -> storage_types::StorageResult<
        &'a replication_types::configuration::membership::RegionalTableBinding,
    > {
        let mut bindings = import
            .bindings()
            .iter()
            .filter(|binding| binding.local_table == self.state.configuration.table);
        let binding = bindings
            .next()
            .ok_or_else(crate::native_record::corrupt_record)?;
        if bindings.next().is_some() {
            return Err(crate::native_record::corrupt_record());
        }
        if self.adoption.is_none() && self.reenable.is_none() {
            self.state.validate_import((binding, &import.manifest().origin), region)
                .map_err(replication_provider::regional_application::error::RegionalApplicationError::from)?;
        }
        Ok(binding)
    }

    pub(super) fn require_baseline_scope(
        &self,
        baseline: &crate::journal::BaselineAuthority,
    ) -> storage_types::StorageResult<()> {
        if let Some(adoption) = &self.adoption {
            return adoption
                .definition
                .initial()
                .require_baseline_scope(baseline);
        }
        let item = baseline.item();
        if item.source_table != self.request.operation.table
            || item.schema != self.state.configuration.schema
            || item.lineage != self.state.configuration.lineage
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }

    pub fn reservation(&self) -> HiddenTargetReservation {
        HiddenTargetReservation {
            operation: self.request.operation.id,
            state: self.state.clone(),
            content: self.content,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BaselinePageReceipt {
    pub content: CanonicalDigest,
    pub complete: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetAdoptionRecord {
    pub definition: crate::history_collection::HistoryAdoptionDefinition,
    pub collection: crate::history_collection::HistoryCollectionRecord,
    pub declared_cuts: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetRetainedHistory {
    pub definition: crate::bootstrap::ReenableHistoryDefinition,
    pub next_ordinal: u64,
    pub vector: CanonicalDigest,
}

impl TargetRecord {
    pub(super) fn retained_inventory(
        &self,
    ) -> storage_types::StorageResult<crate::bootstrap::RetainedHistoryInventory> {
        match (&self.reenable, &self.adoption) {
            (Some(retained), None) => {
                crate::bootstrap::RetainedHistoryInventory::try_from(&retained.definition)
            }
            (None, Some(adoption)) => adoption
                .definition
                .retained_inventory()?
                .ok_or_else(crate::native_record::corrupt_record),
            _ => Err(crate::native_record::corrupt_record()),
        }
    }
}
