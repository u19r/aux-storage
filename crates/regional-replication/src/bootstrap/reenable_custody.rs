//! Frozen inherited scope references remain distinct from original local
//! disable receipts.
use replication_types::configuration::{installed::InstalledTableState, proposal::ControlProposal};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::RetainedHistoryInventory;
use crate::native_record::corrupt_record;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableHistoryCustody {
    pub definition: RetainedHistoryInventory,
    pub ordinal: u64,
}

impl ReenableHistoryCustody {
    pub(crate) fn require_custodian(&self, proposal: &ControlProposal) -> StorageResult<()> {
        self.definition.validate()?;
        let previous = &self.definition.proposal;
        let home = &proposal.operation().home_region;
        let local = &previous
            .participant(home)
            .map_err(|_| corrupt_record())?
            .proposed;
        let before = proposal
            .participant(home)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        if previous.operation().id == proposal.operation().id
            || self.ordinal >= self.definition.cuts
            || local.configuration.lineage != before.configuration.lineage
            || !Self::is_same_incarnation(local, before)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
    pub(crate) fn is_same_incarnation(
        left: &InstalledTableState,
        right: &InstalledTableState,
    ) -> bool {
        (
            left.configuration.table,
            left.configuration.tenant,
            left.physical,
            &left.name,
        ) == (
            right.configuration.table,
            right.configuration.tenant,
            right.physical,
            &right.name,
        )
    }
}
