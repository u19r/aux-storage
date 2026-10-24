use replication_types::{
    configuration::proposal::ControlProposal, identity::replica::RegionId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::native_record::corrupt_record;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCaptureRequest {
    pub proposal: ControlProposal,
    pub region: RegionId,
}

impl HistoryCaptureRequest {
    pub fn validate(&self) -> StorageResult<()> {
        replication_provider::control::borrow_addition_source(&self.proposal, &self.region)
            .map(|_| ())
            .map_err(|_| corrupt_record())
    }
    pub(crate) fn require_page(
        &self,
        page: &super::HistoryCaptureVectorPage,
        after: u32,
    ) -> StorageResult<()> {
        let source =
            replication_provider::control::borrow_addition_source(&self.proposal, &self.region)
                .map_err(|_| corrupt_record())?;
        let binding = source
            .sources
            .iter()
            .find(|binding| binding.region == self.region)
            .ok_or_else(corrupt_record)?;
        let width = u32::try_from(page.cuts.len()).map_err(|_| corrupt_record())?;
        let end = after.checked_add(width).ok_or_else(corrupt_record)?;
        let count = u32::from(binding.partitions);
        if page.content != self.content()?
            || width == 0
            || page.cuts.len() > crate::constants::HISTORY_CUT_PAGE_WIDTH
            || end > count
            || page.next_partition != (end < count).then_some(end)
        {
            return Err(corrupt_record());
        }
        for (offset, cut) in page.cuts.iter().enumerate() {
            if cut.origin.region != self.region
                || cut.origin.database_group != binding.database_group
                || cut.origin.journal_generation != binding.generation
                || u32::from(u16::from(cut.origin.journal_partition)) != after + offset as u32
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub fn content(&self) -> StorageResult<CanonicalDigest> {
        self.validate()?;
        CanonicalDigest::of_json(self).map_err(|_| corrupt_record())
    }
}
