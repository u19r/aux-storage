use replication_types::{
    configuration::{
        installed::InstalledTableState, proposal::ControlProposal, table::RegionalMode,
    },
    identity::replica::RegionId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::native_record::corrupt_record;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealRequest {
    pub(crate) proposal: ControlProposal,
    pub(crate) region: RegionId,
}

impl SealRequest {
    pub fn try_new(proposal: ControlProposal, region: RegionId) -> StorageResult<Self> {
        let request = Self { proposal, region };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        replication_provider::control::validate_removal(&self.proposal)
            .map_err(|_| corrupt_record())?;
        if !replication_provider::control::retiring_regions(&self.proposal)
            .map_err(|_| corrupt_record())?
            .contains(&self.region)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn before(&self) -> StorageResult<&InstalledTableState> {
        self.proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)
    }

    pub(crate) fn source(
        &self,
    ) -> StorageResult<&replication_types::configuration::installed::RegionalSourceBinding> {
        self.before()?
            .sources
            .iter()
            .find(|source| source.region == self.region)
            .ok_or_else(corrupt_record)
    }

    pub(crate) fn survivors(&self) -> StorageResult<&[RegionId]> {
        let primary = &self
            .proposal
            .participant(&self.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .proposed;
        Ok(match &primary.configuration.regional {
            RegionalMode::Global { members, .. } => members,
            RegionalMode::Local => std::slice::from_ref(&self.proposal.operation().home_region),
        })
    }

    pub(crate) fn content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(self).map_err(|_| corrupt_record())
    }
    pub(crate) fn require_history_cut(
        &self,
        cut: (
            &replication_types::identity::origin::OriginId,
            replication_types::identity::origin::OriginPosition,
            &[(
                RegionId,
                replication_types::identity::origin::OriginPosition,
            )],
        ),
        partition: u32,
        survivors: &[RegionId],
    ) -> StorageResult<()> {
        let (origin, through, receipts) = cut;
        let source = self.source()?;
        let actual = (
            &origin.region,
            origin.database_group,
            origin.journal_generation,
            u32::from(u16::from(origin.journal_partition)),
        );
        let expected = (
            &source.region,
            source.database_group,
            source.generation,
            partition,
        );
        if actual != expected
            || partition >= u32::from(source.partitions)
            || receipts.len() != survivors.len()
        {
            return Err(corrupt_record());
        }
        if receipts
            .iter()
            .zip(survivors)
            .any(|((region, covered), survivor)| region != survivor || *covered < through)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
