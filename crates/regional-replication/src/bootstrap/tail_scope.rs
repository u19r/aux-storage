use std::sync::Arc;

use replication_types::event::interval::ClosedSourceInterval;
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{BaselineCopyScope, SourceCaptureCut};
use crate::{delivery::DeliveryScope, native_record::corrupt_record};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapTailScope {
    pub transfer: BaselineCopyScope,
    pub cut: SourceCaptureCut,
}

impl BootstrapTailScope {
    pub(crate) fn delivery(&self) -> StorageResult<DeliveryScope> {
        self.transfer.validate()?;
        replication_provider::control::source_capture_state(&self.transfer.proposal)
            .map_err(|_| corrupt_record())?;
        let source = self
            .transfer
            .proposal
            .participant(&self.transfer.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?;
        let binding = source
            .proposed
            .sources
            .iter()
            .find(|binding| binding.region == self.cut.origin.region)
            .ok_or_else(corrupt_record)?;
        if self.cut.origin.region != self.transfer.proposal.operation().home_region
            || binding.database_group != self.cut.origin.database_group
            || binding.generation != self.cut.origin.journal_generation
        {
            return Err(corrupt_record());
        }
        binding
            .partitions
            .require_partition(self.cut.origin.journal_partition)
            .map_err(|_| corrupt_record())?;
        Ok(DeliveryScope {
            origin: self.cut.origin.clone(),
            table: source.proposed.configuration.table,
            local_table: source.proposed.configuration.table,
            lineage: source.proposed.configuration.lineage,
            membership: source.proposed.membership,
            destination: self.transfer.destination.clone(),
        })
    }

    pub(crate) fn require_range(&self, range: ClosedSourceInterval) -> StorageResult<()> {
        if range.through() > self.cut.through {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

pub(crate) struct BootstrapTailExecution {
    pub scope: Arc<BootstrapTailScope>,
    pub import: Arc<replication_types::regional_import::RegionalImport>,
    pub receipt: Arc<crate::transport::RemoteImportReceipt>,
    pub codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
    pub limits: Arc<replication_provider::runtime_configuration::limits::WorkLimits>,
}
