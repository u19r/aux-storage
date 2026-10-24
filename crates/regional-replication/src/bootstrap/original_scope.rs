use std::sync::Arc;

use replication_types::{
    configuration::installed::RegionalSourceBinding, identity::replica::RegionId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{BaselineCopyScope, SourceCaptureCut};
use crate::{
    delivery::DeliveryScope, history_collection::HistoryAdoptionDefinition,
    native_record::corrupt_record,
};

/// Reference to a bounded fact in the primary's completed native collection.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AdoptedOriginalPage {
    Active {
        supplier: RegionId,
        after: u32,
    },
    Retired {
        supplier: RegionId,
        definition: CanonicalDigest,
        after: u32,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptedOriginalScope {
    pub transfer: BaselineCopyScope,
    pub definition: HistoryAdoptionDefinition,
    pub page: AdoptedOriginalPage,
    pub source: RegionalSourceBinding,
    pub cut: SourceCaptureCut,
}

impl AdoptedOriginalScope {
    pub(crate) fn delivery(&self) -> StorageResult<DeliveryScope> {
        self.transfer
            .require_definition(&self.definition.clone().into())?;
        let before = replication_provider::control::borrow_addition_source(
            &self.transfer.proposal,
            &self.transfer.proposal.operation().home_region,
        )
        .map_err(|_| corrupt_record())?;
        if self.source.region != self.cut.origin.region
            || self.source.database_group != self.cut.origin.database_group
            || self.source.generation != self.cut.origin.journal_generation
        {
            return Err(corrupt_record());
        }
        self.source
            .partitions
            .require_partition(self.cut.origin.journal_partition)
            .map_err(|_| corrupt_record())?;
        Ok(DeliveryScope {
            origin: self.cut.origin.clone(),
            table: self.source.table,
            local_table: before.configuration.table,
            lineage: before.configuration.lineage,
            membership: before.membership,
            destination: self.transfer.destination.clone(),
        })
    }
}

pub(crate) struct AdoptedOriginalExecution {
    pub scope: Arc<AdoptedOriginalScope>,
    pub import: Arc<replication_types::regional_import::RegionalImport>,
    pub receipt: Arc<crate::transport::RemoteImportReceipt>,
    pub codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
    pub limits: Arc<replication_provider::runtime_configuration::limits::WorkLimits>,
}
