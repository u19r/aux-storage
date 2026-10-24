mod custody;
mod export;
mod inventory;
mod preparation;
mod record;
mod transfer;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
pub(in crate::journal) use record::InventorySettlement;
use record::{InventoryPhase, InventoryRecord, RetainedLineage};
use replication_types::{
    configuration::proposal::ControlProposal,
    identity::resource::{ControlOperationId, TableIdentity},
};
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{
    constants::{REENABLE_INVENTORY_SUFFIX, RETAINED_LINEAGE_SUFFIX},
    native_record::corrupt_record,
};

impl JournalStore {
    async fn load_reenable_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        protection: ReadProtection,
    ) -> StorageResult<Option<(DurableRecord, InventoryRecord)>> {
        let key = self.identity_key(REENABLE_INVENTORY_SUFFIX, &operation)?;
        let row = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten();
        let Some(row) = row else {
            return Ok(None);
        };
        let record: InventoryRecord = self.records.decode(&row)?;
        record.proposal.validate().map_err(|_| corrupt_record())?;
        let source = replication_provider::control::source_capture_state(&record.proposal)
            .map_err(|_| corrupt_record())?;
        self.require_local_source(&source)?;
        if record.proposal.operation().id != operation || row.key != key {
            return Err(corrupt_record());
        }
        self.require_inventory_phase(&record)?;
        Ok(Some((row, record)))
    }

    fn require_inventory_phase(&self, record: &InventoryRecord) -> StorageResult<()> {
        if let InventoryPhase::Inherited { custody, .. } = &record.phase {
            return custody.require_custodian(&record.proposal);
        }
        let (lineage, partition, source) = match &record.phase {
            InventoryPhase::Survivor {
                lineage, partition, ..
            } => (lineage, *partition, None),
            InventoryPhase::Retired {
                lineage,
                partition,
                source,
                ..
            } => (lineage, *partition, Some(source)),
            InventoryPhase::Baselines { lineage, .. } | InventoryPhase::Catalog { lineage, .. } => {
                (lineage, 0, None)
            }
            InventoryPhase::Ready | InventoryPhase::Released if record.cuts == 0 => {
                return Err(corrupt_record());
            }
            _ => return Ok(()),
        };
        self.require_retained_lineage(&record.proposal, lineage)?;
        let next = match &record.phase {
            InventoryPhase::Baselines { next, .. }
            | InventoryPhase::Survivor { next, .. }
            | InventoryPhase::Catalog { next, .. }
            | InventoryPhase::Retired { next, .. } => next,
            _ => return Err(corrupt_record()),
        };
        if *next != self.retained_lineage_key(lineage)? {
            return Err(corrupt_record());
        }
        let source = source
            .or_else(|| {
                lineage
                    .survivor
                    .sources
                    .iter()
                    .find(|source| source.region == self.region)
            })
            .ok_or_else(corrupt_record)?;
        if partition >= u32::from(source.partitions) {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn retained_lineage_prefix(
        &self,
        table: TableIdentity,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(RETAINED_LINEAGE_SUFFIX, &table)
    }

    fn require_retained_lineage(
        &self,
        proposal: &ControlProposal,
        lineage: &RetainedLineage,
    ) -> StorageResult<()> {
        lineage.disable.validate().map_err(|_| corrupt_record())?;
        let current = proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        let survivor = &lineage.survivor;
        let expected = &lineage
            .disable
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed;
        if !matches!(
            lineage.disable.operation().change,
            replication_types::configuration::control::ConfigurationChange::DisableGlobal
        ) || lineage.disable.operation().lineage != survivor.configuration.lineage
            || survivor != expected
            || survivor.configuration.lineage == proposal.operation().lineage
            || (
                survivor.configuration.table,
                survivor.configuration.tenant,
                survivor.physical,
                &survivor.name,
            ) != (
                current.configuration.table,
                current.configuration.tenant,
                current.physical,
                &current.name,
            )
        {
            return Err(corrupt_record());
        }
        self.require_local_source(survivor)
    }
}

mod baseline_inventory;
mod baseline_transfer;
mod inheritance;

mod transfer_progress;
