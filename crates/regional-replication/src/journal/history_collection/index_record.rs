//! Immutable source-vector indexes bind their address and frozen definition.
use infrastructure_provider::record::{DurableRecord, DurableRecordKey, RecordChange};
use replication_types::identity::{replica::RegionId, resource::ControlOperationId};
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

use crate::{
    history_capture::{HistoryCaptureDefinition, HistoryCaptureVectorPage},
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct CollectedHistoryPage {
    operation: ControlOperationId,
    region: RegionId,
    partition: u32,
    page: HistoryCaptureVectorPage,
}

impl CollectedHistoryPage {
    pub(in crate::journal) fn definition_key(
        &self,
        journal: &JournalStore,
    ) -> StorageResult<DurableRecordKey> {
        journal.identity_key(
            crate::constants::COLLECTED_HISTORY_DEFINITION_SUFFIX,
            &(self.operation, &self.region),
        )
    }

    pub(in crate::journal) fn qualify(
        self,
        definition: &HistoryCaptureDefinition,
    ) -> StorageResult<HistoryCaptureVectorPage> {
        let request = definition.request();
        if self.operation != request.proposal.operation().id || self.region != request.region {
            return Err(corrupt_record());
        }
        definition.require_page(&self.page, self.partition)?;
        Ok(self.page)
    }
}

impl JournalStore {
    pub(in crate::journal) fn decode_collected_history_definition(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<HistoryCaptureDefinition> {
        let definition: HistoryCaptureDefinition = self.records.decode(row)?;
        definition.validate()?;
        let request = definition.request();
        self.require_collection_request(request)?;
        if row.revision != Revision::INITIAL
            || row.key
                != self.identity_key(
                    crate::constants::COLLECTED_HISTORY_DEFINITION_SUFFIX,
                    &(request.proposal.operation().id, &request.region),
                )?
        {
            return Err(corrupt_record());
        }
        Ok(definition)
    }

    pub(in crate::journal) fn decode_collected_history_page(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<CollectedHistoryPage> {
        let record: CollectedHistoryPage = self.records.decode(row)?;
        if row.revision != Revision::INITIAL
            || row.key
                != self.identity_key(
                    crate::constants::COLLECTED_HISTORY_PAGE_SUFFIX,
                    &(record.operation, &record.region, record.partition),
                )?
        {
            return Err(corrupt_record());
        }
        Ok(record)
    }

    pub(in crate::journal) fn require_collected_baseline_definition(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<()> {
        let definition: crate::history_capture::RetainedBaselineDefinition =
            self.records.decode(row)?;
        definition.validate()?;
        let request = definition.request();
        self.require_collection_request(request)?;
        if row.revision != Revision::INITIAL
            || row.key
                != self.identity_key(
                    crate::constants::COLLECTED_BASELINE_DEFINITION_SUFFIX,
                    &(request.proposal.operation().id, &request.region),
                )?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn prepare_collected_history_page(
        &self,
        definition: &HistoryCaptureDefinition,
        partition: u32,
        page: &HistoryCaptureVectorPage,
    ) -> StorageResult<RecordChange> {
        definition.require_page(page, partition)?;
        let request = definition.request();
        let record = CollectedHistoryPage {
            operation: request.proposal.operation().id,
            region: request.region.clone(),
            partition,
            page: page.clone(),
        };
        let key = self.identity_key(
            crate::constants::COLLECTED_HISTORY_PAGE_SUFFIX,
            &(record.operation, &record.region, partition),
        )?;
        self.records.prepare(key, None, &record)
    }
}
