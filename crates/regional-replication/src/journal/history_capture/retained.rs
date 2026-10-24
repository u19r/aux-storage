//! AddRegion pins the primary's complete installed bootstrap across bounded
//! capture and cleanup.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordChange},
};
use replication_types::identity::resource::ControlOperationId;
use storage_types::{ReadProtection, StorageResult};

use super::record::{HistoryCapturePhase, HistoryCaptureRecord};
use crate::{
    history::hold_discovery::ReaderHoldObservation,
    journal::{JournalStore, reader_hold::RetentionHoldReference},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) fn adoption_inventory_key(
        &self,
        operation: ControlOperationId,
    ) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::READER_HOLD_SUFFIX.to_vec();
        suffix.push(5);
        suffix.extend_from_slice(operation.as_ref());
        self.record_key(suffix)
    }

    pub(super) async fn retain_adoption_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &HistoryCaptureRecord,
        admission: super::admission::AdoptionAdmission,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            admission.require_capture(record)?;
            let operation = record.request.proposal.operation();
            self.retain_history_epoch(attempt, operation.lineage)
                .await?;
            let key = self.adoption_inventory_key(operation.id)?;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(
                        key,
                        None,
                        &RetentionHoldReference::Adoption(operation.id),
                    )?],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn require_adoption_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &HistoryCaptureRecord,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let operation = record.request.proposal.operation().id;
            let key = self.adoption_inventory_key(operation)?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten();
            let is_live = record.retained.is_some()
                && !matches!(
                    record.phase,
                    HistoryCapturePhase::Admitting { .. }
                        | HistoryCapturePhase::Released
                        | HistoryCapturePhase::Cancelled
                );
            if !is_live {
                return if row.is_none() {
                    Ok(())
                } else {
                    Err(corrupt_record())
                };
            }
            let row = row.ok_or_else(corrupt_record)?;
            if self.decode_retention_hold(&row)? != RetentionHoldReference::Adoption(operation) {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn settle_adoption_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &HistoryCaptureRecord,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if record.retained.is_none()
                || !matches!(
                    record.phase,
                    HistoryCapturePhase::Released | HistoryCapturePhase::Cancelled
                )
            {
                return Ok(());
            }
            let operation = record.request.proposal.operation();
            let key = self.adoption_inventory_key(operation.id)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            if self.decode_retention_hold(&row)? != RetentionHoldReference::Adoption(operation.id) {
                return Err(corrupt_record());
            }
            self.retain_history_epoch(attempt, operation.lineage)
                .await?;
            self.records
                .store(
                    attempt,
                    &[RecordChange::Delete {
                        key,
                        expected: row.revision,
                    }],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn observe_adoption_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        protection: ReadProtection,
    ) -> StorageResult<ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let record = self
                .load_live_adoption_inventory(attempt, operation, protection)
                .await?;
            let definition = record.retained.as_ref().ok_or_else(corrupt_record)?;
            Ok(ReaderHoldObservation::AdoptionInventory {
                operation,
                table: record.request.proposal.operation().table,
                bootstrap: definition.content(),
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn load_adoption_trim_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
    ) -> StorageResult<
        Option<(
            replication_types::configuration::proposal::ControlProposal,
            crate::bootstrap::RetainedHistoryInventory,
        )>,
    > {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let record = self
                .load_live_adoption_inventory(attempt, operation, ReadProtection::Shared)
                .await?;
            let inventory = record
                .retained
                .as_ref()
                .ok_or_else(corrupt_record)?
                .retained_inventory()?;
            Ok(inventory.map(|inventory| (record.request.proposal, inventory)))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_live_adoption_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        protection: ReadProtection,
    ) -> StorageResult<HistoryCaptureRecord> {
        let key = self.identity_key(crate::constants::HISTORY_CAPTURE_SUFFIX, &operation)?;
        let row = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        let record: HistoryCaptureRecord = self.records.decode(&row)?;
        record.require_local(&self.region, self.partitions)?;
        if row.key != key
            || record.request.proposal.operation().id != operation
            || matches!(
                record.phase,
                HistoryCapturePhase::Admitting { .. }
                    | HistoryCapturePhase::Released
                    | HistoryCapturePhase::Cancelled
            )
        {
            return Err(corrupt_record());
        }
        self.require_adoption_inventory(attempt, &record, protection)
            .await?;
        Ok(record)
    }
}

impl JournalStore {
    pub(crate) async fn load_capture_retained_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &crate::history_capture::HistoryCaptureRequest,
        protection: ReadProtection,
    ) -> StorageResult<Option<crate::bootstrap::RetainedHistoryInventory>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, record) = self
                .load_history_capture(attempt, request, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(record.phase, HistoryCapturePhase::Ready) {
                return Err(corrupt_record());
            }
            record
                .retained
                .ok_or_else(corrupt_record)?
                .retained_inventory()
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn load_capture_retained_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &crate::history_capture::HistoryCaptureRequest,
        ordinal: u64,
        protection: ReadProtection,
    ) -> StorageResult<crate::bootstrap::ReenableHistoryPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = self
                .load_capture_retained_inventory(attempt, request, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            let custody = crate::bootstrap::ReenableHistoryCustody {
                definition,
                ordinal,
            };
            self.load_retained_custody_page(attempt, &request.proposal, &custody, protection)
                .await
        }
        .await;
        settlement_guard.complete(result)
    }
}
