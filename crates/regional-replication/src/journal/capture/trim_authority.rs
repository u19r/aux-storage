//! New capture lineage is born under its exact native initial record and
//! control slot.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::InstalledTableState;
use storage_types::{ReadProtection, Revision, StorageResult};

use super::record::{CapturePhase, CaptureRecord};
use crate::{
    constants::{CONTROL_SLOT_SUFFIX, SOURCE_CAPTURE_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn is_cancelled_trim_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: replication_types::identity::resource::ControlOperationId,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(SOURCE_CAPTURE_SUFFIX, &operation)?;
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
            let record: CaptureRecord = self.records.decode(&row)?;
            replication_provider::control::source_capture_state(&record.proposal)
                .map_err(|_| corrupt_record())?;
            if row.key != key
                || record.proposal.operation().id != operation
                || record.proposal.operation().home_region != self.region
            {
                return Err(corrupt_record());
            }
            Ok(matches!(record.phase, CapturePhase::Cancelled))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_fresh_trim_lineage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        previous: &InstalledTableState,
        next: &InstalledTableState,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(CONTROL_SLOT_SUFFIX, &previous.configuration.table)?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let operation = self
                .decode_control_slot(&row, previous.configuration.table)?
                .require_configuration()?;
            let key = self.identity_key(SOURCE_CAPTURE_SUFFIX, &operation)?;
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
            let record: CaptureRecord = self.records.decode(&row)?;
            let capture = replication_provider::control::source_capture_state(&record.proposal)
                .map_err(|_| corrupt_record())?;
            let before = record
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            if row.key != key
                || row.revision != Revision::INITIAL
                || record.proposal.operation().id != operation
                || record.proposal.operation().home_region != self.region
                || !matches!(record.phase, CapturePhase::Holding(0))
                || previous != before
                || next != &capture
                || previous.physical != next.physical
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
