use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::CONTROL_OPERATION_SUFFIX, control::ControlRecord, journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn observe_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: &ControlProposal,
    ) -> StorageResult<Option<ControlRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            operation.validate().map_err(|_| corrupt_record())?;
            self.load_control(attempt, operation, ReadProtection::Snapshot)
                .await?
                .map(|row| self.decode_control(&row, operation))
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: &ControlProposal,
        protection: ReadProtection,
    ) -> StorageResult<Option<DurableRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(CONTROL_OPERATION_SUFFIX, &operation.operation().id)?;
            let mut rows = self.records.load(attempt, &[key], protection).await?;
            Ok(rows.pop().flatten())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn decode_control(
        &self,
        row: &DurableRecord,
        operation: &ControlProposal,
    ) -> StorageResult<ControlRecord> {
        let record = self.decode_control_record(row)?;
        if &record.proposal != operation {
            return Err(corrupt_record());
        }
        Ok(record)
    }

    pub(in crate::journal) fn decode_control_record(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<ControlRecord> {
        let record: ControlRecord = self.records.decode(row)?;
        let operation = record.proposal.operation();
        record.proposal.validate().map_err(|_| corrupt_record())?;
        record.validate_evidence()?;
        if row.key != self.identity_key(CONTROL_OPERATION_SUFFIX, &operation.id)?
            || !operation.participants.contains(&self.region)
        {
            return Err(corrupt_record());
        }
        Ok(record)
    }
}
