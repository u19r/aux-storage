//! Shared pending selection resolves the exact operation family before
//! dispatch.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{
        CONTROL_OPERATION_SUFFIX, CONTROL_PENDING_SUFFIX, CONTROL_RECOVERY_SUFFIX,
        PUBLIC_TRIM_OPERATION_SUFFIX, PUBLIC_TRIM_PROPOSAL_SUFFIX,
    },
    control::pending::{PendingControlReference, PendingControlWork},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn select_pending_work<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<Option<PendingControlWork>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let selected = self
                .select_pending_record(attempt, (CONTROL_PENDING_SUFFIX, CONTROL_RECOVERY_SUFFIX))
                .await?;
            let Some(row) = selected else {
                return Ok(None);
            };
            let reference = self.decode_pending_control_reference(&row)?;
            let suffix = match reference {
                PendingControlReference::Configuration(_) => CONTROL_OPERATION_SUFFIX,
                PendingControlReference::Trim(_) => PUBLIC_TRIM_OPERATION_SUFFIX,
            };
            let mut keys = vec![self.identity_key(suffix, &reference.operation())?];
            if matches!(reference, PendingControlReference::Trim(_)) {
                keys.push(self.identity_key(PUBLIC_TRIM_PROPOSAL_SUFFIX, &reference.operation())?);
            }
            let mut rows = self
                .records
                .load(attempt, &keys, ReadProtection::Shared)
                .await?;
            let row = rows[0].take().ok_or_else(corrupt_record)?;
            let work = match reference {
                PendingControlReference::Configuration(operation) => {
                    let control = self.decode_control_record(&row)?;
                    replication_provider::control::validate_pending_control(&control.proposal)
                        .map_err(|_| corrupt_record())?;
                    if control.proposal.operation().id != operation
                        || control.proposal.operation().home_region != self.region
                    {
                        return Err(corrupt_record());
                    }
                    PendingControlWork::Configuration(control.proposal)
                }
                PendingControlReference::Trim(operation) => {
                    let record = self.decode_public_trim_record(
                        &row,
                        rows[1].as_ref().ok_or_else(corrupt_record)?,
                    )?;
                    if record.proposal.operation != operation
                        || record.proposal.participant(&self.region).is_err()
                    {
                        return Err(corrupt_record());
                    }
                    PendingControlWork::Trim(record.proposal)
                }
            };
            Ok(Some(work))
        }
        .await;
        settlement.complete(result)
    }
}
