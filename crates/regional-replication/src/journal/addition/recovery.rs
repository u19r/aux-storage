use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::{AddRegionPlan, AdditionPhase, AdditionRecord},
    constants::{ADDITION_OPERATION_SUFFIX, ADDITION_PENDING_SUFFIX, ADDITION_RECOVERY_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    /// Persist the round-robin cursor before peer work. A failed target leaves
    /// its pending entry intact while later operations remain discoverable.
    pub(crate) async fn select_pending_addition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<Option<AddRegionPlan>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let selected = self
                .select_pending_record(attempt, (ADDITION_PENDING_SUFFIX, ADDITION_RECOVERY_SUFFIX))
                .await?;
            let Some(row) = selected else {
                return Ok(None);
            };
            let operation: replication_types::identity::resource::ControlOperationId =
                self.records.decode(&row)?;
            if row.key != self.identity_key(ADDITION_PENDING_SUFFIX, &operation)? {
                return Err(corrupt_record());
            }
            let key = self.identity_key(ADDITION_OPERATION_SUFFIX, &operation)?;
            let roots = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let record: AdditionRecord = self
                .records
                .decode(roots[0].as_ref().ok_or_else(corrupt_record)?)?;
            record.plan.validate()?;
            if record.plan.operation.id != operation
                || record.content != record.plan.content()?
                || matches!(
                    record.phase,
                    AdditionPhase::Completed | AdditionPhase::Cancelled
                )
            {
                return Err(corrupt_record());
            }
            Ok(Some(record.plan))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn retire_pending_addition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        plan: &AddRegionPlan,
    ) -> StorageResult<RecordChange> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(ADDITION_PENDING_SUFFIX, &plan.operation.id)?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
            if self
                .records
                .decode::<replication_types::identity::resource::ControlOperationId>(row)?
                != plan.operation.id
            {
                return Err(corrupt_record());
            }
            Ok(RecordChange::Delete {
                key,
                expected: row.revision,
            })
        }
        .await;
        settlement.complete(result)
    }
}
