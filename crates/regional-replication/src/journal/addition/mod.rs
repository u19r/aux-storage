mod description;
mod observation;
mod preparation;
mod recovery;
mod source;
mod transitions;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::{AddRegionPlan, AdditionChange, AdditionPhase, AdditionRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn execute_addition<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        plan: &AddRegionPlan,
        change: &AdditionChange,
    ) -> StorageResult<AdditionRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            plan.validate()?;
            if plan.operation.home_region != self.region {
                return Err(corrupt_record());
            }
            let (row, mut record) = self
                .load_addition(attempt, plan, ReadProtection::Conflict)
                .await?;
            let Some(row) = row else {
                if !matches!(change, AdditionChange::Observe) {
                    return Err(corrupt_record());
                }
                return self.prepare_addition(attempt, plan).await;
            };
            if matches!(
                record.phase,
                AdditionPhase::Completed | AdditionPhase::Cancelled
            ) {
                return Ok(record);
            }
            match change {
                AdditionChange::Observe => return Ok(record),
                AdditionChange::Reserve(state) => {
                    self.retain_addition_reservation(plan, &mut record, state)?
                }
                AdditionChange::Advance(expected) => {
                    self.advance_addition_phase(attempt, &mut record, *expected)
                        .await?
                }
                AdditionChange::Cancel => {
                    self.cancel_addition_control(attempt, &mut record).await?
                }
            }
            let mut changes = vec![self.records.prepare(row.key, Some(row.revision), &record)?];
            if matches!(
                record.phase,
                AdditionPhase::Completed | AdditionPhase::Cancelled
            ) {
                changes.push(self.retire_pending_addition(attempt, plan).await?);
            }
            self.records.store(attempt, &changes).await?;
            Ok(record)
        }
        .await;
        settlement.complete(result)
    }
}

mod cancellation;
