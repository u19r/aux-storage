mod cancellation;
mod observation;
mod preparation;
mod recovery;
mod transitions;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    enable::{EnableChange, EnablePlan, EnableRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn execute_enable<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        plan: &EnablePlan,
        change: &EnableChange,
    ) -> StorageResult<EnableRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            plan.validate()?;
            if plan.operation.home_region != self.region {
                return Err(corrupt_record());
            }
            let (row, mut record) = self
                .load_enable(attempt, plan, ReadProtection::Conflict)
                .await?;
            let Some(row) = row else {
                if !matches!(change, EnableChange::Observe) {
                    return Err(corrupt_record());
                }
                return self.prepare_enable(attempt, plan).await;
            };
            if record.phase == crate::enable::EnablePhase::Completed
                && matches!(change, EnableChange::Cancel)
            {
                return Err(storage_types::StorageError::unsupported(
                    "Committed global enable cannot be cancelled",
                ));
            }
            if matches!(
                record.phase,
                crate::enable::EnablePhase::Completed | crate::enable::EnablePhase::Cancelled
            ) {
                if let EnableChange::Reserve(state) = change
                    && !record.reservations.contains(state)
                {
                    return Err(corrupt_record());
                }
                return Ok(record);
            }
            match change {
                EnableChange::Observe => return Ok(record),
                EnableChange::Reserve(state) => {
                    self.retain_enable_reservation(plan, &mut record, state)?
                }
                EnableChange::Advance(expected) => {
                    self.advance_enable_phase(attempt, plan, &mut record, *expected)
                        .await?
                }
                EnableChange::Cancel => self.cancel_enable_control(attempt, &mut record).await?,
            }
            let changed = self.records.prepare(row.key, Some(row.revision), &record)?;
            let mut changes = vec![changed];
            if matches!(
                record.phase,
                crate::enable::EnablePhase::Completed | crate::enable::EnablePhase::Cancelled
            ) {
                changes.push(self.retire_pending_enable(attempt, plan).await?);
            }
            self.records.store(attempt, &changes).await?;
            Ok(record)
        }
        .await;
        settlement.complete(result)
    }
}

mod description;
