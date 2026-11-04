use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::{AddRegionPlan, AdditionPhase, AdditionRecord},
    constants::ADDITION_OPERATION_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn load_addition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        plan: &AddRegionPlan,
        protection: ReadProtection,
    ) -> StorageResult<(Option<DurableRecord>, AdditionRecord)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(ADDITION_OPERATION_SUFFIX, &plan.operation.id)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten();
            let Some(row) = row else {
                return Ok((
                    None,
                    AdditionRecord {
                        content: plan.content()?,
                        plan: plan.clone(),
                        reservation: None,
                        proposal: None,
                        phase: AdditionPhase::Reserving,
                    },
                ));
            };
            let record = self.decode_addition_record(&row)?;
            if record.content != plan.content()? {
                return Err(corrupt_record());
            }
            Ok((Some(row), record))
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) fn decode_addition_record(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<AdditionRecord> {
        let record: AdditionRecord = self.records.decode(row)?;
        record.plan.validate()?;
        let plan = &record.plan;
        if row.key != self.identity_key(ADDITION_OPERATION_SUFFIX, &plan.operation.id)?
            || plan.content()? != record.content
        {
            return Err(corrupt_record());
        }
        match (&record.reservation, &record.proposal) {
            (Some(state), Some(proposal)) => {
                plan.require_reservation(plan.operation.id, plan.target.content()?, state)?;
                if &plan.bind_reserved_state(state.clone())? != proposal
                    || record.phase == AdditionPhase::Reserving
                {
                    return Err(corrupt_record());
                }
            }
            (None, None)
                if matches!(
                    record.phase,
                    AdditionPhase::Reserving | AdditionPhase::Cancelling | AdditionPhase::Cancelled
                ) => {}
            _ => return Err(corrupt_record()),
        }
        Ok(record)
    }
}
