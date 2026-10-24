use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::AddRegionPlan,
    constants::{CONTROL_OPERATION_SUFFIX, CONTROL_SLOT_SUFFIX, HISTORY_CAPTURE_SUFFIX},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn finish_unreserved_addition_cancellation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        plan: &AddRegionPlan,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let table = plan.target.source.configuration.table;
            let slot_key = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
            let keys = [
                slot_key.clone(),
                self.identity_key(CONTROL_OPERATION_SUFFIX, &plan.operation.id)?,
                self.identity_key(HISTORY_CAPTURE_SUFFIX, &plan.operation.id)?,
            ];
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if rows[1..].iter().any(Option::is_some) {
                return Err(corrupt_record());
            }
            let slot = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let owner = self.decode_control_slot(slot, table)?;
            if owner != ControlSlotOwner::Configuration(plan.operation.id) {
                return Err(corrupt_record());
            }
            let change =
                self.prepare_control_slot(table, Some(slot.revision), ControlSlotOwner::Idle)?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement.complete(result)
    }
}
