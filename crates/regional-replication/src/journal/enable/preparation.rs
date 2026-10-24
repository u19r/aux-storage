use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{
        CONTROL_OPERATION_SUFFIX, CONTROL_SLOT_SUFFIX, ENABLE_OPERATION_SUFFIX,
        ENABLE_PENDING_SUFFIX, SOURCE_CAPTURE_SUFFIX,
    },
    enable::{EnablePhase, EnablePlan, EnableRecord},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn prepare_enable<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        plan: &EnablePlan,
    ) -> StorageResult<EnableRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let source = &plan.targets[0].source;
            self.require_local_source(source)?;
            let observed = self
                .observe_table_state(attempt, source.configuration.table, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if observed.state() != source {
                return Err(corrupt_record());
            }
            self.require_enable_source_history(attempt, source).await?;
            let schema = self.freeze_capture_schema(attempt, source).await?;
            if serde_json::to_vec(&schema).map_err(|_| corrupt_record())?
                != serde_json::to_vec(&plan.targets[0].schema).map_err(|_| corrupt_record())?
            {
                return Err(corrupt_record());
            }
            let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &source.configuration.table)?;
            let keys = [
                slot.clone(),
                self.identity_key(CONTROL_OPERATION_SUFFIX, &plan.operation.id)?,
                self.identity_key(SOURCE_CAPTURE_SUFFIX, &plan.operation.id)?,
            ];
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if rows[1..].iter().any(Option::is_some) {
                return Err(corrupt_record());
            }
            if let Some(row) = &rows[0]
                && self.decode_control_slot(row, source.configuration.table)?
                    != ControlSlotOwner::Idle
            {
                return Err(corrupt_record());
            }
            let record = EnableRecord {
                content: plan.content()?,
                plan: plan.clone(),
                reservations: Vec::new(),
                proposal: None,
                phase: EnablePhase::Reserving,
            };
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let key = self.identity_key(ENABLE_OPERATION_SUFFIX, &plan.operation.id)?;
            let pending = self.identity_key(ENABLE_PENDING_SUFFIX, &plan.operation.id)?;
            let changes = [
                self.records.prepare(key, None, &record)?,
                self.records.prepare(pending, None, &plan.operation.id)?,
                self.prepare_control_slot(
                    source.configuration.table,
                    rows[0].as_ref().map(|row| row.revision),
                    ControlSlotOwner::Configuration(plan.operation.id),
                )?,
            ];
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            Ok(record)
        }
        .await;
        settlement.complete(result)
    }
}
