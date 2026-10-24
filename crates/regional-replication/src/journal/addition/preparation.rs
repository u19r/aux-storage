use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::{AddRegionPlan, AdditionPhase, AdditionRecord},
    constants::{
        ADDITION_OPERATION_SUFFIX, ADDITION_PENDING_SUFFIX, ADDITION_TABLE_SUFFIX,
        CONTROL_OPERATION_SUFFIX, CONTROL_SLOT_SUFFIX, HISTORY_CAPTURE_SUFFIX,
    },
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn prepare_addition<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        plan: &AddRegionPlan,
    ) -> StorageResult<AdditionRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let source = &plan.target.source;
            self.require_local_source(source)?;
            let observed = self
                .observe_table_state(attempt, source.configuration.table, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if observed.state() != source {
                return Err(corrupt_record());
            }
            let current = self
                .load_configuration_control(attempt, source, ReadProtection::Shared)
                .await?;
            plan.require_current(&current.proposal)?;
            let primary = current
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            if &primary.proposed != source {
                return Err(corrupt_record());
            }
            let schema = self.freeze_capture_schema(attempt, source).await?;
            if serde_json::to_vec(&schema).map_err(|_| corrupt_record())?
                != serde_json::to_vec(&plan.target.schema).map_err(|_| corrupt_record())?
            {
                return Err(corrupt_record());
            }
            let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &source.configuration.table)?;
            let keys = [
                slot.clone(),
                self.identity_key(CONTROL_OPERATION_SUFFIX, &plan.operation.id)?,
                self.identity_key(HISTORY_CAPTURE_SUFFIX, &plan.operation.id)?,
            ];
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if rows[1..].iter().any(Option::is_some)
                || rows[0]
                    .as_ref()
                    .map(|row| self.decode_control_slot(row, source.configuration.table))
                    .transpose()?
                    .is_some_and(|owner| owner != ControlSlotOwner::Idle)
            {
                return Err(corrupt_record());
            }
            let record = AdditionRecord {
                content: plan.content()?,
                plan: plan.clone(),
                reservation: None,
                proposal: None,
                phase: AdditionPhase::Reserving,
            };
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let pointer = self.identity_key(ADDITION_TABLE_SUFFIX, &source.configuration.table)?;
            let pointers = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&pointer),
                    ReadProtection::Conflict,
                )
                .await?;
            let changes = [
                self.records.prepare(
                    pointer,
                    pointers[0].as_ref().map(|row| row.revision),
                    &plan.operation.id,
                )?,
                self.records.prepare(
                    self.identity_key(ADDITION_OPERATION_SUFFIX, &plan.operation.id)?,
                    None,
                    &record,
                )?,
                self.records.prepare(
                    self.identity_key(ADDITION_PENDING_SUFFIX, &plan.operation.id)?,
                    None,
                    &plan.operation.id,
                )?,
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
