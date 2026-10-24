use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{
    MultiRegionConsistency, ReplicaDescription, ReplicaStatus, StorageResult, TableDescription,
};

use crate::{
    addition::{AddRegionPlan, AdditionPhase},
    journal::JournalStore,
};

impl JournalStore {
    pub(in crate::journal) async fn project_pending_addition_description<
        A: PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &replication_types::configuration::installed::InstalledTableState,
        description: &mut TableDescription,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let pointer = self.identity_key(
                crate::constants::ADDITION_TABLE_SUFFIX,
                &state.configuration.table,
            )?;
            let rows = self
                .records
                .load(attempt, &[pointer], storage_types::ReadProtection::Snapshot)
                .await?;
            let Some(row) = &rows[0] else {
                return Ok(false);
            };
            let operation: replication_types::identity::resource::ControlOperationId =
                self.records.decode(row)?;
            let key = self.identity_key(crate::constants::ADDITION_OPERATION_SUFFIX, &operation)?;
            let roots = self
                .records
                .load(attempt, &[key], storage_types::ReadProtection::Snapshot)
                .await?;
            let record: crate::addition::AdditionRecord = self.records.decode(
                roots[0]
                    .as_ref()
                    .ok_or_else(crate::native_record::corrupt_record)?,
            )?;
            self.load_addition(
                attempt,
                &record.plan,
                storage_types::ReadProtection::Snapshot,
            )
            .await?;
            if record.plan.operation.id != operation {
                return Err(crate::native_record::corrupt_record());
            }
            if matches!(
                record.phase,
                AdditionPhase::Completed | AdditionPhase::Cancelled
            ) {
                return Ok(false);
            }
            let proposed = record
                .proposal
                .as_ref()
                .and_then(|proposal| proposal.participant(&self.region).ok())
                .map(|participant| &participant.proposed);
            if &record.plan.target.source != state && proposed != Some(state) {
                return Err(crate::native_record::corrupt_record());
            }
            self.project_addition_description(description, &record.plan, record.phase)?;
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) fn project_addition_description(
        &self,
        description: &mut TableDescription,
        plan: &AddRegionPlan,
        phase: AdditionPhase,
    ) -> StorageResult<()> {
        plan.validate()?;
        if matches!(phase, AdditionPhase::Cancelled) {
            return Ok(());
        }
        let destination = &plan.destination()?.region;
        description.aux_configuration_primary_region =
            Some(plan.operation.home_region.as_ref().to_owned());
        description.multi_region_consistency = Some(MultiRegionConsistency::Eventual);
        description.replicas = Some(
            plan.operation
                .participants
                .iter()
                .map(|region| ReplicaDescription {
                    region_name: region.as_ref().to_owned(),
                    replica_status: if region != destination || phase == AdditionPhase::Completed {
                        ReplicaStatus::Active
                    } else {
                        ReplicaStatus::Creating
                    },
                    replica_status_description: None,
                    replica_inaccessible_date_time: None,
                })
                .collect(),
        );
        if phase != AdditionPhase::Completed {
            description.table_status = storage_types::TableStatus::Updating;
        }
        Ok(())
    }
}
