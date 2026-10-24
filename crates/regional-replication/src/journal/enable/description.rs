use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{installed::RegionalTablePhase, table::RegionalMode};
use storage_types::{
    BoundTableInfo, MultiRegionConsistency, ReadProtection, ReplicaDescription, ReplicaStatus,
    StorageResult, TableDescription,
};

use crate::{
    constants::{CONTROL_SLOT_SUFFIX, ENABLE_OPERATION_SUFFIX},
    control::ControlDecision,
    enable::{EnablePhase, EnablePlan},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) fn project_enable_description(
        &self,
        description: &mut TableDescription,
        plan: &EnablePlan,
        phase: EnablePhase,
    ) -> StorageResult<()> {
        plan.validate()?;
        if matches!(phase, EnablePhase::Cancelled) {
            return Ok(());
        }
        description.aux_configuration_primary_region =
            Some(plan.operation.home_region.as_ref().to_owned());
        description.multi_region_consistency = Some(MultiRegionConsistency::Eventual);
        description.replicas = Some(
            plan.operation
                .participants
                .iter()
                .map(|region| ReplicaDescription {
                    region_name: region.as_ref().to_owned(),
                    replica_status: if region == &self.region || phase == EnablePhase::Completed {
                        ReplicaStatus::Active
                    } else if matches!(phase, EnablePhase::Cancelling(_)) {
                        ReplicaStatus::Deleting
                    } else {
                        ReplicaStatus::Creating
                    },
                    replica_status_description: None,
                    replica_inaccessible_date_time: None,
                })
                .collect(),
        );
        if phase != EnablePhase::Completed {
            description.table_status = storage_types::TableStatus::Updating;
        }
        Ok(())
    }

    pub(crate) async fn project_regional_description<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        description: &mut TableDescription,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(state) = self.observe_capture_state(attempt, table, ReadProtection::Snapshot).await? else { return Ok(()); };
            let state = state.state();
            self.require_local_source(state)?;
            description.aux_configuration_primary_region = match &state.configuration.regional { RegionalMode::Global { primary, .. } => Some(primary.as_ref().to_owned()), _ => None };
            if self.project_pending_addition_description(attempt, state, description).await? { return Ok(()); }
            if self.project_pending_control_description(attempt, state, description).await? { return Ok(()); }
            if let RegionalMode::Global { members, .. } = &state.configuration.regional {
                let control = self.load_configuration_control(attempt, state, ReadProtection::Snapshot).await?;
                let completed = control.decision == ControlDecision::Completed;
                let creating = !completed && matches!(control.proposal.operation().change, replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. } | replication_types::configuration::control::ConfigurationChange::AddRegion { .. });
                let idle = self.is_local_control_idle(attempt, state.configuration.table, ReadProtection::Snapshot).await?;
                description.multi_region_consistency = Some(MultiRegionConsistency::Eventual);
                description.replicas = Some(members.iter().map(|region| ReplicaDescription {
                    region_name: region.as_ref().to_owned(), replica_status: if state.phase == RegionalTablePhase::Sealing && region == &self.region { ReplicaStatus::Deleting } else if !creating || match &control.proposal.operation().change {
                        replication_types::configuration::control::ConfigurationChange::AddRegion { region: target } => region != target,
                        _ => region == &self.region,
                    } { ReplicaStatus::Active } else { ReplicaStatus::Creating },
                    replica_status_description: None, replica_inaccessible_date_time: None,
                }).collect());
                if !completed || !idle || state.phase != RegionalTablePhase::Active { description.table_status = storage_types::TableStatus::Updating; }
                return Ok(());
            }
            let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &state.configuration.table)?;
            let rows = self.records.load(attempt, &[slot], ReadProtection::Snapshot).await?;
            let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let crate::journal::control_slot::ControlSlotOwner::Configuration(operation) = self.decode_control_slot(row, state.configuration.table)? else { return Ok(()); };
            let key = self.identity_key(ENABLE_OPERATION_SUFFIX, &operation)?;
            let roots = self.records.load(attempt, &[key], ReadProtection::Snapshot).await?;
            let row = roots[0].as_ref().ok_or_else(corrupt_record)?;
            let root = self.decode_enable_record(row)?;
            if root.plan.operation.id != operation || root.plan.source.physical != table.history_id || root.plan.source.configuration.table != state.configuration.table || root.plan.source.configuration.tenant != state.configuration.tenant { return Err(corrupt_record()); }
            if matches!(root.phase, EnablePhase::Completed | EnablePhase::Cancelled) && state.phase == RegionalTablePhase::Active { return Ok(()); }
            self.project_enable_description(description, &root.plan, root.phase)

        }.await;
        settlement.complete(result)
    }
}
