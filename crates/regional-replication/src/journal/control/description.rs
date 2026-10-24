use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{installed::InstalledTableState, proposal::ControlProposal},
    identity::resource::ControlOperationId,
};
use storage_types::{
    IndexStatus, MultiRegionConsistency, ReadProtection, ReplicaDescription, ReplicaStatus,
    StorageResult, TableDescription, TableStatus,
};

use crate::{
    constants::{CONTROL_OPERATION_SUFFIX, CONTROL_PENDING_SUFFIX, CONTROL_TABLE_SUFFIX},
    control::{ControlDecision, ControlRecord},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) fn project_control_description(
        &self,
        description: &mut TableDescription,
        proposal: &ControlProposal,
        decision: ControlDecision,
    ) -> StorageResult<()> {
        replication_provider::control::validate_pending_control(proposal)
            .map_err(|_| corrupt_record())?;
        let retiring = if matches!(
            proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::RemoveRegion { .. }
                | replication_types::configuration::control::ConfigurationChange::DisableGlobal
        ) {
            replication_provider::control::retiring_regions(proposal)
                .map_err(|_| corrupt_record())?
        } else {
            Vec::new()
        };
        description.aux_configuration_primary_region =
            Some(proposal.operation().home_region.as_ref().to_owned());
        description.table_status = TableStatus::Updating;
        description.multi_region_consistency = Some(MultiRegionConsistency::Eventual);
        description.replicas = Some(
            proposal
                .operation()
                .participants
                .iter()
                .map(|region| ReplicaDescription {
                    region_name: region.as_ref().to_owned(),
                    replica_status: if retiring.contains(region) {
                        ReplicaStatus::Deleting
                    } else {
                        ReplicaStatus::Active
                    },
                    replica_status_description: None,
                    replica_inaccessible_date_time: None,
                })
                .collect(),
        );
        if let replication_types::configuration::control::ConfigurationChange::TableMetadata {
            definition,
            ..
        } = &proposal.operation().change
        {
            for index in description.global_secondary_indexes.iter_mut().flatten() {
                if !matches!(
                    decision,
                    ControlDecision::Completed | ControlDecision::Cancelled
                ) && definition
                    .created_indexes()
                    .any(|name| name == &index.index_name)
                {
                    index.index_status = Some(IndexStatus::Creating);
                    index.backfilling = Some(true);
                }
            }
        }
        Ok(())
    }

    pub(in crate::journal) async fn project_pending_control_description<
        A: PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        description: &mut TableDescription,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(control) = self.load_described_control(attempt, state).await? else {
                return Ok(false);
            };
            let participant = control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            let mut current = state.clone();
            if current.phase
                == replication_types::configuration::installed::RegionalTablePhase::Sealing
            {
                current.phase =
                    replication_types::configuration::installed::RegionalTablePhase::Active;
            }
            if participant.before.as_ref() != Some(&current) && participant.proposed != current {
                return Err(corrupt_record());
            }
            self.project_control_description(description, &control.proposal, control.decision)?;
            description.aux_configuration_primary_region = match &state.configuration.regional {
                replication_types::configuration::table::RegionalMode::Global {
                    primary, ..
                } => Some(primary.as_ref().to_owned()),
                _ => None,
            };
            if control.decision == ControlDecision::Cancelled {
                for replica in description.replicas.iter_mut().flatten() {
                    replica.replica_status = ReplicaStatus::Active;
                }
            }
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) async fn load_described_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
    ) -> StorageResult<Option<ControlRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = [self.identity_key(crate::constants::CONTROL_SLOT_SUFFIX, &state.configuration.table)?, self.identity_key(CONTROL_TABLE_SUFFIX, &state.configuration.table)?];
            let rows = self.records.load(attempt, &keys, ReadProtection::Snapshot).await?;
            let slot = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let held = match self.decode_control_slot(slot, state.configuration.table)? {
                ControlSlotOwner::Configuration(operation) => Some(operation), ControlSlotOwner::Idle => None,
                ControlSlotOwner::Trim(_) => return Ok(None),
            };
            let pointed = rows[1].as_ref().map(|row| self.records.decode::<ControlOperationId>(row)).transpose()?;
            let Some(operation) = held.or(pointed) else { return Ok(None); };
            if held.is_none() {
                let key = self.identity_key(CONTROL_PENDING_SUFFIX, &operation)?;
                let row = self.records.load(attempt, &[key], ReadProtection::Snapshot).await?.pop().flatten();
                let Some(row) = row else { return Ok(None); };
                if self.decode_pending_control_reference(&row)? != crate::control::pending::PendingControlReference::Configuration(operation) { return Err(corrupt_record()); }
            }
            let key = self.identity_key(CONTROL_OPERATION_SUFFIX, &operation)?;
            let row = self.records.load(attempt, &[key], ReadProtection::Snapshot).await?.pop().flatten();
            let Some(row) = row else { return if held.is_some() { Ok(None) } else { Err(corrupt_record()) }; };
            let control = self.decode_control_record(&row)?;
            if control.proposal.operation().id != operation { return Err(corrupt_record()); }
            if matches!(control.proposal.operation().change, replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. } | replication_types::configuration::control::ConfigurationChange::AddRegion { .. } | replication_types::configuration::control::ConfigurationChange::CreateTable { .. }) { return Ok(None); }
            Ok(Some(control))

        }.await;
        settlement.complete(result)
    }
}
