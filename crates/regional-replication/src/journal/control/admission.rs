use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{installed::RegionalTablePhase, table::RegionalMode};
use storage_types::{BoundTableInfo, ReadProtection, StorageError, StorageResult};

use crate::{
    constants::CONTROL_SLOT_SUFFIX,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    /// Resolve and guard in the caller's schema attempt. There is no routing
    /// pre-read that can race enablement or a new table incarnation.
    pub(crate) async fn require_local_metadata<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(state) = self.load_capture_state(attempt, table).await? else {
                return Ok(());
            };
            let state = state.state();
            self.require_local_source(state)?;
            if !self
                .is_local_control_idle(attempt, state.configuration.table, ReadProtection::Shared)
                .await?
            {
                return Err(StorageError::validation(
                    "table configuration control is in progress",
                ));
            }
            if state.configuration.regional != RegionalMode::Local
                || state.phase != RegionalTablePhase::Active
            {
                return Err(StorageError::unsupported(
                    "regional table metadata requires its configuration control owner",
                ));
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn admits_expiration<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(state) = self.load_capture_state(attempt, table).await? else {
                return Ok(true);
            };
            let state = state.state();
            self.require_local_source(state)?;
            match (state.phase, &state.configuration.regional) {
                (RegionalTablePhase::Active, RegionalMode::Global { .. })
                | (RegionalTablePhase::Capturing, RegionalMode::Local) => Ok(true),
                (RegionalTablePhase::Active, RegionalMode::Local) => {
                    self.is_local_control_idle(
                        attempt,
                        state.configuration.table,
                        ReadProtection::Shared,
                    )
                    .await
                }
                _ => Ok(false),
            }
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn admits_gsi_build<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(state) = self.load_capture_state(attempt, table).await? else {
                return Ok(true);
            };
            let state = state.state();
            self.require_local_source(state)?;
            if state.phase != RegionalTablePhase::Active {
                return Ok(false);
            }
            self.admits_installed_gsi_control(attempt, state).await
        }
        .await;
        settlement.complete(result)
    }

    async fn admits_installed_gsi_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<bool> {
        let key = self.identity_key(CONTROL_SLOT_SUFFIX, &state.configuration.table)?;
        let row = self
            .records
            .load(attempt, &[key], ReadProtection::Shared)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        let owner = self.decode_control_slot(&row, state.configuration.table)?;
        let owner = match owner {
            ControlSlotOwner::Idle => return Ok(true),
            ControlSlotOwner::Trim(_) => return Ok(false),
            ControlSlotOwner::Configuration(owner) => owner,
        };
        let key = self.identity_key(crate::constants::CONTROL_OPERATION_SUFFIX, &owner)?;
        let row = self
            .records
            .load(attempt, &[key], ReadProtection::Shared)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        let control = self.decode_control_record(&row)?;
        Ok(control.proposal.operation().id == owner
            && control.decision == crate::control::ControlDecision::Installed
            && matches!(
                control.proposal.operation().change,
                replication_types::configuration::control::ConfigurationChange::TableMetadata { .. }
            )
            && &control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                == state)
    }

    pub(in crate::journal) async fn is_local_control_idle<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: replication_types::identity::resource::TableIdentity,
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
            let rows = self.records.load(attempt, &[key], protection).await?;
            let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
            Ok(self.decode_control_slot(row, table)? == ControlSlotOwner::Idle)
        }
        .await;
        settlement.complete(result)
    }
}
