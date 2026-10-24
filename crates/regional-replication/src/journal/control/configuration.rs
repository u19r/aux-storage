use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalTablePhase},
        proposal::ControlProposal,
    },
    identity::resource::ControlOperationId,
};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::CONFIGURATION_OPERATION_SUFFIX,
    control::{ControlDecision, ControlRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn store_configuration_operation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let table = &proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                .configuration;
            let key =
                self.identity_key(CONFIGURATION_OPERATION_SUFFIX, &(table.table, table.config))?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            if let Some(row) = &rows[0] {
                if self.records.decode::<ControlOperationId>(row)? != proposal.operation().id {
                    return Err(corrupt_record());
                }
                return Ok(());
            }
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(key, None, &proposal.operation().id)?],
                )
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_configuration_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        protection: ReadProtection,
    ) -> StorageResult<ControlRecord> {
        self.load_configuration_controls(attempt, &[state], protection)
            .await?
            .pop()
            .ok_or_else(corrupt_record)
    }

    /// Pointer observations precede dependent controls; each inventory stays
    /// ordered across NativeRecords waves inside the caller's one protected
    /// attempt.
    pub(crate) async fn load_configuration_controls<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        states: &[&InstalledTableState],
        protection: ReadProtection,
    ) -> StorageResult<Vec<ControlRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let keys = states
                .iter()
                .map(|state| {
                    self.require_local_source(state)?;
                    self.identity_key(
                        CONFIGURATION_OPERATION_SUFFIX,
                        &(state.configuration.table, state.configuration.config),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(settlement.as_mut(), &keys, protection)
                .await?;
            let operations = rows
                .iter()
                .map(|row| {
                    self.records
                        .decode::<ControlOperationId>(row.as_ref().ok_or_else(corrupt_record)?)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            drop(rows);
            drop(keys);
            let keys = operations
                .iter()
                .map(|operation| {
                    self.identity_key(crate::constants::CONTROL_OPERATION_SUFFIX, operation)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(settlement.as_mut(), &keys, protection)
                .await?;
            states
                .iter()
                .zip(operations)
                .zip(rows)
                .map(|((state, operation), row)| {
                    let control =
                        self.decode_control_record(row.as_ref().ok_or_else(corrupt_record)?)?;
                    self.require_configuration_control(state, operation, &control)?;
                    Ok(control)
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }

    fn require_configuration_control(
        &self,
        state: &InstalledTableState,
        operation: ControlOperationId,
        control: &ControlRecord,
    ) -> StorageResult<()> {
        let mut expected = state.clone();
        if expected.phase == RegionalTablePhase::Sealing {
            expected.phase = RegionalTablePhase::Active;
        }
        if control.proposal.operation().id != operation
            || !matches!(
                control.decision,
                ControlDecision::Installed | ControlDecision::Completed
            )
            || control
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                != expected
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
