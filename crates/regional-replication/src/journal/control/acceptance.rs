use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{
    installed::RegionalTablePhase, proposal::ControlProposal, table::RegionalMode,
};
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use crate::{
    constants::{CONTROL_PENDING_SUFFIX, CONTROL_TABLE_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn accept_pending_control<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<Option<storage_provider::TableUpdateChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            replication_provider::control::validate_pending_control(proposal)
                .map_err(|_| corrupt_record())?;
            if proposal.operation().home_region != self.region {
                return Err(StorageError::validation(
                    "regional configuration control requires the configuration primary",
                ));
            }
            let before = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            if before.phase != RegionalTablePhase::Active
                || !matches!(before.configuration.regional, RegionalMode::Global { .. })
            {
                return Err(corrupt_record());
            }
            if !self
                .is_local_control_idle(attempt, before.configuration.table, ReadProtection::Shared)
                .await?
            {
                return Err(StorageError::validation(
                    "table configuration control is in progress",
                ));
            }
            let current = self
                .load_configuration_control(attempt, before, ReadProtection::Shared)
                .await?;
            for participant in proposal.participants() {
                if participant.before.as_ref()
                    != Some(
                        &current
                            .proposal
                            .participant(&participant.region)
                            .map_err(|_| corrupt_record())?
                            .proposed,
                    )
                {
                    return Err(corrupt_record());
                }
            }
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let (_, metadata) = self.prepare_control(attempt, proposal, observed_at).await?;
            let pending = self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation().id)?;
            let pointer = self.identity_key(CONTROL_TABLE_SUFFIX, &before.configuration.table)?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&pointer),
                    ReadProtection::Conflict,
                )
                .await?;
            let changes = [
                self.records.prepare(
                    pending,
                    None,
                    &crate::control::pending::PendingControlReference::Configuration(
                        proposal.operation().id,
                    ),
                )?,
                self.records.prepare(
                    pointer,
                    rows[0].as_ref().map(|row| row.revision),
                    &proposal.operation().id,
                )?,
            ];
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            Ok(metadata)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn retire_pending_control<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let control = self
                .load_control(attempt, proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                self.decode_control(&control, proposal)?.decision,
                crate::control::ControlDecision::Completed
                    | crate::control::ControlDecision::Cancelled
            ) {
                return Err(corrupt_record());
            }
            let key = self.identity_key(CONTROL_PENDING_SUFFIX, &proposal.operation().id)?;
            let transfer = self.identity_key(
                crate::constants::CONTROL_REMOVAL_TRANSFER_SUFFIX,
                &proposal.operation().id,
            )?;
            let mut rows = self
                .records
                .load(attempt, &[key.clone(), transfer], ReadProtection::Conflict)
                .await?;
            let transfer =
                self.prepare_removal_transfer_retirement(rows.pop().flatten(), proposal)?;
            let mut changes = Vec::with_capacity(2);
            if let Some(row) = rows.pop().flatten() {
                if self.decode_pending_control_reference(&row)?
                    != crate::control::pending::PendingControlReference::Configuration(
                        proposal.operation().id,
                    )
                {
                    return Err(corrupt_record());
                }
                changes.push(infrastructure_provider::record::RecordChange::Delete {
                    key,
                    expected: row.revision,
                });
            }
            changes.extend(transfer);
            if changes.is_empty() {
                return Ok(());
            }
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }
}
