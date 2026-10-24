use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::identity::resource::ControlOperationId;
use storage_types::{ReadProtection, StorageResult};

use crate::{control::ControlRecord, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn require_enable_source_history<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        source: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if u64::from(source.configuration.config) == 0 {
                return Ok(());
            }
            let previous = self
                .load_configuration_control(attempt, source, ReadProtection::Shared)
                .await?;
            if previous.decision != crate::control::ControlDecision::Completed
                || !matches!(
                    previous.proposal.operation().change,
                    replication_types::configuration::control::ConfigurationChange::DisableGlobal
                )
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_control_predecessor<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &ControlRecord,
        protection: ReadProtection,
    ) -> StorageResult<Option<ControlOperationId>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let operation = record.proposal.operation();
            let Some(before) = &record
                .proposal
                .participant(&operation.home_region)
                .map_err(|_| corrupt_record())?
                .before
            else {
                return Ok(None);
            };
            if matches!(
                operation.change,
                replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. }
            ) && u64::from(before.configuration.config) == 0
            {
                return Ok(None);
            }
            let previous = self
                .load_configuration_control(attempt, before, protection)
                .await?;
            if before.configuration.lineage != operation.lineage
                && !matches!(
                    previous.proposal.operation().change,
                    replication_types::configuration::control::ConfigurationChange::DisableGlobal
                )
            {
                return Err(corrupt_record());
            }
            let predecessor = previous.proposal.operation().id;
            if predecessor == operation.id {
                return Err(corrupt_record());
            }
            Ok(Some(predecessor))
        }
        .await;
        settlement.complete(result)
    }
}
