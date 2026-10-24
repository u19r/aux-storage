use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{control::ConfigurationChange, proposal::ControlProposal};
use storage_provider::CatalogAttempt;
use storage_types::StorageResult;

use crate::{
    bootstrap::TargetBootstrapDefinition, history_capture::HistoryCaptureRequest,
    journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn load_source_bootstrap_definition<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<TargetBootstrapDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            match proposal.operation().change {
                ConfigurationChange::EnableGlobal { .. } => {
                    self.load_enable_bootstrap_definition(attempt, proposal, protection)
                        .await
                }
                ConfigurationChange::AddRegion { .. } => {
                    self.require_control_revision(attempt, proposal, protection)
                        .await?;
                    let request = HistoryCaptureRequest {
                        proposal: proposal.clone(),
                        region: proposal.operation().home_region.clone(),
                    };
                    let current = self
                        .require_ready_history_capture(attempt, &request, protection)
                        .await?;
                    let definition = self
                        .load_completed_history_definition(attempt, proposal, protection)
                        .await?
                        .ok_or_else(corrupt_record)?;
                    if definition.current().content() != current.content() {
                        return Err(corrupt_record());
                    }
                    let initial = self
                        .load_retained_baseline_definition(attempt, &request, protection)
                        .await?;
                    if initial.initial().content() != definition.initial().content() {
                        return Err(corrupt_record());
                    }
                    Ok(definition.into())
                }
                _ => Err(corrupt_record()),
            }
        }
        .await;
        settlement_guard.complete(result)
    }
}

impl JournalStore {
    pub(in crate::journal) async fn load_enable_bootstrap_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<TargetBootstrapDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let before = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            let definition = if u64::from(before.configuration.config) == 0 {
                self.load_source_capture_definition(attempt, proposal, protection)
                    .await?
                    .into()
            } else {
                TargetBootstrapDefinition::Reenable(Box::new(
                    self.load_reenable_history_definition(attempt, proposal, protection)
                        .await?,
                ))
            };
            definition.validate()?;
            Ok(definition)
        }
        .await;
        settlement_guard.complete(result)
    }
}
