use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_types::StorageResult;

use super::record::TargetRecord;
use crate::{
    bootstrap::{SourceCaptureDefinition, TargetBootstrapDefinition},
    constants::TARGET_BOOTSTRAP_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn load_retained_bootstrap_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<TargetBootstrapDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &proposal.operation().id)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let retained: TargetRecord = self.records.decode(&row)?;
            let (_, root) = self
                .load_hidden_target(attempt, &retained.request, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            let definition = self.qualify_target_bootstrap(&root, proposal)?;
            if root.cancelled || !root.baselines_complete {
                return Err(corrupt_record());
            }
            self.require_verification_evidence(
                &root,
                root.verification.as_ref().ok_or_else(corrupt_record)?,
            )?;
            Ok(definition)
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) async fn load_target_retained_catalog_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<crate::bootstrap::ReenableHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let definition = self
                .load_retained_bootstrap_definition(attempt, proposal, protection)
                .await?;
            if definition.retained_inventory()?.is_none() {
                return Err(corrupt_record());
            }
            let key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &proposal.operation().id)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let root: TargetRecord = self.records.decode(&row)?;
            let page = self
                .load_target_retained_page(attempt, &root, ordinal, protection)
                .await?;
            if !self
                .require_target_retained_baselines(attempt, &root, &page, protection)
                .await?
            {
                return Err(corrupt_record());
            }
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }
    pub(super) fn qualify_target_bootstrap(
        &self,
        root: &TargetRecord,
        proposal: &ControlProposal,
    ) -> StorageResult<TargetBootstrapDefinition> {
        if let Some(retained) = &root.reenable {
            if retained.definition.initial().proposal() != proposal {
                return Err(corrupt_record());
            }
            let definition =
                TargetBootstrapDefinition::Reenable(Box::new(retained.definition.clone()));
            self.require_target_bootstrap(root, &definition)?;
            return Ok(definition);
        }
        let definition = match &root.adoption {
            Some(adoption) => {
                if adoption.definition.proposal() != proposal {
                    return Err(corrupt_record());
                }
                TargetBootstrapDefinition::Addition(Box::new(adoption.definition.clone()))
            }
            None => SourceCaptureDefinition::from_ready(
                proposal.clone(),
                root.request.schema.clone(),
                root.source_vector.ok_or_else(corrupt_record)?,
            )?
            .into(),
        };
        self.require_target_bootstrap(root, &definition)?;
        Ok(definition)
    }
}
