//! Immutable installed bootstrap definition survives mutable workflow
//! retirement.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_types::configuration::{control::ConfigurationChange, proposal::ControlProposal};
use storage_types::{ReadProtection, Revision, StorageResult};

use crate::{
    bootstrap::{SourceCaptureDefinition, TargetBootstrapDefinition},
    constants::BASELINE_LINEAGE_AUTHORITY_SUFFIX,
    control::{ControlDecision, ControlRecord},
    history_capture::HistoryCaptureRequest,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn retain_initial_baseline_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        control: &ControlRecord,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let proposal = &control.proposal;
            if !self.is_baseline_installation(proposal) {
                return Ok(());
            }
            let state = &proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed;
            let key = self.identity_key(
                BASELINE_LINEAGE_AUTHORITY_SUFFIX,
                &(state.configuration.table, state.configuration.lineage),
            )?;
            let rows = self
                .load_immutable_waves(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            if let Some(row) = &rows[0] {
                let old = self.decode_baseline_lineage_definition(row)?;
                if old.proposal() != proposal || control.bootstrap != Some(old.content()) {
                    return Err(corrupt_record());
                }
                return Ok(());
            }
            if matches!(
                control.decision,
                ControlDecision::Installed | ControlDecision::Completed
            ) {
                return Err(corrupt_record());
            }
            let definition = self
                .select_baseline_lineage_authority(attempt, control)
                .await?
                .ok_or_else(corrupt_record)?;
            definition.validate()?;
            if definition.proposal() != proposal || control.bootstrap != Some(definition.content())
            {
                return Err(corrupt_record());
            }
            self.records
                .store(attempt, &[self.records.prepare(key, None, &definition)?])
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn decode_baseline_lineage_definition(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<TargetBootstrapDefinition> {
        let definition: TargetBootstrapDefinition = self.records.decode(row)?;
        definition.validate()?;
        let proposal = definition.proposal();
        let state = &proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed;
        self.require_local_source(state)?;
        if !self.is_baseline_installation(proposal)
            || row.revision != Revision::INITIAL
            || row.key
                != self.identity_key(
                    BASELINE_LINEAGE_AUTHORITY_SUFFIX,
                    &(state.configuration.table, state.configuration.lineage),
                )?
            || definition.initial().proposal().operation().lineage != state.configuration.lineage
        {
            return Err(corrupt_record());
        }
        Ok(definition)
    }

    pub(in crate::journal) async fn load_initial_baseline_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<SourceCaptureDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if request.region == request.proposal.operation().home_region {
                let (_, record) = self
                    .load_history_capture(attempt, request, protection)
                    .await?
                    .ok_or_else(corrupt_record)?;
                return Ok(record
                    .retained
                    .ok_or_else(corrupt_record)?
                    .initial()
                    .clone());
            }
            let state = replication_provider::control::borrow_addition_source(
                &request.proposal,
                &request.region,
            )
            .map_err(|_| corrupt_record())?;
            self.load_lineage_baseline_definition(attempt, state, protection)
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn load_lineage_baseline_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &replication_types::configuration::installed::InstalledTableState,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<SourceCaptureDefinition> {
        Ok(self
            .load_lineage_bootstrap_definition(attempt, state, protection)
            .await?
            .initial()
            .clone())
    }

    pub(in crate::journal) async fn load_lineage_bootstrap_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &replication_types::configuration::installed::InstalledTableState,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<TargetBootstrapDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.identity_key(
                BASELINE_LINEAGE_AUTHORITY_SUFFIX,
                &(state.configuration.table, state.configuration.lineage),
            )?;
            let row = self
                .load_immutable_waves(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let definition = self.decode_baseline_lineage_definition(&row)?;
            let local = &definition
                .proposal()
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed;
            if local.configuration.table != state.configuration.table
                || local.physical != state.physical
                || local.configuration.lineage != state.configuration.lineage
            {
                return Err(corrupt_record());
            }
            Ok(definition)
        }
        .await;
        settlement_guard.complete(result)
    }

    fn is_baseline_installation(&self, proposal: &ControlProposal) -> bool {
        matches!(
            proposal.operation().change,
            ConfigurationChange::EnableGlobal { .. }
        ) || matches!(&proposal.operation().change, ConfigurationChange::AddRegion { region } if region == &self.region)
    }

    async fn select_baseline_lineage_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        control: &ControlRecord,
    ) -> StorageResult<Option<TargetBootstrapDefinition>> {
        match &control.proposal.operation().change {
            ConfigurationChange::EnableGlobal { .. } => self
                .load_initial_capture_authority(attempt, &control.proposal)
                .await
                .map(Some),
            ConfigurationChange::AddRegion { region } if region == &self.region => self
                .load_retained_bootstrap_definition(
                    attempt,
                    &control.proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await
                .map(Some),
            _ => Ok(None),
        }
    }

    async fn load_initial_capture_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<TargetBootstrapDefinition> {
        if proposal.operation().home_region == self.region {
            self.load_enable_bootstrap_definition(
                attempt,
                proposal,
                storage_types::ReadProtection::Shared,
            )
            .await
        } else {
            self.load_retained_bootstrap_definition(
                attempt,
                proposal,
                storage_types::ReadProtection::Shared,
            )
            .await
        }
    }
}
