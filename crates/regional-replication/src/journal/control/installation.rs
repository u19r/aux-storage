use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    control::{ControlDecision, ControlRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn install_control<
        A: PhysicalRecordAttempt
            + storage_provider::UpdateTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        certificate: &crate::control::CommittedControl,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = &certificate.proposal;
            let row = self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let mut record = self.decode_control(&row, proposal)?;
            if record.bootstrap != certificate.bootstrap {
                return Err(corrupt_record());
            }
            if matches!(
                record.decision,
                ControlDecision::Installed | ControlDecision::Completed
            ) {
                if record.retirement != certificate.retirement {
                    return Err(corrupt_record());
                }
                self.retain_initial_baseline_authority(attempt, &record)
                    .await?;
                self.retain_retired_source_catalog(attempt, &record).await?;
                self.retain_installed_history_catalog(attempt, &record)
                    .await?;
                return Ok(record);
            }
            if record.decision == ControlDecision::Cancelled
                || (proposal.operation().home_region == self.region
                    && record.decision != ControlDecision::Committed)
            {
                return Err(corrupt_record());
            }
            self.require_retirement_evidence(attempt, proposal, &certificate.retirement)
                .await?;
            let (expected, bootstrap) = self.require_control_before(attempt, proposal).await?;
            if bootstrap != record.bootstrap {
                return Err(corrupt_record());
            }
            let target = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            let observed = self
                .observe_table_state(
                    attempt,
                    target.proposed.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?;
            if observed.as_ref().map(|state| state.state()) != expected.as_ref() {
                return Err(corrupt_record());
            }
            record.retirement = certificate.retirement.clone();
            self.install_ttl_policy(attempt, proposal).await?;
            self.install_table_metadata(attempt, proposal, observed_at)
                .await?;
            self.retain_initial_baseline_authority(attempt, &record)
                .await?;
            self.retain_retired_source_catalog(attempt, &record).await?;
            self.retain_installed_history_catalog(attempt, &record)
                .await?;
            record.decision = ControlDecision::Installed;
            self.store_control_transition(attempt, (row.revision, &record))
                .await?;
            self.introduce_public_trim_sources(attempt, &record).await?;
            self.persist_table_state(attempt, (observed, target.proposed.clone()))
                .await?;
            self.store_configuration_operation(attempt, proposal)
                .await?;
            self.finish_table_seal_installation(attempt, proposal)
                .await?;
            Ok(record)
        }
        .await;
        settlement.complete(result)
    }
}
