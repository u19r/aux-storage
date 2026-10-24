use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{installed::InstalledTableState, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::StorageResult;

use crate::{
    history_capture::HistoryCaptureRequest, journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn require_addition_before<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<(InstalledTableState, CanonicalDigest)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            replication_provider::control::validate_addition(proposal)
                .map_err(|_| corrupt_record())?;
            self.require_addition_control_phase(attempt, proposal)
                .await?;
            let participant = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            let Some(before) = &participant.before else {
                return self
                    .require_verified_control_target(attempt, proposal)
                    .await;
            };
            self.require_control_revision(attempt, proposal, storage_types::ReadProtection::Shared)
                .await?;
            let adoption = self
                .load_completed_history_definition(
                    attempt,
                    proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?
                .ok_or_else(corrupt_record)?;
            let request = HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: self.region.clone(),
            };
            let native = self
                .require_control_history_capture(attempt, &request, adoption.content())
                .await?;
            let collected = self
                .load_collected_history_definition(
                    attempt,
                    proposal,
                    &self.region,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            if native.content() != collected.content() {
                return Err(corrupt_record());
            }
            let current = self.freeze_capture_schema(attempt, before).await?;
            if serde_json::to_vec(&current).map_err(|_| corrupt_record())?
                != serde_json::to_vec(native.schema()).map_err(|_| corrupt_record())?
            {
                return Err(corrupt_record());
            }
            let initial = self
                .load_initial_baseline_definition(
                    attempt,
                    &request,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            if initial.content() != adoption.initial().content() {
                return Err(corrupt_record());
            }
            Ok((before.clone(), adoption.content()))
        }
        .await;
        settlement.complete(result)
    }
    async fn require_addition_control_phase<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        if proposal.operation().home_region != self.region {
            return Ok(());
        }
        let key = self.identity_key(
            crate::constants::ADDITION_OPERATION_SUFFIX,
            &proposal.operation().id,
        )?;
        let rows = self
            .records
            .load(attempt, &[key], storage_types::ReadProtection::Shared)
            .await?;
        let Some(row) = &rows[0] else {
            return Ok(());
        };
        let record: crate::addition::AdditionRecord = self.records.decode(row)?;
        if record.content != record.plan.content()?
            || record.proposal.as_ref() != Some(proposal)
            || record.phase != crate::addition::AdditionPhase::Controlling
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
