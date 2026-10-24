use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{
    control::ConfigurationChange, installed::InstalledTableState, proposal::ControlProposal,
};
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::CONTROL_SLOT_SUFFIX,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    /// One expected-state owner serves both preparation and installation.
    /// Bootstrap intermediates require native proof, not a caller's status.
    pub(super) async fn require_control_before<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<(
        Option<InstalledTableState>,
        Option<replication_types::protocol::CanonicalDigest>,
    )> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if matches!(
                proposal.operation().change,
                ConfigurationChange::AddRegion { .. }
            ) {
                return self
                    .require_addition_before(attempt, proposal)
                    .await
                    .map(|(before, bootstrap)| (Some(before), Some(bootstrap)));
            }
            if matches!(
                proposal.operation().change,
                ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
            ) {
                return self
                    .require_removal_before(attempt, proposal)
                    .await
                    .map(|before| (Some(before), None));
            }
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::EnableGlobal { .. }
            ) {
                self.require_control_revision(
                    attempt,
                    proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
                return Ok((
                    proposal
                        .participant(&self.region)
                        .map_err(|_| corrupt_record())?
                        .before
                        .clone(),
                    None,
                ));
            }
            let target = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            self.require_local_source(&target.proposed)?;
            let key =
                self.identity_key(CONTROL_SLOT_SUFFIX, &target.proposed.configuration.table)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let owner = self.decode_control_slot(
                rows[0].as_ref().ok_or_else(corrupt_record)?,
                target.proposed.configuration.table,
            )?;
            if owner != ControlSlotOwner::Configuration(proposal.operation().id) {
                return Err(corrupt_record());
            }
            let expected = if proposal.operation().home_region == self.region {
                let state = self
                    .require_capture_table(attempt, proposal, storage_types::ReadProtection::Shared)
                    .await?;
                let definition = self
                    .load_enable_bootstrap_definition(
                        attempt,
                        proposal,
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                let current = self.freeze_capture_schema(attempt, &state).await?;
                if serde_json::to_vec(&current).map_err(|_| corrupt_record())?
                    != serde_json::to_vec(definition.schema()).map_err(|_| corrupt_record())?
                {
                    return Err(corrupt_record());
                }
                (state, definition.content())
            } else {
                self.require_verified_control_target(attempt, proposal)
                    .await?
            };
            Ok((Some(expected.0), Some(expected.1)))
        }
        .await;
        settlement.complete(result)
    }
}
