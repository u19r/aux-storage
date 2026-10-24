use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{
    control::ConfigurationChange, installed::InstalledTableState, proposal::ControlProposal,
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::SealPhase;
use crate::{journal::JournalStore, lifecycle::SealRequest, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn require_removal_before<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<InstalledTableState> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            replication_provider::control::validate_removal(proposal)
                .map_err(|_| corrupt_record())?;
            if !replication_provider::control::retiring_regions(proposal)
                .map_err(|_| corrupt_record())?
                .contains(&self.region)
            {
                self.require_control_revision(
                    attempt,
                    proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
                return proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .before
                    .clone()
                    .ok_or_else(corrupt_record);
            }
            let request = SealRequest::try_new(proposal.clone(), self.region.clone())?;
            let (_, record) = self
                .load_table_seal(attempt, &request, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(record.phase, SealPhase::Ready) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSource,
                }
                .into());
            }
            self.require_sealing_table(attempt, &request).await?;
            let mut before = request.before()?.clone();
            before.phase = replication_types::configuration::installed::RegionalTablePhase::Sealing;
            Ok(before)
        }
        .await;
        settlement.complete(result)
    }

    /// Native installation owns seal removal, state and its installation
    /// receipt in one atomic attempt, regardless of their staging order.
    pub(in crate::journal) async fn finish_table_seal_installation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
            ) {
                return Ok(());
            }
            if !replication_provider::control::retiring_regions(proposal)
                .map_err(|_| corrupt_record())?
                .contains(&self.region)
            {
                return Ok(());
            }
            let request = SealRequest::try_new(proposal.clone(), self.region.clone())?;
            let (row, mut record) = self
                .load_table_seal(attempt, &request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(record.phase, SealPhase::Ready) {
                return Err(corrupt_record());
            }
            record.phase = SealPhase::Removed;
            self.store_table_seal(attempt, &record, Some(row.revision))
                .await
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) async fn require_table_seal_vector<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
        vector: replication_types::protocol::CanonicalDigest,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (_, record) = self
                .load_table_seal(attempt, request, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                record.phase,
                SealPhase::Ready | SealPhase::Removed | SealPhase::Retired
            ) || record.vector != vector
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
