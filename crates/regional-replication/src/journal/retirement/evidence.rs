use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{control::ConfigurationChange, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, StorageResult};

use super::record::ReceivedSeal;
use crate::{
    constants::RECEIVED_SEAL_SUFFIX, control::RetiredSealEvidence, journal::JournalStore,
    lifecycle::SealRequest, native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn load_received_seal_vector<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<CanonicalDigest> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_seal_survivor(request)?;
            let key = self.identity_key(
                RECEIVED_SEAL_SUFFIX,
                &(request.proposal.operation().id, &request.region),
            )?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let Some(row) = &rows[0] else {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSource,
                }
                .into());
            };
            let root: ReceivedSeal = self.records.decode(row)?;
            if root.request.content()? != request.content()?
                || !root
                    .progress(u32::from(request.source()?.partitions))
                    .complete
            {
                return Err(corrupt_record());
            }
            Ok(root.vector)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_retirement_evidence<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<Vec<RetiredSealEvidence>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
            ) {
                return Ok(Vec::new());
            }
            let mut evidence = Vec::new();
            for region in replication_provider::control::retiring_regions(proposal)
                .map_err(|_| corrupt_record())?
            {
                let request = SealRequest::try_new(proposal.clone(), region.clone())?;
                evidence.push(RetiredSealEvidence {
                    region,
                    vector: self.load_received_seal_vector(attempt, &request).await?,
                });
            }
            Ok(evidence)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_retirement_evidence<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        evidence: &[RetiredSealEvidence],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
            ) {
                return if evidence.is_empty() {
                    Ok(())
                } else {
                    Err(corrupt_record())
                };
            }
            let retiring = replication_provider::control::retiring_regions(proposal)
                .map_err(|_| corrupt_record())?;
            if evidence.len() != retiring.len() {
                return Err(corrupt_record());
            }
            for (binding, region) in evidence.iter().zip(retiring) {
                if binding.region != region {
                    return Err(corrupt_record());
                }
                let request = SealRequest::try_new(proposal.clone(), region)?;
                if request.survivors()?.contains(&self.region)
                    && self.load_received_seal_vector(attempt, &request).await? != binding.vector
                {
                    return Err(corrupt_record());
                }
                if request.region == self.region {
                    self.require_table_seal_vector(attempt, &request, binding.vector)
                        .await?;
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
