use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{configuration::proposal::ControlProposal, identity::replica::RegionId};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{
        COLLECTED_HISTORY_DEFINITION_SUFFIX, COLLECTED_HISTORY_PAGE_SUFFIX,
        HISTORY_COLLECTION_SUFFIX,
    },
    history_capture::{HistoryCaptureDefinition, HistoryCaptureVectorPage},
    history_collection::{HistoryCollectionObservation, HistoryCollectionRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn observe_history_collection<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: ReadProtection,
    ) -> StorageResult<Option<HistoryCollectionObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            replication_provider::control::validate_addition(proposal)
                .map_err(|_| corrupt_record())?;
            replication_provider::control::borrow_addition_source(proposal, &self.region)
                .map_err(|_| corrupt_record())?;
            let key = self.identity_key(HISTORY_COLLECTION_SUFFIX, &proposal.operation().id)?;
            let Some(row) = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
            else {
                return Ok(None);
            };
            let record: HistoryCollectionRecord = self.records.decode(&row)?;
            record.validate()?;
            if row.key != key || record.proposal != *proposal {
                return Err(corrupt_record());
            }
            self.require_declared_collection(attempt, &record, protection)
                .await?;
            Ok(Some(HistoryCollectionObservation {
                revision: row.revision,
                record,
            }))
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_collected_history_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        region: &RegionId,
        protection: ReadProtection,
    ) -> StorageResult<HistoryCaptureDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let root = self
                .require_complete_history_source(attempt, proposal, region, protection)
                .await?;
            let request = crate::history_capture::HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: region.clone(),
            };
            self.load_history_definition_fact(attempt, &request, root.policy, protection)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_completed_history_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: ReadProtection,
    ) -> StorageResult<Option<crate::history_collection::HistoryAdoptionDefinition>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(root) = self
                .observe_history_collection(attempt, proposal, protection)
                .await?
            else {
                return Ok(None);
            };
            if !root.record.is_complete() {
                return Ok(None);
            }
            let request = crate::history_capture::HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: proposal.operation().home_region.clone(),
            };
            let current = self
                .load_history_definition_fact(attempt, &request, root.record.policy, protection)
                .await?;
            Ok(Some(
                crate::history_collection::HistoryAdoptionDefinition::from_native(
                    &root.record,
                    current,
                )?,
            ))
        }
        .await;
        settlement.complete(result)
    }

    async fn load_history_definition_fact<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &crate::history_capture::HistoryCaptureRequest,
        policy: replication_types::protocol::CanonicalDigest,
        protection: ReadProtection,
    ) -> StorageResult<HistoryCaptureDefinition> {
        let key = self.identity_key(
            COLLECTED_HISTORY_DEFINITION_SUFFIX,
            &(request.proposal.operation().id, &request.region),
        )?;
        let row = self
            .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        let definition = self.decode_collected_history_definition(&row)?;
        if definition.request().content()? != request.content()?
            || definition.policy_digest()? != policy
        {
            return Err(corrupt_record());
        }
        Ok(definition)
    }

    pub(crate) async fn load_collected_history_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        scope: (&RegionId, u32),
        protection: ReadProtection,
    ) -> StorageResult<HistoryCaptureVectorPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let definition = self
                .load_collected_history_definition(attempt, proposal, scope.0, protection)
                .await?;
            let key = self.identity_key(
                COLLECTED_HISTORY_PAGE_SUFFIX,
                &(proposal.operation().id, scope.0, scope.1),
            )?;
            let row = self
                .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let page = self
                .decode_collected_history_page(&row)?
                .qualify(&definition)?;
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }
}
