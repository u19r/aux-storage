use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::HISTORY_COLLECTION_SUFFIX, history_capture::HistoryCaptureRequest,
    history_collection::HistoryCollectionRecord, journal::JournalStore,
};

impl JournalStore {
    pub(crate) async fn begin_history_collection<
        A: PhysicalRecordAttempt + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self.region != proposal.operation().home_region {
                return Err(crate::native_record::corrupt_record());
            }
            if self
                .observe_history_collection(attempt, proposal, ReadProtection::Conflict)
                .await?
                .is_some()
            {
                return Ok(());
            }
            self.require_control_revision(attempt, proposal, storage_types::ReadProtection::Shared)
                .await?;
            let request = HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: self.region.clone(),
            };
            let definition = self
                .require_ready_history_capture(
                    attempt,
                    &request,
                    storage_types::ReadProtection::Conflict,
                )
                .await?;
            let record = HistoryCollectionRecord::from_primary_definition(&definition)?;
            let key = self.identity_key(HISTORY_COLLECTION_SUFFIX, &proposal.operation().id)?;
            self.records
                .store(attempt, &[self.records.prepare(key, None, &record)?])
                .await
        }
        .await;
        settlement.complete(result)
    }
}

impl JournalStore {
    pub(crate) fn collection_catalog_scope(
        &self,
        after: Option<infrastructure_provider::record::DurableRecordKey>,
    ) -> crate::history_capture::HistoryCatalogScope {
        crate::history_capture::HistoryCatalogScope {
            after,
            max_records: std::num::NonZeroUsize::MIN,
            max_bytes: self.records.limits.max_record_bytes(),
        }
    }
}
