use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::{
    configuration::proposal::ControlProposal, identity::replica::RegionId,
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{COLLECTED_CATALOG_SUFFIX, COLLECTED_RETIRED_PAGE_SUFFIX},
    history_capture::{HistoryCaptureRequest, RetiredHistoryCatalogPage, RetiredHistoryPage},
    history_collection::HistoryCollectionRecord,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn require_complete_history_source<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        region: &RegionId,
        protection: ReadProtection,
    ) -> StorageResult<HistoryCollectionRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let root = self
                .observe_history_collection(attempt, proposal, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if !root.record.is_complete() || !root.record.sources()?.contains(region) {
                return Err(corrupt_record());
            }
            Ok(root.record)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_collected_retired_catalog<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        scope: (&RegionId, Option<DurableRecordKey>),
        protection: ReadProtection,
    ) -> StorageResult<RetiredHistoryCatalogPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_complete_history_source(attempt, proposal, scope.0, protection)
                .await?;
            let key = self.identity_key(
                COLLECTED_CATALOG_SUFFIX,
                &(proposal.operation().id, scope.0, &scope.1),
            )?;
            let row = self
                .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let page: RetiredHistoryCatalogPage = self.decode_collected_catalog_page(&row)?;
            let request = HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: scope.0.clone(),
            };
            page.require_scope(&request, &self.collection_catalog_scope(scope.1))?;
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_collected_retired_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        scope: (&RegionId, CanonicalDigest, u32),
        protection: ReadProtection,
    ) -> StorageResult<RetiredHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_complete_history_source(attempt, proposal, scope.0, protection)
                .await?;
            let key = self.identity_key(
                COLLECTED_RETIRED_PAGE_SUFFIX,
                &(proposal.operation().id, scope.0, scope.1, scope.2),
            )?;
            let row = self
                .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let page: RetiredHistoryPage = self.decode_collected_retired_index(&row)?;
            let request = HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: scope.0.clone(),
            };
            page.definition.require_capture_request(&request)?;
            if page.definition.content() != scope.1 {
                return Err(corrupt_record());
            }
            page.definition.require_page(&page, scope.2)?;
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }
}
