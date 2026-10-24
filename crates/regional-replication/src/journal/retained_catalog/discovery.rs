//! One native catalog slot per turn bounds qualification and preserves
//! hidden-slot progress.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::DurableRecordKey,
};
use replication_types::configuration::installed::InstalledTableState;
use storage_types::{ReadProtection, StorageError, StorageResult};

use crate::{
    history::discovery::{DiscoveredHistorySource, HistorySourceFrontier, HistorySourcePosition},
    journal::{JournalStore, history_discovery::SourceDiscoveryPage},
};

impl JournalStore {
    pub(in crate::journal) async fn load_historical_discovery_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        after: Option<DurableRecordKey>,
        protection: ReadProtection,
    ) -> StorageResult<SourceDiscoveryPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let prefix = self.retained_catalog_prefix(state.physical)?;
            let original = self.record_prefix_range(&prefix)?;
            let bounds = original.bounds();
            let range = BoundedRecordRange::try_new(
                bounds.0.clone(),
                bounds.1.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            if after.as_ref().is_some_and(|key| !range.contains(key)) {
                return Err(StorageError::validation(
                    "Historical discovery cursor belongs to another table",
                ));
            }
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after,
                        max_bytes: self.records.limits.max_bytes(),
                        protection,
                    },
                )
                .await?;
            let next = page
                .continuation
                .map(|after| HistorySourcePosition::Historical { after: Some(after) });
            let mut result = SourceDiscoveryPage {
                source: None,
                available: Vec::new(),
                next,
            };
            if let Some(row) = page.records.first()
                && let Some(page) = self
                    .load_installed_catalog_entry(attempt, state, &row.key, protection)
                    .await?
            {
                let cut = page.cut.cut;
                result.source = Some(DiscoveredHistorySource::Historical {
                    lineage: cut.lineage,
                    membership: cut.membership,
                    source: cut.source,
                });
                result.available.push(HistorySourceFrontier {
                    origin: cut.origin,
                    through: cut.through,
                });
            }
            Ok(result)
        }
        .await;
        settlement_guard.complete(result)
    }
}
