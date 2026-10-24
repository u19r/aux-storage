//! Historical vectors reproduce the activated lineage inventory, including
//! completeness.
use infrastructure_provider::{
    page::RecordPageRequest, physical_attempt::PhysicalRecordAttempt, record::RecordAddress,
};
use replication_types::{
    configuration::{installed::InstalledTableState, membership::MembershipRevision},
    identity::resource::TableLineage,
};
use storage_types::{ReadProtection, StorageResult};

use super::record::CatalogLineage;
use crate::{
    bootstrap::ReenableHistoryPage,
    history::{response_budget::encoded_length, source_cut::CanonicalSourceCut},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn load_installed_history_vector<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        input: ((TableLineage, MembershipRevision), usize),
        protection: ReadProtection,
    ) -> StorageResult<Vec<CanonicalSourceCut>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (root, reference, activation) = self
                .load_installed_catalog_lineage(attempt, state, input.0, protection)
                .await?;
            let mut prefix: RecordAddress = self.retained_catalog_prefix(state.physical)?.into();
            prefix.suffix.extend_from_slice(root.lineage.as_ref());
            let range = self.record_prefix_range(&prefix.try_into()?)?;
            let mut after = None;
            let mut remaining_scan = input.1;
            let mut remaining_result = input.1;
            let mut entries = Vec::new();
            loop {
                let bytes = std::num::NonZeroUsize::new(
                    remaining_scan.min(self.records.limits.max_bytes().get()),
                )
                .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
                let page = self
                    .records
                    .scan(
                        attempt,
                        RecordPageRequest {
                            range: range.clone(),
                            after,
                            max_bytes: bytes,
                            protection,
                        },
                    )
                    .await?;
                for row in page.records {
                    remaining_scan = remaining_scan
                        .checked_sub(row.bytes.len().max(1))
                        .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
                    if let Some(entry) = self
                        .load_installed_catalog_entry(attempt, state, &row.key, protection)
                        .await?
                    {
                        if entry.definition != activation.definition
                            || entry.cut.cut.lineage != root.lineage
                            || entry.cut.cut.membership != root.membership
                        {
                            return Err(corrupt_record());
                        }
                        remaining_result = remaining_result
                            .checked_sub(encoded_length(&entry, remaining_result)?)
                            .ok_or_else(corrupt_record)?;
                        entries.push(entry);
                    }
                }
                after = page.continuation;
                if after.is_none() {
                    break;
                }
            }
            self.require_complete_catalog_vector(&root, &reference, &mut entries)?;
            Ok(entries.into_iter().map(|entry| entry.cut.cut).collect())
        }
        .await;
        settlement_guard.complete(result)
    }

    fn require_complete_catalog_vector(
        &self,
        root: &CatalogLineage,
        reference: &super::record::CatalogLineageReference,
        entries: &mut [ReenableHistoryPage],
    ) -> StorageResult<()> {
        entries.sort_unstable_by_key(|entry| entry.cut.ordinal);
        if u64::try_from(entries.len()).map_err(|_| corrupt_record())? != reference.count
            || entries.first().map(|entry| entry.cut.ordinal) != Some(reference.first.ordinal)
            || entries.last().map(|entry| entry.cut.ordinal) != Some(reference.last)
        {
            return Err(corrupt_record());
        }
        let mut vector = root.seed()?;
        let mut previous = None;
        for entry in entries {
            if previous.is_some_and(|ordinal| ordinal >= entry.cut.ordinal) {
                return Err(corrupt_record());
            }
            previous = Some(entry.cut.ordinal);
            vector = CatalogLineage::fold(vector, &entry.cut)?;
        }
        if vector != reference.vector {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
