use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::plan::PlannedBaselineCopy;
use crate::{
    bootstrap::{CommittedBaselinePage, baseline_page_content},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn acknowledge_baseline_copy<
        A: PhysicalRecordAttempt + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        planned: &PlannedBaselineCopy,
        receipt: &CommittedBaselinePage,
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = self
                .load_source_bootstrap_definition(
                    attempt,
                    &planned.scope.proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let content = baseline_page_content(definition.content(), &planned.page)?;
            if receipt.sequence != planned.page.sequence
                || receipt.content != content
                || receipt.baselines_complete != planned.page.complete
            {
                return Err(corrupt_record());
            }
            let (row, current) = self
                .load_source_copy(
                    attempt,
                    (&planned.scope, &definition),
                    ReadProtection::Conflict,
                )
                .await?;
            let mut next = planned.after.clone();
            next.last_receipt = Some(content);
            if current == next {
                return Ok(current.complete);
            }
            if current.sequence > next.sequence {
                return Ok(current.complete);
            }
            if current != planned.before {
                return Err(corrupt_record());
            }
            let selected = self
                .load_selected_copy(attempt, &planned.scope, &current)
                .await?
                .ok_or_else(corrupt_record)?;
            if selected.content != content || selected.after != planned.after {
                return Err(corrupt_record());
            }
            let key = self.source_copy_key(&planned.scope)?;
            let change = self
                .records
                .prepare(key, row.map(|row| row.revision), &next)?;
            self.records.store(attempt, &[change]).await?;
            Ok(next.complete)
        }
        .await;
        settlement_guard.complete(result)
    }
}
