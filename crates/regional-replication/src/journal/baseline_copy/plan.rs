use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::CanonicalBytes;
use storage_types::{ReadProtection, StorageResult};

use super::record::{CopyProgress, SelectedCopyPage};
use crate::{
    bootstrap::{
        BaselineCopyPage, BaselineCopyScope, TargetBootstrapDefinition,
        copy_scope::BaselineCopyBudget,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

/// A page selected from a native snapshot. It carries no acknowledgement.
pub(crate) struct PlannedBaselineCopy {
    pub scope: Arc<BaselineCopyScope>,
    pub definition: Arc<TargetBootstrapDefinition>,
    pub page: Arc<BaselineCopyPage>,
    pub(super) before: CopyProgress,
    pub(super) after: CopyProgress,
}

impl JournalStore {
    pub(crate) async fn plan_baseline_copy<
        A: stream_provider::retained_image::RetainedOwnershipAttempt
            + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: Arc<BaselineCopyScope>,
        input: (&CanonicalJsonCodec, BaselineCopyBudget),
    ) -> StorageResult<Option<PlannedBaselineCopy>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = Arc::new(
                self.load_source_bootstrap_definition(
                    attempt,
                    &scope.proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?,
            );
            let (_, before) = self
                .load_source_copy(attempt, (&scope, &definition), ReadProtection::Conflict)
                .await?;
            if before.complete {
                return Ok(None);
            }
            let selected = self.load_selected_copy(attempt, &scope, &before).await?;
            let budget =
                self.bound_copy_budget(selected.as_ref().map_or(input.1, |record| record.budget))?;
            let page = self
                .load_baseline_page(
                    attempt,
                    (scope.proposal.operation().lineage, before.after.clone()),
                    (input.0, budget),
                )
                .await?;
            for authority in &page.authorities {
                definition.initial().require_baseline_scope(authority)?;
            }
            let complete = page.continuation.is_none();
            let items = page
                .authorities
                .into_iter()
                .map(|authority| {
                    CanonicalBytes::try_new(
                        authority.into_bytes(),
                        self.records.limits.max_record_bytes(),
                    )
                    .map_err(|_| corrupt_record())
                })
                .collect::<StorageResult<Vec<_>>>()?;
            if !complete && items.is_empty() {
                return Err(corrupt_record());
            }
            let mut after = before.clone();
            after.sequence = after.sequence.checked_add(1).ok_or_else(corrupt_record)?;
            after.after = page.continuation;
            after.complete = complete;
            let page = Arc::new(BaselineCopyPage {
                sequence: before.sequence,
                items,
                complete,
            });
            let planned = PlannedBaselineCopy {
                scope,
                definition,
                page,
                before,
                after,
            };
            self.preserve_copy_selection(attempt, &planned, (selected, budget))
                .await?;
            Ok(Some(planned))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn preserve_copy_selection<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        planned: &PlannedBaselineCopy,
        input: (Option<SelectedCopyPage>, BaselineCopyBudget),
    ) -> StorageResult<()> {
        let content =
            crate::bootstrap::baseline_page_content(planned.definition.content(), &planned.page)?;
        if let Some(selected) = input.0 {
            if selected.content != content || selected.after != planned.after {
                return Err(corrupt_record());
            }
        } else {
            let selected = SelectedCopyPage {
                before: planned.before.clone(),
                after: planned.after.clone(),
                content,
                budget: input.1,
            };
            let key = self.source_copy_page_key(&planned.scope, planned.before.sequence)?;
            self.records
                .store(attempt, &[self.records.prepare(key, None, &selected)?])
                .await?;
        }
        Ok(())
    }

    pub(in crate::journal) fn bound_copy_budget(
        &self,
        requested: BaselineCopyBudget,
    ) -> StorageResult<BaselineCopyBudget> {
        let records = self
            .records
            .limits
            .max_records()
            .get()
            .saturating_sub(2)
            .max(1)
            .min(requested.records.get());
        Ok(BaselineCopyBudget {
            records: std::num::NonZeroUsize::new(records).ok_or_else(corrupt_record)?,
            bytes: requested.bytes.min(self.records.limits.max_bytes()),
        })
    }
}
