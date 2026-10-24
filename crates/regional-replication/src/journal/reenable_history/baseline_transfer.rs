//! Durable source progress for immutable history-only baseline dependencies.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::{CanonicalBytes, CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::StorageResult;

use crate::{
    bootstrap::{
        BaselineCopyScope, BaselineDependencyProgress, ReenableBaselineDefinition,
        ReenableBaselinePage, ReenableBaselineReceipt, copy_scope::BaselineCopyBudget,
    },
    constants::REENABLE_BASELINE_SOURCE_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceBaselineProgress {
    scope: CanonicalDigest,
    definition: CanonicalDigest,
    progress: BaselineDependencyProgress,
    after: Option<DurableRecordKey>,
    last: Option<CanonicalDigest>,
    selected: Option<SelectedBaselinePage>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedBaselinePage {
    budget: BaselineCopyBudget,
    content: CanonicalDigest,
    after: Option<DurableRecordKey>,
}

impl JournalStore {
    async fn load_reenable_baseline_progress<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        definition: &ReenableBaselineDefinition,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(
        Option<DurableRecord>,
        SourceBaselineProgress,
        DurableRecordKey,
    )> {
        let key = self.identity_key(
            REENABLE_BASELINE_SOURCE_SUFFIX,
            &(
                scope.proposal.operation().id,
                &scope.destination.region,
                definition.initial.proposal().operation().lineage,
            ),
        )?;
        let row = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten();
        let progress = match &row {
            Some(row) => self.records.decode::<SourceBaselineProgress>(row)?,
            None => SourceBaselineProgress {
                scope: scope.content()?,
                definition: definition.content,
                progress: BaselineDependencyProgress::from_definition(definition)?,
                after: None,
                last: None,
                selected: None,
            },
        };
        if progress.scope != scope.content()? || progress.definition != definition.content {
            return Err(corrupt_record());
        }
        progress.progress.require_definition(definition)?;
        Ok((row, progress, key))
    }

    pub(crate) async fn plan_reenable_baseline<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (&BaselineCopyScope, u64),
        encoding: (&CanonicalJsonCodec, BaselineCopyBudget),
    ) -> StorageResult<Option<ReenableBaselinePage>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_reenable_copy_complete(
                attempt,
                selection.0,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            let (cut, _) = self
                .require_reenable_delivery(
                    attempt,
                    selection.0,
                    selection.1,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let (row, mut current, key) = self
                .load_reenable_baseline_progress(
                    attempt,
                    selection.0,
                    &cut.cut.baseline,
                    storage_types::ReadProtection::Conflict,
                )
                .await?;
            if current.progress.complete {
                return Ok(None);
            }
            let budget = self.bound_copy_budget(
                current
                    .selected
                    .as_ref()
                    .map_or(encoding.1, |selected| selected.budget),
            )?;
            let (page, after) = self
                .select_reenable_baseline_page(
                    attempt,
                    &cut.cut.baseline,
                    (&current, encoding.0, budget),
                )
                .await?;
            if let Some(selected) = &current.selected {
                if selected.content != page.content || selected.after != after {
                    return Err(corrupt_record());
                }
            } else {
                current.selected = Some(SelectedBaselinePage {
                    budget,
                    content: page.content,
                    after,
                });
                self.records
                    .store(
                        attempt,
                        &[self
                            .records
                            .prepare(key, row.map(|row| row.revision), &current)?],
                    )
                    .await?;
            }
            Ok(Some(page))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn select_reenable_baseline_page<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        definition: &ReenableBaselineDefinition,
        input: (
            &SourceBaselineProgress,
            &CanonicalJsonCodec,
            BaselineCopyBudget,
        ),
    ) -> StorageResult<(ReenableBaselinePage, Option<DurableRecordKey>)> {
        let native = self
            .load_baseline_page(
                attempt,
                (
                    definition.initial.proposal().operation().lineage,
                    input.0.after.clone(),
                ),
                (input.1, input.2),
            )
            .await?;
        let items = native
            .authorities
            .into_iter()
            .map(|authority| {
                definition.initial.require_baseline_scope(&authority)?;
                CanonicalBytes::try_new(
                    authority.into_bytes(),
                    self.records.limits.max_record_bytes(),
                )
                .map_err(|_| corrupt_record())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let page = ReenableBaselinePage::from_native(
            definition.content,
            input.0.progress.sequence,
            items,
            native.continuation.is_none(),
        )?;
        let mut projected = input.0.progress.clone();
        projected.accept_page(definition, (&page, input.1, input.2))?;
        Ok((page, native.continuation))
    }

    pub(crate) async fn record_reenable_baseline<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (&BaselineCopyScope, u64),
        input: (&ReenableBaselinePage, &ReenableBaselineReceipt),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            input.1.require_page(input.0)?;
            self.require_reenable_copy_complete(
                attempt,
                selection.0,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            let (cut, _) = self
                .require_reenable_delivery(
                    attempt,
                    selection.0,
                    selection.1,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let definition = &cut.cut.baseline;
            let (row, mut current, key) = self
                .load_reenable_baseline_progress(
                    attempt,
                    selection.0,
                    definition,
                    storage_types::ReadProtection::Conflict,
                )
                .await?;
            if current.progress.sequence != input.0.sequence {
                if current.progress.sequence
                    == input.0.sequence.checked_add(1).ok_or_else(corrupt_record)?
                    && current.last == Some(input.0.content)
                {
                    return Ok(current.progress.complete);
                }
                return Err(corrupt_record());
            }
            let selected = current.selected.as_ref().ok_or_else(corrupt_record)?;
            input.0.require_content(definition, selected.budget)?;
            let (native, after) = self
                .select_reenable_baseline_page(
                    attempt,
                    definition,
                    (&current, codec, selected.budget),
                )
                .await?;
            if native.content != input.0.content
                || selected.content != native.content
                || selected.after != after
            {
                return Err(corrupt_record());
            }
            current
                .progress
                .accept_page(definition, (input.0, codec, selected.budget))?;
            current.selected = None;
            current.after = after;
            current.last = Some(input.0.content);
            let complete = current.progress.complete;
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(key, row.map(|row| row.revision), &current)?],
                )
                .await?;
            Ok(complete)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn require_reenable_baseline_complete<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (&BaselineCopyScope, u64),
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(
        crate::bootstrap::ReenableHistoryPage,
        crate::delivery::DeliveryScope,
    )> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (cut, delivery) = self
                .require_reenable_delivery(attempt, selection.0, selection.1, protection)
                .await?;
            let (_, current, _) = self
                .load_reenable_baseline_progress(
                    attempt,
                    selection.0,
                    &cut.cut.baseline,
                    protection,
                )
                .await?;
            if !current.progress.complete {
                return Err(corrupt_record());
            }
            Ok((cut, delivery))
        }
        .await;
        settlement_guard.complete(result)
    }
}
