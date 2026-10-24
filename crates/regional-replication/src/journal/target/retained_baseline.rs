//! Hidden target acceptance of original baseline dependencies, without item
//! effects.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{
        BaselineDependencyProgress, HiddenTargetRequest, ReenableBaselinePage,
        ReenableBaselineReceipt, ReenableHistoryPage, copy_scope::BaselineCopyBudget,
    },
    constants::{REENABLE_BASELINE_RECEIPT_SUFFIX, REENABLE_BASELINE_TARGET_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetBaselineProgress {
    definition: CanonicalDigest,
    progress: BaselineDependencyProgress,
    verified: bool,
}

impl JournalStore {
    pub(crate) async fn accept_target_retained_baselines<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        selection: (u64, &ReenableBaselinePage),
        encoding: (&CanonicalJsonCodec, BaselineCopyBudget),
    ) -> StorageResult<ReenableBaselineReceipt> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, root) = self
                .load_hidden_target(attempt, request, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_adoption_reservation(attempt, &root, ReadProtection::Shared)
                .await?;
            self.require_target_retained_inventory(&root)?;
            if !root.baselines_complete || root.verification.is_some() {
                return Err(corrupt_record());
            }
            let cut = self
                .load_target_retained_page(
                    attempt,
                    &root,
                    selection.0,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let definition = &cut.cut.baseline;
            let budget = self.bound_copy_budget(encoding.1)?;
            selection.1.require_content(definition, budget)?;
            let receipt_key = self.identity_key(
                REENABLE_BASELINE_RECEIPT_SUFFIX,
                &(
                    request.operation.id,
                    cut.cut.cut.lineage,
                    selection.1.sequence,
                ),
            )?;
            let receipt = ReenableBaselineReceipt {
                definition: selection.1.definition,
                sequence: selection.1.sequence,
                content: selection.1.content,
                complete: selection.1.complete,
            };
            if let Some(row) = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&receipt_key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
            {
                if row.revision != storage_types::Revision::INITIAL {
                    return Err(corrupt_record());
                }
                let stored: ReenableBaselineReceipt = self.records.decode(&row)?;
                stored.require_page(selection.1)?;
                return Ok(stored);
            }
            let key = self.identity_key(
                REENABLE_BASELINE_TARGET_SUFFIX,
                &(request.operation.id, cut.cut.cut.lineage),
            )?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let mut progress = row
                .as_ref()
                .map(|row| self.records.decode::<TargetBaselineProgress>(row))
                .transpose()?
                .unwrap_or(TargetBaselineProgress {
                    definition: definition.content,
                    progress: BaselineDependencyProgress::from_definition(definition)?,
                    verified: false,
                });
            if progress.definition != definition.content {
                return Err(corrupt_record());
            }
            let authorities = progress
                .progress
                .accept_page(definition, (selection.1, encoding.0, budget))?;
            for authority in authorities {
                self.retain_received_baseline(
                    attempt,
                    (&authority, root.state.physical),
                    encoding.0,
                )
                .await?;
            }
            self.retain_history_epoch(attempt, cut.cut.cut.lineage)
                .await?;
            self.records
                .store(
                    attempt,
                    &[
                        self.records
                            .prepare(key, row.map(|row| row.revision), &progress)?,
                        self.records.prepare(receipt_key, None, &receipt)?,
                    ],
                )
                .await?;
            Ok(receipt)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn load_target_retained_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<ReenableHistoryPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let retained = root.retained_inventory()?;
            if ordinal >= retained.cuts {
                return Err(corrupt_record());
            }
            let key = self.target_retained_page_key(root.request.operation.id, ordinal)?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            if row.key != key {
                return Err(corrupt_record());
            }
            let page: ReenableHistoryPage = self.records.decode(&row)?;
            if page.definition != retained.bootstrap || page.cut.ordinal != ordinal {
                return Err(corrupt_record());
            }
            page.cut.require_scope(&retained.proposal)?;
            let next = ordinal.checked_add(1).ok_or_else(corrupt_record)?;
            if page.next_ordinal != (next < retained.cuts).then_some(next) {
                return Err(corrupt_record());
            }
            Ok(page)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn require_target_retained_baselines<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        page: &ReenableHistoryPage,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.identity_key(
                REENABLE_BASELINE_TARGET_SUFFIX,
                &(root.request.operation.id, page.cut.cut.lineage),
            )?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let progress: TargetBaselineProgress = self.records.decode(&row)?;
            if progress.definition != page.cut.baseline.content || !progress.progress.complete {
                return Err(corrupt_record());
            }
            progress.progress.require_definition(&page.cut.baseline)?;
            Ok(progress.verified)
        }
        .await;
        settlement_guard.complete(result)
    }
    pub(super) async fn verify_target_retained_baselines<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        progress: &mut crate::bootstrap::VerificationProgress,
        encoding: (
            &CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let crate::bootstrap::VerificationStage::RetainedBaselines {
                ordinal,
                after,
                mut count,
                mut vector,
            } = progress.stage.clone()
            else {
                return Err(corrupt_record());
            };
            let cut = self
                .load_target_retained_page(
                    attempt,
                    root,
                    ordinal,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            self.require_target_retained_baselines(
                attempt,
                root,
                &cut,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            let budget = BaselineCopyBudget {
                records: encoding
                    .1
                    .import_events
                    .min(self.records.limits.max_records()),
                bytes: encoding.1.import_bytes.min(self.records.limits.max_bytes()),
            };
            let page = self
                .load_baseline_page(attempt, (cut.cut.cut.lineage, after), (encoding.0, budget))
                .await?;
            for authority in page.authorities {
                vector = cut.cut.baseline.fold_authority(vector, &authority)?;
                count = count.checked_add(1).ok_or_else(corrupt_record)?;
            }
            if count > cut.cut.baseline.count {
                return Err(corrupt_record());
            }
            if let Some(after) = page.continuation {
                progress.stage = crate::bootstrap::VerificationStage::RetainedBaselines {
                    ordinal,
                    after: Some(after),
                    count,
                    vector,
                };
                return Ok(());
            }
            cut.cut.baseline.require_complete(count, vector)?;
            let key = self.identity_key(
                REENABLE_BASELINE_TARGET_SUFFIX,
                &(root.request.operation.id, cut.cut.cut.lineage),
            )?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let mut native: TargetBaselineProgress = self.records.decode(&row)?;
            if native.definition != cut.cut.baseline.content || !native.progress.complete {
                return Err(corrupt_record());
            }
            native.progress.require_definition(&cut.cut.baseline)?;
            native.verified = true;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(key, Some(row.revision), &native)?],
                )
                .await?;
            progress.history = CanonicalDigest::of_json(&(
                progress.history,
                cut.cut.baseline.content,
                count,
                vector,
            ))
            .map_err(|_| corrupt_record())?;
            progress.stage = crate::bootstrap::VerificationStage::RetainedCuts(ordinal);
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }
}
