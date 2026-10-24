//! Durable ordered dependency and original phases for AddRegion's retained
//! inventory.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{BaselineCopyScope, retained_transfer_executor::RetainedTransferStage},
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedTransferProgress {
    scope: CanonicalDigest,
    bootstrap: CanonicalDigest,
    stage: RetainedTransferStage,
}

impl JournalStore {
    pub(crate) async fn load_retained_transfer_stage<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<RetainedTransferStage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = self
                .load_source_bootstrap_definition(attempt, &scope.proposal, protection)
                .await?;
            scope.require_definition(&definition)?;
            let Some(inventory) = definition.retained_inventory()? else {
                return Ok(RetainedTransferStage::Complete);
            };
            self.require_reenable_copy_complete(attempt, scope, protection)
                .await?;
            let key = self.identity_key(
                crate::constants::RETAINED_TRANSFER_PROGRESS_SUFFIX,
                &(scope.proposal.operation().id, &scope.destination.region),
            )?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten();
            let Some(row) = row else {
                return Ok(RetainedTransferStage::Baselines(0));
            };
            let progress: RetainedTransferProgress = self.records.decode(&row)?;
            if progress.scope != scope.content()? || progress.bootstrap != inventory.bootstrap {
                return Err(corrupt_record());
            }
            match progress.stage {
                RetainedTransferStage::Baselines(ordinal)
                | RetainedTransferStage::Originals(ordinal)
                    if ordinal >= inventory.cuts =>
                {
                    Err(corrupt_record())
                }
                _ => Ok(progress.stage),
            }
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn advance_retained_transfer_stage<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        expected: RetainedTransferStage,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let current = self
                .load_retained_transfer_stage(attempt, scope, storage_types::ReadProtection::Shared)
                .await?;
            if current != expected || current == RetainedTransferStage::Complete {
                return Ok(());
            }
            let inventory = self
                .load_source_bootstrap_definition(
                    attempt,
                    &scope.proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?
                .retained_inventory()?
                .ok_or_else(corrupt_record)?;
            let stage = match current {
                RetainedTransferStage::Baselines(ordinal) => {
                    self.require_reenable_baseline_complete(
                        attempt,
                        (scope, ordinal),
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                    if ordinal + 1 < inventory.cuts {
                        RetainedTransferStage::Baselines(ordinal + 1)
                    } else {
                        RetainedTransferStage::Originals(0)
                    }
                }
                RetainedTransferStage::Originals(ordinal) => {
                    let (page, coverage) = self
                        .require_reenable_original_coverage(
                            attempt,
                            scope,
                            ordinal,
                            storage_types::ReadProtection::Shared,
                        )
                        .await?;
                    if coverage.through() != page.cut.cut.through || !coverage.pending().is_empty()
                    {
                        return Err(corrupt_record());
                    }
                    if ordinal + 1 < inventory.cuts {
                        RetainedTransferStage::Originals(ordinal + 1)
                    } else {
                        RetainedTransferStage::Complete
                    }
                }
                RetainedTransferStage::Complete => return Ok(()),
            };
            let key = self.identity_key(
                crate::constants::RETAINED_TRANSFER_PROGRESS_SUFFIX,
                &(scope.proposal.operation().id, &scope.destination.region),
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
            if let Some(row) = &row {
                let progress: RetainedTransferProgress = self.records.decode(row)?;
                if progress.scope != scope.content()? || progress.bootstrap != inventory.bootstrap {
                    return Err(corrupt_record());
                }
                if progress.stage != current {
                    return Ok(());
                }
            } else if current != RetainedTransferStage::Baselines(0) {
                return Err(corrupt_record());
            }
            let progress = RetainedTransferProgress {
                scope: scope.content()?,
                bootstrap: inventory.bootstrap,
                stage,
            };
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(key, row.map(|row| row.revision), &progress)?],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }
}
