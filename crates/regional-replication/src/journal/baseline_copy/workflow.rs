use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{BaselineCopyScope, BootstrapTailScope},
    constants::SOURCE_TRANSFER_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TransferPhase {
    Capturing,
    DeclaringRetained(u64),
    Copying,
    RetainedBaselines(u64),
    RetainedOriginals(u64),
    Tailing(u32),
    Verifying,
    Verified,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferRecord {
    scope: CanonicalDigest,
    phase: TransferPhase,
}

impl JournalStore {
    pub(crate) async fn load_transfer_phase<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
    ) -> StorageResult<TransferPhase> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            scope.validate()?;
            let key = self.identity_key(
                SOURCE_TRANSFER_SUFFIX,
                &(scope.proposal.operation().id, &scope.destination.region),
            )?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Snapshot)
                .await?;
            rows[0]
                .as_ref()
                .map(|row| self.decode_transfer_phase(row, scope))
                .transpose()
                .map(|phase| phase.unwrap_or(TransferPhase::Capturing))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn advance_transfer_phase<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        expected: TransferPhase,
    ) -> StorageResult<TransferPhase> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            scope.validate()?;
            let key = self.identity_key(
                SOURCE_TRANSFER_SUFFIX,
                &(scope.proposal.operation().id, &scope.destination.region),
            )?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let current = rows[0]
                .as_ref()
                .map(|row| self.decode_transfer_phase(row, scope))
                .transpose()?
                .unwrap_or(TransferPhase::Capturing);
            if current != expected || current == TransferPhase::Verified {
                return Ok(current);
            }
            let next = self
                .qualified_transfer_phase(attempt, scope, current)
                .await?;
            let record = TransferRecord {
                scope: scope.content()?,
                phase: next,
            };
            let change =
                self.records
                    .prepare(key, rows[0].as_ref().map(|row| row.revision), &record)?;
            self.records.store(attempt, &[change]).await?;
            Ok(next)
        }
        .await;
        settlement_guard.complete(result)
    }

    fn decode_transfer_phase(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
        scope: &BaselineCopyScope,
    ) -> StorageResult<TransferPhase> {
        let record: TransferRecord = self.records.decode(row)?;
        if record.scope != scope.content()? {
            return Err(corrupt_record());
        }
        if let TransferPhase::Tailing(partition) = record.phase
            && partition >= u32::from(self.partitions)
        {
            return Err(corrupt_record());
        }
        Ok(record.phase)
    }

    async fn qualified_transfer_phase<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        phase: TransferPhase,
    ) -> StorageResult<TransferPhase> {
        let definition = self
            .load_enable_bootstrap_definition(
                attempt,
                &scope.proposal,
                storage_types::ReadProtection::Shared,
            )
            .await?;
        scope.require_definition(&definition)?;
        match phase {
            TransferPhase::Capturing => Ok(
                if matches!(
                    definition,
                    crate::bootstrap::TargetBootstrapDefinition::Reenable(_)
                ) {
                    TransferPhase::DeclaringRetained(0)
                } else {
                    TransferPhase::Copying
                },
            ),
            TransferPhase::DeclaringRetained(ordinal) => {
                let (page, _) = self
                    .require_reenable_delivery(
                        attempt,
                        scope,
                        ordinal,
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                Ok(page
                    .next_ordinal
                    .map_or(TransferPhase::Copying, TransferPhase::DeclaringRetained))
            }
            TransferPhase::RetainedBaselines(ordinal) => {
                let (page, _) = self
                    .require_reenable_baseline_complete(
                        attempt,
                        (scope, ordinal),
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                Ok(page.next_ordinal.map_or(
                    TransferPhase::RetainedOriginals(0),
                    TransferPhase::RetainedBaselines,
                ))
            }
            TransferPhase::RetainedOriginals(ordinal) => {
                let (page, covered) = self
                    .require_reenable_original_coverage(
                        attempt,
                        scope,
                        ordinal,
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                if covered.through() != page.cut.cut.through || !covered.pending().is_empty() {
                    return Err(corrupt_record());
                }
                Ok(page
                    .next_ordinal
                    .map_or(TransferPhase::Tailing(0), TransferPhase::RetainedOriginals))
            }
            TransferPhase::Copying => {
                let (_, copied) = self
                    .load_source_copy(attempt, (scope, &definition), ReadProtection::Shared)
                    .await?;
                if !copied.complete {
                    return Err(corrupt_record());
                }
                Ok(
                    if matches!(
                        definition,
                        crate::bootstrap::TargetBootstrapDefinition::Reenable(_)
                    ) {
                        TransferPhase::RetainedBaselines(0)
                    } else {
                        TransferPhase::Tailing(0)
                    },
                )
            }
            TransferPhase::Tailing(partition) => {
                let cut = self
                    .load_source_capture_cut(attempt, &scope.proposal, partition)
                    .await?;
                let tail = BootstrapTailScope {
                    transfer: scope.clone(),
                    cut,
                };
                let covered = self
                    .require_bootstrap_tail(attempt, &tail, ReadProtection::Shared)
                    .await?;
                if covered.through() < tail.cut.through {
                    return Err(corrupt_record());
                }
                let next = partition.checked_add(1).ok_or_else(corrupt_record)?;
                Ok(if next == u32::from(self.partitions) {
                    TransferPhase::Verifying
                } else {
                    TransferPhase::Tailing(next)
                })
            }
            TransferPhase::Verifying => Ok(TransferPhase::Verified),
            TransferPhase::Verified => Ok(phase),
        }
    }
}
