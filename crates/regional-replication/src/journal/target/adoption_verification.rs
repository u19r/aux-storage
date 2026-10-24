use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::DurableRecordKey,
};
use storage_types::{ReadProtection, StorageResult};

use super::{cuts::TargetCut, record::TargetRecord};
use crate::{
    bootstrap::{VerificationProgress, VerificationStage},
    constants::TARGET_CUT_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn verify_adopted_target_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (Option<DurableRecordKey>, &mut VerificationProgress),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let adoption = root.adoption.as_ref().ok_or_else(corrupt_record)?;
            self.require_target_adoption(root, &adoption.definition)?;
            let first = self.identity_key(TARGET_CUT_SUFFIX, &root.request.operation.id)?;
            let original = self.record_prefix_range(&first)?;
            let (first, last, _) = original.bounds();
            let count = std::num::NonZeroUsize::new(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .saturating_sub(1)
                    .max(1),
            )
            .ok_or_else(corrupt_record)?;
            let range = BoundedRecordRange::try_new(first.clone(), last.clone(), count)?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after: input.0,
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Shared,
                    },
                )
                .await?;
            for row in page.records {
                let cut: TargetCut = self.records.decode(&row)?;
                self.require_target_cut(root, &cut)?;
                if self.target_cut_key(root.request.operation.id, cut.scope())? != row.key {
                    return Err(corrupt_record());
                }
                cut.require_complete_coverage()?;
                if cut.scope().1 == root.state.configuration.lineage {
                    input.1.cut_count = input
                        .1
                        .cut_count
                        .checked_add(1)
                        .ok_or_else(corrupt_record)?;
                }
            }
            if input.1.cut_count > adoption.declared_cuts {
                return Err(corrupt_record());
            }
            if page.continuation.is_none()
                && (input.1.cut_count != adoption.declared_cuts
                    || Some(input.1.vector) != root.source_vector)
            {
                return Err(corrupt_record());
            }
            input.1.stage = page
                .continuation
                .map_or(VerificationStage::Baselines(None), |after| {
                    VerificationStage::AdoptedCuts(Some(after))
                });
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
