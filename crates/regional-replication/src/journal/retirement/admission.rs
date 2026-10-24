use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::regional_import::RegionalImport;
use storage_types::{ReadProtection, StorageResult};

use super::record::{ReceivedSeal, RetiredOrigin};
use crate::{
    constants::{RECEIVED_SEAL_SUFFIX, RETIRED_ORIGIN_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn require_retired_origin_bounds<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            for binding in import.bindings() {
                let key = self.identity_key(
                    RETIRED_ORIGIN_SUFFIX,
                    &(
                        binding.local_table,
                        binding.source_table,
                        binding.lineage,
                        &import.manifest().origin,
                    ),
                )?;
                let rows = self
                    .records
                    .load(attempt, &[key], ReadProtection::Shared)
                    .await?;
                let Some(row) = &rows[0] else {
                    continue;
                };
                let retired: RetiredOrigin = self.records.decode(row)?;
                let root = self.load_retired_seal_root(attempt, &retired).await?;
                if retired.cut.origin != import.manifest().origin
                    || root.request.before()?.configuration.table != binding.source_table
                    || root.request.before()?.configuration.lineage != binding.lineage
                {
                    return Err(corrupt_record());
                }
                let Some(control) = self
                    .load_control(attempt, &root.request.proposal, ReadProtection::Shared)
                    .await?
                else {
                    return Err(corrupt_record());
                };
                let decision = self
                    .decode_control(&control, &root.request.proposal)?
                    .decision;
                if matches!(
                    decision,
                    crate::control::ControlDecision::Installed
                        | crate::control::ControlDecision::Completed
                ) && import.manifest().range.through() > retired.cut.through
                {
                    return Err(corrupt_record());
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
    pub(super) async fn load_retired_seal_root<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        retired: &RetiredOrigin,
    ) -> StorageResult<ReceivedSeal> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key =
                self.identity_key(RECEIVED_SEAL_SUFFIX, &(retired.operation, &retired.region))?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let root: ReceivedSeal = self
                .records
                .decode(rows[0].as_ref().ok_or_else(corrupt_record)?)?;
            if root.request.content()? != retired.content
                || root.request.region != retired.region
                || root.request.proposal.operation().id != retired.operation
            {
                return Err(corrupt_record());
            }
            Ok(root)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_cancelled_retired_origin<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        retired: &RetiredOrigin,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let root = self.load_retired_seal_root(attempt, retired).await?;
            let row = self
                .load_control(attempt, &root.request.proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            if self.decode_control(&row, &root.request.proposal)?.decision
                != crate::control::ControlDecision::Cancelled
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
