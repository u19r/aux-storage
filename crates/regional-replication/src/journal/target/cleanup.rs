use infrastructure_provider::{
    page::RecordPageRequest, physical_attempt::PhysicalRecordAttempt, record::RecordChange,
};
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, StorageResult};

use super::record::TargetRecord;
use crate::{
    constants::{EXPECTED_WINNER_SUFFIX, PROJECTION_OWNER_SUFFIX, TARGET_BOOTSTRAP_SUFFIX},
    control::ControlDecision,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    /// Retire only operation-local derived indexes after native terminal proof.
    /// Deletion itself is durable progress; immutable authority and receipts
    /// stay.
    pub(crate) async fn cleanup_target_verification<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let control = self
                .load_control(attempt, proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self.decode_control(&control, proposal)?;
            if !matches!(
                control.decision,
                ControlDecision::Completed | ControlDecision::Cancelled
            ) {
                return Err(corrupt_record());
            }
            let key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &proposal.operation().id)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let Some(row) = &rows[0] else {
                return Ok(true);
            };
            let root: TargetRecord = self.records.decode(row)?;
            let mut operation = proposal.operation().clone();
            operation.content = [0; 32].into();
            if root.request.operation != operation || root.content != root.request.content()? {
                return Err(corrupt_record());
            }
            match control.decision {
                ControlDecision::Completed => {
                    self.require_verification_evidence(
                        &root,
                        root.verification.as_ref().ok_or_else(corrupt_record)?,
                    )?;
                    if control.bootstrap != root.definition || root.definition.is_none() {
                        return Err(corrupt_record());
                    }
                }
                ControlDecision::Cancelled if root.cancelled => {}
                _ => return Err(corrupt_record()),
            }
            for prefix in [EXPECTED_WINNER_SUFFIX, PROJECTION_OWNER_SUFFIX] {
                let first = self.identity_key(prefix, &operation.id)?;
                let page = self
                    .records
                    .scan(
                        attempt,
                        RecordPageRequest {
                            range: self.record_prefix_range(&first)?,
                            after: None,
                            max_bytes: self.records.limits.max_bytes(),
                            protection: ReadProtection::Conflict,
                        },
                    )
                    .await?;
                if page.records.is_empty() {
                    continue;
                }
                let changes = page
                    .records
                    .into_iter()
                    .map(|row| RecordChange::Delete {
                        key: row.key,
                        expected: row.revision,
                    })
                    .collect::<Vec<_>>();
                self.records.store(attempt, &changes).await?;
                return Ok(false);
            }
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }
}
