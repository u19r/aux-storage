use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, StorageResult};

use super::{record::CapturePhase, status};
use crate::{
    bootstrap::SourceCaptureStatus, control::ControlDecision, journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    /// All participant installations precede native completion. Releasing the
    /// retention authority preserves the immutable cuts for receipt recovery.
    pub(crate) async fn release_source_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<SourceCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (row, mut record) = self
                .load_source_capture(attempt, proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self
                .load_control(attempt, proposal, ReadProtection::Shared)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self.decode_control(&control, proposal)?;
            let definition = self
                .load_enable_bootstrap_definition(
                    attempt,
                    proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            if control.decision != ControlDecision::Completed
                || control.bootstrap != Some(definition.content())
            {
                return Err(corrupt_record());
            }
            match record.phase {
                CapturePhase::BaselineReady => record.phase = CapturePhase::Releasing(0),
                CapturePhase::Releasing(_) => {}
                CapturePhase::Released => return Ok(status(&record.phase)),
                _ => return Err(corrupt_record()),
            }
            self.advance_source_cuts(attempt, &mut record).await?;
            if matches!(record.phase, CapturePhase::Released) {
                self.settle_reenable_inventory(
                    attempt,
                    proposal,
                    super::super::reenable_history::InventorySettlement::Release,
                )
                .await?;
            }
            self.store_source_capture(attempt, &record, Some(row.revision))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }
}
