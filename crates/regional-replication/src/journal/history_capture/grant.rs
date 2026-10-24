use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::protocol::CanonicalDigest;
use storage_types::{ReadProtection, StorageResult};

use super::record::HistoryCapturePhase;
use crate::{
    control::ControlDecision,
    history_capture::{HistoryCaptureDefinition, HistoryCaptureRequest},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    /// Qualify a frozen capture for grant or installation. Preparation readers
    /// continue to reject an existing control decision at their own boundary.
    pub(in crate::journal) async fn require_control_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        bootstrap: CanonicalDigest,
    ) -> StorageResult<HistoryCaptureDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (_, record) = self
                .load_history_capture(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(record.phase, HistoryCapturePhase::Ready) {
                return Err(corrupt_record());
            }
            self.require_history_owner(attempt, request, storage_types::ReadProtection::Conflict)
                .await?;
            if let Some(row) = self
                .load_control(attempt, &request.proposal, ReadProtection::Shared)
                .await?
            {
                let control = self.decode_control(&row, &request.proposal)?;
                if !matches!(
                    control.decision,
                    ControlDecision::Prepared | ControlDecision::Committed
                ) || control.bootstrap != Some(bootstrap)
                {
                    return Err(corrupt_record());
                }
            }
            HistoryCaptureDefinition::from_ready(record.request, record.schema, record.vector)
        }
        .await;
        settlement.complete(result)
    }
}
