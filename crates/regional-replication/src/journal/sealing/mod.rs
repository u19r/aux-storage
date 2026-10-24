mod cancellation;
mod control;
mod cuts;
mod drain;
mod observation;
mod preparation;
mod record;
mod vector;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use record::SealPhase;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    journal::JournalStore,
    lifecycle::{SealRequest, SealStatus},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn advance_table_seal<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<SealStatus> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some((row, mut record)) = self
                .load_table_seal(attempt, request, ReadProtection::Conflict)
                .await?
            else {
                return self.begin_table_seal(attempt, request).await;
            };
            if matches!(
                record.phase,
                SealPhase::Removed | SealPhase::Retired | SealPhase::Cancelled
            ) {
                return record.status();
            }
            if let Some(control) = self
                .load_control(attempt, &request.proposal, ReadProtection::Shared)
                .await?
                && self.decode_control(&control, &request.proposal)?.decision
                    == crate::control::ControlDecision::Cancelled
            {
                return Err(corrupt_record());
            }
            self.require_sealing_table(attempt, request).await?;
            match record.phase {
                SealPhase::RecordingCuts(_) => {
                    self.record_table_seal_cuts(attempt, &mut record).await?
                }
                SealPhase::Draining(_) => {
                    self.advance_table_seal_drain(attempt, &mut record).await?
                }
                SealPhase::Ready => return record.status(),
                _ => return Err(corrupt_record()),
            }
            self.store_table_seal(attempt, &record, Some(row.revision))
                .await?;
            record.status()
        }
        .await;
        settlement.complete(result)
    }
}

mod retirement;
