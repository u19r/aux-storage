use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{SealPhase, SealRecord};
use crate::{
    journal::JournalStore,
    lifecycle::{SealRequest, SealStatus},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn begin_table_seal<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<SealStatus> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_control_revision(
                attempt,
                &request.proposal,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            if self
                .load_control(attempt, &request.proposal, ReadProtection::Conflict)
                .await?
                .is_some()
            {
                return Err(corrupt_record());
            }
            let slot = self.reserve_control(attempt, &request.proposal).await?;
            let before = request.before()?;
            let observed = self
                .observe_table_state(
                    attempt,
                    before.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?
                .ok_or_else(corrupt_record)?;
            if observed.state() != before {
                return Err(corrupt_record());
            }
            let mut sealing = before.clone();
            sealing.phase =
                replication_types::configuration::installed::RegionalTablePhase::Sealing;
            self.persist_table_state(attempt, (Some(observed), sealing))
                .await?;
            self.records.store(attempt, &[slot]).await?;
            let content = request.content()?;
            let record = SealRecord {
                request: request.clone(),
                content,
                vector: content,
                phase: SealPhase::RecordingCuts(0),
            };
            self.store_table_seal(attempt, &record, None).await?;
            record.status()
        }
        .await;
        settlement.complete(result)
    }
}
