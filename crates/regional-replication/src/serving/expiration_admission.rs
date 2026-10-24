use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::{
    maintenance::{
        ExpirationDelete, ExpirationEffectPolicy, ExpirationPort, ExpirationTargetAdmission,
        accept_expiration_item_deletes, prepare_expiration_item_deletes,
    },
    prepare_accepted_delete,
};
use storage_types::{BoundTableInfo, StorageResult, TimestampMillis};

use crate::{
    journal::{JournalStore, origin::capture_expiration_deletions},
    native_record::corrupt_record,
};

#[derive(Clone)]
pub(super) struct RegionalExpirationAdmission {
    pub(super) journal: Arc<JournalStore>,
    pub(super) codec: Arc<CanonicalJsonCodec>,
}

impl<A: PhysicalRecordAttempt> ExpirationTargetAdmission<A> for RegionalExpirationAdmission {
    async fn admits_target(&self, attempt: &mut A, table: &BoundTableInfo) -> StorageResult<bool> {
        self.journal.admits_expiration(attempt, table).await
    }
}

impl<A: ExpirationPort + PhysicalRecordAttempt> ExpirationEffectPolicy<A>
    for RegionalExpirationAdmission
{
    async fn delete_items(
        &self,
        attempt: &mut A,
        table: Arc<BoundTableInfo>,
        items: Vec<ExpirationDelete>,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let configured = self.journal.load_capture_state(attempt, &table).await?;
            let Some(configured) = configured
                .as_ref()
                .filter(|state| state.state().captures_origin())
            else {
                return prepare_expiration_item_deletes(attempt, table, items, captured_at).await;
            };
            let accepted = accept_expiration_item_deletes(attempt, table, items)
                .await?
                .collect::<StorageResult<Vec<_>>>()?;
            let wall_ms =
                u64::try_from(captured_at.timestamp_millis()).map_err(|_| corrupt_record())?;
            let provenance = capture_expiration_deletions(
                &self.journal,
                attempt,
                (configured, &accepted, wall_ms),
                &self.codec,
            )
            .await?;
            if accepted.len() != provenance.len() {
                return Err(corrupt_record());
            }
            for (item, winner) in accepted.into_iter().zip(provenance) {
                prepare_accepted_delete(attempt, &item, captured_at, winner).await?;
            }
            attempt.flush_dynamo_write().await?;
            self.journal.require_acceptance_capacity(attempt).await
        }
        .await;
        settlement.complete(result)
    }
}
