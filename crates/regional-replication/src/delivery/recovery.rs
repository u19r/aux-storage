use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use crate::journal::{DeliverySelection, JournalStore};

/// Commit the round-robin cursor before peer I/O so a failed destination cannot
/// prevent other tables or destinations from making progress.
pub(super) async fn select_pending_delivery<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
) -> StorageResult<DeliverySelection>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move { journal.select_pending_delivery(attempt).await })
                    },
                )
                .await
        }
    })
    .await
}
