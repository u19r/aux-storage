use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use crate::journal::JournalStore;

pub(super) async fn select_pending_work<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
) -> StorageResult<Option<super::pending::PendingControlWork>>
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
                        Box::pin(async move { journal.select_pending_work(attempt).await })
                    },
                )
                .await
        }
    })
    .await
}

pub(super) async fn retire_pending_control<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let proposal = Arc::clone(&proposal);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
                    move |attempt, _| {
                        Box::pin(
                            async move { journal.retire_pending_control(attempt, &proposal).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}
