use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::BaselineCopyScope;
use crate::journal::{JournalStore, TransferPhase};

pub(super) async fn load_transfer_phase<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<BaselineCopyScope>,
) -> StorageResult<TransferPhase>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move { journal.load_transfer_phase(attempt, &scope).await })
            },
        )
        .await
}

pub(super) async fn advance_transfer_phase<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<BaselineCopyScope>,
    expected: TransferPhase,
) -> StorageResult<TransferPhase>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let scope = Arc::clone(&scope);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .advance_transfer_phase(attempt, &scope, expected)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}
