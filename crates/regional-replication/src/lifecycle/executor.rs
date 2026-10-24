use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{SealRequest, SealStatus};
use crate::journal::JournalStore;

pub async fn advance_table_seal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<SealRequest>,
) -> StorageResult<SealStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move { journal.advance_table_seal(attempt, &request).await })
                    },
                )
                .await
        }
    })
    .await
}

pub async fn observe_table_seal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<SealRequest>,
) -> StorageResult<Option<SealStatus>>
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
                Box::pin(async move { journal.observe_table_seal(attempt, &request).await })
            },
        )
        .await
}

pub async fn cancel_table_seal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<SealRequest>,
) -> StorageResult<SealStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move { journal.cancel_table_seal(attempt, &request).await })
                    },
                )
                .await
        }
    })
    .await
}

pub async fn load_sealed_table_cuts<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<SealRequest>,
    after: u32,
) -> StorageResult<super::SealVectorPage>
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
                Box::pin(async move {
                    journal
                        .load_sealed_table_cuts(attempt, &request, after)
                        .await
                })
            },
        )
        .await
}
