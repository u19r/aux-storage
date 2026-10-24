use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{
    BaselineCopyPage, CommittedBaselinePage, HiddenTargetRequest, HiddenTargetReservation,
    TargetBootstrapDefinition,
};
use crate::journal::JournalStore;

pub async fn reserve_hidden_target<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HiddenTargetRequest>,
) -> StorageResult<HiddenTargetReservation>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let cache = port.metadata_cache();
    let name = request.target.name.clone();
    let result = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Schema,
                    move |attempt, now| {
                        Box::pin(async move {
                            journal.reserve_hidden_target(attempt, &request, now).await
                        })
                    },
                )
                .await
        }
    })
    .await;
    cache.invalidate(&name);
    result
}

pub async fn reconcile_hidden_target<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HiddenTargetRequest>,
) -> StorageResult<Option<HiddenTargetReservation>>
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
                Box::pin(async move { journal.observe_hidden_target(attempt, &request).await })
            },
        )
        .await
}

pub struct BaselineCopyExecution {
    pub target: Arc<HiddenTargetRequest>,
    pub definition: Arc<TargetBootstrapDefinition>,
    pub page: Arc<BaselineCopyPage>,
    pub codec: Arc<CanonicalJsonCodec>,
}

pub async fn copy_baseline_page<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<BaselineCopyExecution>,
) -> StorageResult<CommittedBaselinePage>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let execution = Arc::clone(&execution);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .copy_baseline_page(
                                    attempt,
                                    &execution.target,
                                    (&execution.definition, &execution.page, &execution.codec),
                                )
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub async fn reconcile_baseline_page<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<BaselineCopyExecution>,
) -> StorageResult<Option<CommittedBaselinePage>>
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
                        .observe_baseline_page(
                            attempt,
                            &execution.target,
                            (&execution.definition, &execution.page),
                        )
                        .await
                })
            },
        )
        .await
}

pub async fn cancel_hidden_target<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HiddenTargetRequest>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let cache = port.metadata_cache();
    let name = request.target.name.clone();
    let result = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Schema,
                    move |attempt, now| {
                        Box::pin(async move {
                            journal.cancel_hidden_target(attempt, &request, now).await
                        })
                    },
                )
                .await
        }
    })
    .await;
    cache.invalidate(&name);
    result
}

/// False includes absence and is unresolved; only true proves cancellation.
pub async fn reconcile_hidden_cancellation<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HiddenTargetRequest>,
) -> StorageResult<bool>
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
                Box::pin(
                    async move { journal.observe_hidden_cancellation(attempt, &request).await },
                )
            },
        )
        .await
}
