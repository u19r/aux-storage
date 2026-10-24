use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{HistoryCaptureRequest, HistoryCaptureStatus, HistoryCaptureVectorPage};
use crate::journal::JournalStore;

pub async fn advance_history_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
) -> StorageResult<HistoryCaptureStatus>
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
                        Box::pin(
                            async move { journal.advance_history_capture(attempt, &request).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}

pub async fn observe_history_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
) -> StorageResult<Option<HistoryCaptureStatus>>
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
                Box::pin(async move { journal.observe_history_capture(attempt, &request).await })
            },
        )
        .await
}

pub async fn load_history_capture_cuts<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
    after: u32,
) -> StorageResult<HistoryCaptureVectorPage>
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
                        .load_history_capture_cuts(attempt, &request, after)
                        .await
                })
            },
        )
        .await
}

pub async fn cancel_history_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
) -> StorageResult<HistoryCaptureStatus>
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
                        Box::pin(
                            async move { journal.cancel_history_capture(attempt, &request).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}

pub async fn release_history_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
) -> StorageResult<HistoryCaptureStatus>
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
                        Box::pin(
                            async move { journal.release_history_capture(attempt, &request).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}

pub async fn load_history_capture_definition<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
) -> StorageResult<super::HistoryCaptureDefinition>
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
                        .load_history_capture_definition(attempt, &request)
                        .await
                })
            },
        )
        .await
}

/// Protected descriptive reads use a writable-capability attempt to retain
/// their native catalog/root locks. They stage no record or item mutations.
pub async fn load_retired_history_page<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
    scope: (
        replication_types::configuration::installed::RegionalSourceBinding,
        u32,
    ),
) -> StorageResult<super::RetiredHistoryPage>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        let scope = scope.clone();
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
                                .load_retired_history_page(attempt, &request, (&scope.0, scope.1))
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub async fn load_retained_baseline_page<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
    input: (
        Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
        Arc<super::RetainedBaselineScope>,
    ),
) -> StorageResult<super::RetainedBaselinePage>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        let input = (Arc::clone(&input.0), Arc::clone(&input.1));
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
                                .load_retained_baseline_page(
                                    attempt,
                                    &request,
                                    (&input.0, &input.1),
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

pub async fn load_retired_source_catalog<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
    scope: Arc<super::HistoryCatalogScope>,
) -> StorageResult<super::RetiredHistoryCatalogPage>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
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
                                .load_retired_source_catalog(attempt, &request, &scope)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub async fn load_retained_baseline_definition<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HistoryCaptureRequest>,
) -> StorageResult<super::RetainedBaselineDefinition>
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .load_retained_baseline_definition(
                                    attempt,
                                    &request,
                                    storage_types::ReadProtection::Snapshot,
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
