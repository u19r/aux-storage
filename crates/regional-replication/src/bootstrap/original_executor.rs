use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{event::interval::ClosedSourceInterval, regional_import::RegionalImport};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{AdoptedOriginalScope, original_scope::AdoptedOriginalExecution};
use crate::{delivery::load_bounded_frame, journal::JournalStore, native_record::corrupt_record};

pub(super) async fn plan_original_traversal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<super::BaselineCopyScope>,
) -> StorageResult<crate::journal::OriginalTraversalWork>
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
                    move |attempt, _| {
                        Box::pin(
                            async move { journal.plan_original_traversal(attempt, &scope).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}

pub(super) async fn advance_original_traversal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<AdoptedOriginalScope>,
) -> StorageResult<()>
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal.advance_original_traversal(attempt, &scope).await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub(super) async fn plan_adopted_original<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<AdoptedOriginalScope>,
    configuration: (Arc<CanonicalJsonCodec>, Arc<WorkLimits>),
) -> StorageResult<Option<RegionalImport>>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let scope = Arc::clone(&scope);
        let configuration = configuration.clone();
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
                            let progress =
                                journal.require_adopted_original(attempt, &scope).await?;
                            if progress.through() >= scope.cut.through {
                                return Ok(None);
                            }
                            let delivery = scope.delivery()?;
                            let range = ClosedSourceInterval::try_from((
                                progress.through(),
                                scope.cut.through,
                            ))
                            .map_err(|_| corrupt_record())?;
                            let frame = load_bounded_frame(
                                &journal,
                                attempt,
                                delivery.source_request(range),
                                (&configuration.0, &configuration.1),
                            )
                            .await
                            .map_err(storage_types::StorageError::from)?;
                            let (manifest, events) = frame.into_parts();
                            RegionalImport::try_new(
                                (vec![delivery.binding()], manifest, events),
                                &*configuration.0,
                            )
                            .map(Some)
                            .map_err(|_| corrupt_record())
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub(super) async fn record_adopted_original<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<AdoptedOriginalExecution>,
) -> StorageResult<replication_provider::delivery::coverage::DeliveryCoverage>
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
                                .record_adopted_original(
                                    attempt,
                                    (&execution.scope, &execution.import, &execution.receipt),
                                    (&execution.codec, &execution.limits),
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
