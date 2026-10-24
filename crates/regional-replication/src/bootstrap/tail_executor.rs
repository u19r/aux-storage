use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{event::interval::ClosedSourceInterval, regional_import::RegionalImport};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, ReadProtection, StorageResult};

use super::{BootstrapTailScope, tail_scope::BootstrapTailExecution};
use crate::{delivery::load_bounded_frame, journal::JournalStore, native_record::corrupt_record};

pub(super) async fn plan_bootstrap_tail<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<BootstrapTailScope>,
    configuration: (Arc<CanonicalJsonCodec>, Arc<WorkLimits>),
) -> StorageResult<Option<RegionalImport>>
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
                    let progress = journal
                        .require_bootstrap_tail(attempt, &scope, ReadProtection::Snapshot)
                        .await?;
                    if progress.through() >= scope.cut.through {
                        return Ok(None);
                    }
                    let delivery = scope.delivery()?;
                    let range =
                        ClosedSourceInterval::try_from((progress.through(), scope.cut.through))
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

pub(super) async fn record_bootstrap_tail<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<BootstrapTailExecution>,
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
                                .record_bootstrap_tail(
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
