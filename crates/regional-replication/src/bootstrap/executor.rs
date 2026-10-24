use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::configuration::proposal::ControlProposal;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, ItemTombstoneScanAttempt, PrimaryStateScanAttempt,
};
use storage_types::{AttemptMode, StorageResult};

use super::SourceCaptureStatus;
use crate::journal::JournalStore;

#[derive(Clone, Copy)]
enum CaptureAction {
    Begin,
    Cancel,
    Release,
}

pub async fn begin_source_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<SourceCaptureStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    execute_capture_change(port, journal, (proposal, CaptureAction::Begin)).await
}

pub async fn cancel_source_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<SourceCaptureStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    execute_capture_change(port, journal, (proposal, CaptureAction::Cancel)).await
}

pub(crate) async fn release_source_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<SourceCaptureStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    execute_capture_change(port, journal, (proposal, CaptureAction::Release)).await
}

async fn execute_capture_change<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (Arc<ControlProposal>, CaptureAction),
) -> StorageResult<SourceCaptureStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let (proposal, action) = input;
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move {
                            match action {
                                CaptureAction::Begin => {
                                    journal.begin_source_capture(attempt, &proposal).await
                                }
                                CaptureAction::Cancel => {
                                    journal.cancel_source_capture(attempt, &proposal).await
                                }
                                CaptureAction::Release => {
                                    journal.release_source_capture(attempt, &proposal).await
                                }
                            }
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub async fn advance_source_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
    encoding: (
        Arc<CanonicalJsonCodec>,
        Arc<replication_provider::runtime_configuration::limits::WorkLimits>,
    ),
) -> StorageResult<SourceCaptureStatus>
where
    for<'native> P::Attempt<'native>:
        PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt,
{
    let (codec, limits) = encoding;
    let cache = port.metadata_cache();
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let proposal = Arc::clone(&proposal);
        let codec = Arc::clone(&codec);
        let limits = Arc::clone(&limits);
        let cache = Arc::clone(&cache);
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
                                .advance_source_capture(
                                    attempt,
                                    &proposal,
                                    (&cache, &codec, &limits),
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

/// A fresh observation reports only durable progress. Absence remains
/// unresolved.
pub async fn reconcile_source_capture<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<Option<SourceCaptureStatus>>
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
                Box::pin(async move { journal.observe_source_capture(attempt, &proposal).await })
            },
        )
        .await
}

pub async fn read_source_capture_cut<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
    partition: u32,
) -> StorageResult<super::SourceCaptureCut>
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
                        .load_source_capture_cut(attempt, &proposal, partition)
                        .await
                })
            },
        )
        .await
}

pub async fn read_source_capture_baselines<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
    input: (
        Option<infrastructure_provider::record::DurableRecordKey>,
        Arc<CanonicalJsonCodec>,
    ),
) -> StorageResult<crate::journal::BaselineAuthorityPage>
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
                        .read_capture_baselines(attempt, &proposal, (input.0, &input.1))
                        .await
                })
            },
        )
        .await
}

pub async fn read_source_capture_definition<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<super::SourceCaptureDefinition>
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
                        .load_source_capture_definition(
                            attempt,
                            &proposal,
                            storage_types::ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
}

pub async fn observe_source_schema<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    table: replication_types::identity::resource::TableIdentity,
) -> StorageResult<super::SourceSchemaObservation>
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
                Box::pin(async move { journal.observe_source_schema(attempt, table).await })
            },
        )
        .await
}

pub async fn read_reenable_history_definition<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
) -> StorageResult<super::ReenableHistoryDefinition>
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
                        .load_reenable_history_definition(
                            attempt,
                            &proposal,
                            storage_types::ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
}

pub async fn read_reenable_history_page<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
    ordinal: u64,
) -> StorageResult<super::ReenableHistoryPage>
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
                        .load_reenable_history_page(
                            attempt,
                            &proposal,
                            ordinal,
                            storage_types::ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
}

/// Read original bytes selected by a frozen, actively held native cut.
pub async fn read_reenable_original_frame<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<super::ReenableOriginalRequest>,
    encoding: (
        Arc<CanonicalJsonCodec>,
        Arc<replication_provider::runtime_configuration::limits::WorkLimits>,
    ),
) -> StorageResult<Option<replication_provider::delivery::frame::SourceFrame>>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::RangeRead,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move {
                    journal
                        .load_reenable_original_frame(
                            attempt,
                            &request,
                            (&encoding.0, &encoding.1),
                            storage_types::ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
}
