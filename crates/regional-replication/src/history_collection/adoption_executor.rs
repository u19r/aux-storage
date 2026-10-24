use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, Revision, StorageResult};

use super::{HistoryAdoptionDefinition, HistoryCollectionChange, HistoryCollectionObservation};
use crate::{bootstrap::HiddenTargetRequest, journal::JournalStore};

pub(crate) async fn observe_adoption<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HiddenTargetRequest>,
    definition: Arc<HistoryAdoptionDefinition>,
) -> StorageResult<Option<HistoryCollectionObservation>>
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
                        .observe_target_adoption(attempt, &request, &definition)
                        .await
                })
            },
        )
        .await
}

pub(crate) async fn begin_adoption<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<HiddenTargetRequest>,
    definition: Arc<HistoryAdoptionDefinition>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        let definition = Arc::clone(&definition);
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
                                .begin_target_adoption(attempt, &request, &definition)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub(crate) struct AdoptionAcceptance {
    pub request: Arc<HiddenTargetRequest>,
    pub definition: Arc<HistoryAdoptionDefinition>,
    pub revision: Revision,
    pub change: HistoryCollectionChange,
}

pub(crate) async fn accept_adoption<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: Arc<AdoptionAcceptance>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let input = Arc::clone(&input);
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
                                .accept_target_adoption(
                                    attempt,
                                    &input.request,
                                    (&input.definition, input.revision, &input.change),
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
