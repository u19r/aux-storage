use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, ReadProtection, Revision, StorageResult};

use super::{HistoryCollectionChange, HistoryCollectionObservation};
use crate::journal::JournalStore;

pub(crate) async fn observe_collection<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
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
                        .observe_history_collection(attempt, &proposal, ReadProtection::Snapshot)
                        .await
                })
            },
        )
        .await
}

pub(super) async fn begin_collection<P: DynamoPort>(
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal.begin_history_collection(attempt, &proposal).await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub(crate) async fn accept_collection<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<ControlProposal>,
    input: (Revision, Arc<HistoryCollectionChange>),
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let proposal = Arc::clone(&proposal);
        let input = (input.0, Arc::clone(&input.1));
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
                                .accept_history_collection(attempt, &proposal, (input.0, &input.1))
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}
