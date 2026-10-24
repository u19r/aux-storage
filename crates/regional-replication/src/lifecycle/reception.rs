use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{SealReceptionProgress, SealRequest, SealVectorPage};
use crate::journal::JournalStore;

pub(crate) async fn observe_received_seal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<SealRequest>,
) -> StorageResult<SealReceptionProgress>
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
                Box::pin(async move { journal.observe_received_seal(attempt, &request).await })
            },
        )
        .await
}

pub(crate) async fn receive_sealed_table_cuts<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (Arc<SealRequest>, Arc<SealVectorPage>),
) -> StorageResult<SealReceptionProgress>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let (request, page) = (Arc::clone(&input.0), Arc::clone(&input.1));
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
                                .receive_sealed_table_cuts(attempt, &request, &page)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

/// Known aborts reread the exact incarnation; unknown commits do not issue a
/// cleanup acknowledgment. A later call observes the native Retired root.
pub(crate) async fn retire_removed_table<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<replication_types::configuration::proposal::ControlProposal>,
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
                    move |attempt, now| {
                        Box::pin(async move {
                            journal.retire_removed_table(attempt, &proposal, now).await
                        })
                    },
                )
                .await
        }
    })
    .await
}
