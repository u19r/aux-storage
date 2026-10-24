use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use crate::journal::JournalStore;

pub(super) async fn initialize_journal<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    physical_identity: [u8; 16],
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    loop {
        let ready = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
            let journal = Arc::clone(&journal);
            async move {
                port.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                    Box::pin(
                        async move { journal.register_layout(attempt, physical_identity).await },
                    )
                })
                .await
            }
        })
        .await;
        match ready {
            Ok(true) => break,
            Ok(false) => continue,
            Err(error) if error.is_commit_unknown() => {
                let observer = Arc::clone(&journal);
                let observed = port
                    .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                        Box::pin(async move {
                            observer
                                .observe_initialized_layout(attempt, physical_identity)
                                .await
                        })
                    })
                    .await;
                if !matches!(observed, Ok(true)) {
                    return Err(error);
                }
                break;
            }
            Err(error) => return Err(error),
        }
    }
    qualify_initialized_partitions(port, Arc::clone(&journal)).await?;
    initialize_archive(port, journal, physical_identity).await
}

async fn qualify_initialized_partitions<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let mut after = crate::journal::PartitionQualificationCursor::default();
    loop {
        let observer = Arc::clone(&journal);
        let next = port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    observer
                        .observe_initialized_partition_page(attempt, after)
                        .await
                })
            })
            .await?;
        let Some(next) = next else {
            return Ok(());
        };
        after = next;
    }
}

async fn initialize_archive<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    physical_identity: [u8; 16],
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let result = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        async move {
            port.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move { journal.bind_archive(attempt, physical_identity).await })
            })
            .await
        }
    })
    .await;
    match result {
        Err(error) if error.is_commit_unknown() => {
            let observed = port
                .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                    Box::pin(async move {
                        journal
                            .observe_archive_binding(attempt, physical_identity)
                            .await
                    })
                })
                .await;
            if matches!(observed, Ok(true)) {
                Ok(())
            } else {
                Err(error)
            }
        }
        result => result,
    }
}
