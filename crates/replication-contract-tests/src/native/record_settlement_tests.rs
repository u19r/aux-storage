//! Public native records retain chunk replacement and callback rollback proof.

use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, RecordAddress, RecordChange, RecordExpectation,
    },
};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, ReadProtection, StorageError, StorageResult};

use super::fixture;
use crate::HarnessResult;

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_sqlite_records_when_large_replacement_and_callback_abort_then_bytes_and_atomicity_hold()
-> HarnessResult<()> {
    verify_settlement(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_records_when_large_replacement_and_callback_abort_then_bytes_and_atomicity_hold()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_settlement(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_records_when_large_replacement_and_callback_abort_then_bytes_and_atomicity_hold()
-> HarnessResult<()> {
    verify_settlement(&super::turso::FileTurso::try_new().await?.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_records_when_large_replacement_and_callback_abort_then_bytes_and_atomicity_hold()
-> HarnessResult<()> {
    verify_settlement(&super::rocksdb::RocksDb::try_new().await?.store).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_records_when_large_replacement_and_callback_abort_then_bytes_and_atomicity_hold()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_settlement(&fixture.store).await;
    fixture.finish().await?;
    result
}

async fn verify_settlement<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'a> P::Attempt<'a>: PhysicalRecordAttempt,
{
    let original = record(b"large", 0, &vec![17; 200_000])?;
    let key = original.key.clone();
    let changes = vec![RecordChange::Store {
        expected: RecordExpectation::Absent,
        record: original.clone(),
    }];
    store_records(port, changes).await?;
    let loaded = load_records(port, vec![key.clone()]).await?;
    assert_eq!(
        loaded[0].as_ref().ok_or("large record missing")?.bytes,
        original.bytes
    );
    let small = record(b"large", 1, b"small")?;
    store_records(
        port,
        vec![RecordChange::Store {
            expected: RecordExpectation::Revision(0.into()),
            record: small,
        }],
    )
    .await?;
    let loaded = load_records(port, vec![key.clone()]).await?;
    let loaded = loaded[0].as_ref().ok_or("replacement missing")?;
    assert_eq!(loaded.bytes.as_ref(), b"small");
    assert_eq!(u64::from(loaded.revision), 1);
    verify_callback_rollback(port).await
}

async fn verify_callback_rollback<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'a> P::Attempt<'a>: PhysicalRecordAttempt,
{
    let first = record(b"callback-first", 0, b"unsettled")?;
    let second = record(b"callback-second", 0, b"unsettled")?;
    let keys = vec![first.key.clone(), second.key.clone()];
    let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Absent,
                record: first.clone(),
            }];
            assert_eq!(
                attempt
                    .store_records(&RecordBatch::try_new(&changes, limits()?)?)
                    .await?,
                ConditionalWriteResult::Applied
            );
            let loaded = attempt
                .load_records(&[first.key], ReadProtection::Conflict, limits()?)
                .await?;
            assert_eq!(
                loaded[0]
                    .as_ref()
                    .ok_or_else(|| StorageError::internal("staged record missing"))?
                    .bytes
                    .as_ref(),
                b"unsettled"
            );
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Absent,
                record: second,
            }];
            assert_eq!(
                attempt
                    .store_records(&RecordBatch::try_new(&changes, limits()?)?)
                    .await?,
                ConditionalWriteResult::Applied
            );
            Err::<(), _>(StorageError::internal("record callback aborted"))
        })
    })
    .await;
    let error = result.expect_err("failed callback committed");
    assert_eq!(
        storage_common::diagnostics::storage_error_diagnostic(&error).1,
        "record callback aborted"
    );
    assert!(load_records(port, keys).await?.iter().all(Option::is_none));
    Ok(())
}

fn record(suffix: &[u8], revision: u64, bytes: &[u8]) -> StorageResult<DurableRecord> {
    Ok(DurableRecord {
        key: RecordAddress {
            namespace: "native-record-settlement".into(),
            resource: b"resource".to_vec(),
            generation: 0,
            suffix: suffix.to_vec(),
        }
        .try_into()?,
        revision: revision.into(),
        bytes: Arc::from(bytes),
    })
}

fn limits() -> StorageResult<InfrastructureLimits> {
    InfrastructureLimits::try_new(
        NonZeroUsize::new(1024).unwrap(),
        NonZeroUsize::new(256 * 1024).unwrap(),
        NonZeroUsize::new(512 * 1024).unwrap(),
        NonZeroUsize::new(64).unwrap(),
    )
}

async fn store_records<P>(port: &P, changes: Vec<RecordChange>) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'a> P::Attempt<'a>: PhysicalRecordAttempt,
{
    let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            attempt
                .store_records(&RecordBatch::try_new(&changes, limits()?)?)
                .await
        })
    })
    .await?;
    assert_eq!(result, ConditionalWriteResult::Applied);
    Ok(())
}

async fn load_records<P>(
    port: &P,
    keys: Vec<infrastructure_provider::record::DurableRecordKey>,
) -> HarnessResult<Vec<Option<DurableRecord>>>
where
    P: DynamoPort + 'static,
    for<'a> P::Attempt<'a>: PhysicalRecordAttempt,
{
    Ok(
        fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(&keys, ReadProtection::Snapshot, limits()?)
                    .await
            })
        })
        .await?,
    )
}
