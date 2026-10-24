use std::sync::Arc;

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
        RecordExpectation,
    },
};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, ReadProtection, StorageResult};

use super::fixture;
use crate::HarnessResult;

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_record_when_point_protection_is_selected_then_modes_are_enforced()
-> HarnessResult<()> {
    verify_point_protection(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_record_when_point_protection_is_selected_then_modes_are_enforced()
-> HarnessResult<()> {
    verify_point_protection(&fixture::FileSqlite::try_new().await?.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_record_when_point_protection_is_selected_then_modes_are_enforced()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_point_protection(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_record_when_point_protection_is_selected_then_modes_are_enforced()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_point_protection(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_record_when_point_protection_is_selected_then_modes_are_enforced()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_point_protection(&fixture.store).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_record_when_point_protection_is_selected_then_modes_are_enforced()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_point_protection(&fixture.store).await;
    fixture.finish().await?;
    result
}

async fn verify_point_protection<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    store_initial_record(port, record_key(b"fence")?).await?;
    for protection in [
        ReadProtection::Snapshot,
        ReadProtection::Shared,
        ReadProtection::Conflict,
    ] {
        let loaded = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(&[record_key(b"fence")?], protection, fixture::limits()?)
                    .await
            })
        })
        .await?;
        assert_eq!(
            loaded[0].as_ref().ok_or("record missing")?.bytes.as_ref(),
            b"original"
        );
    }
    for protection in [ReadProtection::Shared, ReadProtection::Conflict] {
        let outcome = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(&[record_key(b"fence")?], protection, fixture::limits()?)
                    .await
            })
        })
        .await;
        assert!(
            outcome.is_err(),
            "protected reads require a write-capable native attempt"
        );
    }
    Ok(())
}

fn record_key(suffix: &[u8]) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: "protected-point".into(),
        resource: b"destination".to_vec(),
        generation: 1,
        suffix: suffix.to_vec(),
    }
    .try_into()
}

async fn store_initial_record<P>(port: &P, key: DurableRecordKey) -> StorageResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Absent,
                record: DurableRecord {
                    key,
                    revision: 0.into(),
                    bytes: Arc::from(&b"original"[..]),
                },
            }];
            assert_eq!(
                attempt
                    .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                    .await?,
                ConditionalWriteResult::Applied
            );
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_held_postgres_shared_fence_when_readers_and_takeover_arrive_then_readers_proceed_and_takeover_waits_for_settlement()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_shared_fence(&fixture).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-postgres")]
async fn verify_shared_fence(fixture: &fixture::Postgres) -> HarnessResult<()> {
    let first = fixture.open_independent_store().await?;
    let second = fixture.open_independent_store().await?;
    let successor = fixture.open_independent_store().await?;
    let observer = fixture.connect().await?;
    store_initial_record(&first, record_key(b"fence")?).await?;
    let ready = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let held = hold_shared_fence(&first, Arc::clone(&ready), Arc::clone(&release));
    let contender = async {
        ready.notified().await;
        let result = verify_contenders(&second, &successor, &observer, &release).await;
        if result.is_err() {
            release.notify_one();
        }
        result
    };
    let outcomes = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::join!(held, contender)
    })
    .await;
    release.notify_one();
    let (held_result, contender_result) = outcomes?;
    held_result.map_err(|error| format!("earlier protected attempt failed: {error}"))?;
    contender_result.map_err(|error| format!("successor barrier failed: {error}"))?;
    let loaded = fixture::with_records(&successor, AttemptMode::ReadOnly, |attempt| {
        Box::pin(async move {
            attempt
                .load_records(
                    &[record_key(b"fence")?, record_key(b"earlier-effect")?],
                    ReadProtection::Snapshot,
                    fixture::limits()?,
                )
                .await
        })
    })
    .await?;
    assert_eq!(
        u64::from(loaded[0].as_ref().ok_or("fence missing")?.revision),
        1
    );
    assert_eq!(
        loaded[1]
            .as_ref()
            .ok_or("earlier effect missing")?
            .bytes
            .as_ref(),
        b"original"
    );
    Ok(())
}

#[cfg(feature = "native-postgres")]
async fn hold_shared_fence<P>(
    port: &P,
    ready: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
) -> StorageResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            let loaded = attempt
                .load_records(
                    &[record_key(b"fence")?],
                    ReadProtection::Shared,
                    fixture::limits()?,
                )
                .await?;
            assert_eq!(
                u64::from(loaded[0].as_ref().expect("seeded fence").revision),
                0
            );
            ready.notify_one();
            release.notified().await;
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Absent,
                record: DurableRecord {
                    key: record_key(b"earlier-effect")?,
                    revision: 0.into(),
                    bytes: Arc::from(&b"original"[..]),
                },
            }];
            assert_eq!(
                attempt
                    .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                    .await?,
                ConditionalWriteResult::Applied
            );
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "native-postgres")]
async fn verify_contenders<P>(
    reader: &P,
    successor: &P,
    observer: &tokio_postgres::Client,
    release: &tokio::sync::Notify,
) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture::with_records(reader, AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                let loaded = attempt
                    .load_records(
                        &[record_key(b"fence")?],
                        ReadProtection::Shared,
                        fixture::limits()?,
                    )
                    .await?;
                assert_eq!(
                    u64::from(loaded[0].as_ref().expect("seeded fence").revision),
                    0
                );
                Ok(())
            })
        }),
    )
    .await??;
    let takeover = advance_fence(successor);
    tokio::pin!(takeover);
    tokio::select! {
        outcome = &mut takeover => { outcome?; Err("takeover settled while an earlier protected attempt was held".into()) }
        outcome = wait_for_native_block(observer) => outcome,
    }?;
    release.notify_one();
    match tokio::time::timeout(std::time::Duration::from_secs(5), &mut takeover).await? {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.as_ref(),
                storage_types::StorageEnum::TransactionConflict { .. }
            ) =>
        {
            // Serializable predicate dependencies can abort this native
            // attempt after waiting. Only a known abort permits fresh retry.
            let loaded = fixture::with_records(successor, AttemptMode::ReadOnly, |attempt| {
                Box::pin(async move {
                    attempt
                        .load_records(
                            &[record_key(b"fence")?],
                            ReadProtection::Snapshot,
                            fixture::limits()?,
                        )
                        .await
                })
            })
            .await?;
            assert_eq!(
                u64::from(loaded[0].as_ref().expect("original fence").revision),
                0
            );
            assert_eq!(
                loaded[0].as_ref().expect("original fence").bytes.as_ref(),
                b"original"
            );
            advance_fence(successor).await?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(feature = "native-postgres")]
async fn advance_fence<P>(port: &P) -> StorageResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    fixture::with_records(port, AttemptMode::ReadWrite, |attempt| {
        Box::pin(async move {
            let key = record_key(b"fence")?;
            let loaded = attempt
                .load_records(
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                    fixture::limits()?,
                )
                .await?;
            let current = loaded[0].as_ref().expect("initialized destination fence");
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Revision(current.revision),
                record: DurableRecord {
                    key,
                    revision: current.revision.checked_next()?,
                    bytes: Arc::from(&b"successor"[..]),
                },
            }];
            assert_eq!(
                attempt
                    .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                    .await?,
                ConditionalWriteResult::Applied
            );
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "native-postgres")]
pub(super) async fn wait_for_native_block(observer: &tokio_postgres::Client) -> HarnessResult<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = observer
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE \
                     application_name = current_schema() AND pid <> pg_backend_pid() AND \
                     cardinality(pg_catalog.pg_blocking_pids(pid)) > 0)",
                    &[],
                )
                .await?
                .get(0);
            if blocked {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await?
}
