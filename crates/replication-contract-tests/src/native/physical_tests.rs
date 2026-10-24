use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::{
    batch::RecordBatch,
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
        RecordExpectation,
    },
};
use storage_provider::DynamoPort;
use storage_types::{
    AttemptMode, InfrastructureContext, InfrastructureResultKind, ReadProtection, Revision,
    StorageEnum, StorageError, StorageResult,
};

use super::fixture;
use crate::HarnessResult;

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_native_writer_lock_when_catalog_begin_fails_then_conflict_skips_callback_and_preserves_records()
-> HarnessResult<()> {
    use sql_backend::{
        SqlAttempt as _, SqlBackend as _, SqlStatement, SqlStore, sqlite::SqliteBackend,
    };

    let fixture = fixture::FileSqlite::try_new().await?;
    let backend = SqliteBackend::initialize(&fixture.path).await?;
    backend
        .with_schema_attempt(|attempt| {
            Box::pin(async move {
                attempt
                    .query(
                        SqlStatement::new("PRAGMA busy_timeout = 0", vec![]),
                        ReadProtection::Snapshot,
                        NonZeroUsize::new(128).expect("pragma page"),
                    )
                    .await?;
                Ok(())
            })
        })
        .await?;
    let store = SqlStore::initialize_schema(backend).await?;
    let original = key(1, b"begin-conflict".to_vec())?;
    seed_records(&store, vec![insert(original.clone(), b"original")]).await?;
    let locker = rusqlite::Connection::open(&fixture.path)?;
    locker.execute_batch("BEGIN IMMEDIATE")?;
    let outcome: StorageResult<()> = fixture::with_records(&store, AttemptMode::ReadWrite, |_| {
        Box::pin(async { panic!("callback must not start after failed native BEGIN") })
    })
    .await;
    assert!(
        matches!(outcome.as_ref(), Err(error) if matches!(error.as_ref(),
        StorageEnum::TransactionConflict { .. }))
    );
    let observed = load_records(&store, vec![original.clone()]).await?;
    assert_eq!(
        b"original",
        observed[0]
            .as_ref()
            .expect("original record")
            .bytes
            .as_ref()
    );
    locker.execute_batch("ROLLBACK")?;
    let fresh = key(1, b"after-begin-conflict".to_vec())?;
    seed_records(&store, vec![insert(fresh.clone(), b"settled")]).await?;
    let observed = load_records(&store, vec![fresh]).await?;
    assert_eq!(
        b"settled",
        observed[0].as_ref().expect("fresh record").bytes.as_ref()
    );
    Ok(())
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_when_a_batch_conflicts_then_even_callback_success_commits_no_batch_effects()
-> HarnessResult<()> {
    verify_atomic_conflict(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_when_a_batch_conflicts_then_even_callback_success_commits_no_batch_effects()
-> HarnessResult<()> {
    verify_atomic_conflict(&fixture::FileSqlite::try_new().await?.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_when_a_batch_conflicts_then_even_callback_success_commits_no_batch_effects()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_atomic_conflict(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_pages_when_continuations_replay_and_old_generation_is_deleted_then_scope_is_exact()
-> HarnessResult<()> {
    verify_bounded_pages(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_pages_when_continuations_replay_and_old_generation_is_deleted_then_scope_is_exact()
-> HarnessResult<()> {
    verify_bounded_pages(&fixture::FileSqlite::try_new().await?.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_pages_when_continuations_replay_and_old_generation_is_deleted_then_scope_is_exact()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_bounded_pages(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_unsigned_addresses_when_paged_then_order_is_unsigned_and_revision_cannot_wrap()
-> HarnessResult<()> {
    verify_unsigned_addresses(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_unsigned_addresses_when_paged_then_order_is_unsigned_and_revision_cannot_wrap()
-> HarnessResult<()> {
    verify_unsigned_addresses(&fixture::FileSqlite::try_new().await?.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_unsigned_addresses_when_paged_then_order_is_unsigned_and_revision_cannot_wrap()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_unsigned_addresses(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_native_callbacks_when_the_caller_cancels_then_supervised_settlement_preserves_commit_or_rollback()
-> HarnessResult<()> {
    verify_callback_settlement(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_native_callbacks_when_the_caller_cancels_then_supervised_settlement_preserves_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    verify_callback_settlement(&fixture.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_native_callbacks_when_the_caller_cancels_then_supervised_settlement_preserves_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_callback_settlement(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_when_a_batch_conflicts_then_even_callback_success_commits_no_batch_effects()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_atomic_conflict(&fixture.store).await
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_pages_when_continuations_replay_and_old_generation_is_deleted_then_scope_is_exact()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_bounded_pages(&fixture.store).await
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_unsigned_addresses_when_paged_then_order_is_unsigned_and_revision_cannot_wrap()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_unsigned_addresses(&fixture.store).await
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_native_callbacks_when_the_caller_cancels_then_supervised_settlement_preserves_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_callback_settlement(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_when_a_batch_conflicts_then_even_callback_success_commits_no_batch_effects()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_atomic_conflict(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_pages_when_continuations_replay_and_old_generation_is_deleted_then_scope_is_exact()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_bounded_pages(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_unsigned_addresses_when_paged_then_order_is_unsigned_and_revision_cannot_wrap()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_unsigned_addresses(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_native_callbacks_when_the_caller_cancels_then_supervised_settlement_preserves_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_callback_settlement(&fixture.store).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_when_a_batch_conflicts_then_even_callback_success_commits_no_batch_effects()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_atomic_conflict(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_pages_when_continuations_replay_and_old_generation_is_deleted_then_scope_is_exact()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_bounded_pages(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_unsigned_addresses_when_paged_then_order_is_unsigned_and_revision_cannot_wrap()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_unsigned_addresses(&fixture.store).await;
    fixture.finish().await?;
    result
}

// Caller cancellation leaves the existing native supervisor responsible for
// settlement. A known existing row supplies a native barrier, not a lease.
async fn verify_callback_settlement<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let failed = key(1, b"callback-failure".to_vec())?;
    let target = failed.clone();
    let outcome: StorageResult<()> =
        fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let changes = [insert(target, b"unsettled")];
                assert_eq!(
                    ConditionalWriteResult::Applied,
                    attempt
                        .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                        .await?
                );
                Err(callback_failure())
            })
        })
        .await;
    assert!(
        matches!(outcome.as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::ProtectedConflict { context: InfrastructureContext::AttemptSettlement })))
    );
    assert!(load_records(port, vec![failed]).await?[0].is_none());
    seed_records(
        port,
        vec![insert(key(1, b"settlement-barrier".to_vec())?, b"existing")],
    )
    .await?;
    verify_caller_cancellation(port, false).await?;
    verify_caller_cancellation(port, true).await
}

async fn verify_caller_cancellation<P>(port: &P, should_commit: bool) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let suffix = if should_commit {
        b"canceled-commit".as_slice()
    } else {
        b"canceled-rollback".as_slice()
    };
    let target = key(1, suffix.to_vec())?;
    let effect = target.clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::clone(&release);
    let (started, observed) = tokio::sync::oneshot::channel();
    {
        let caller = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(
                        &[key(1, b"settlement-barrier".to_vec())?],
                        ReadProtection::Conflict,
                        fixture::limits()?,
                    )
                    .await?;
                let changes = [insert(effect, b"settled")];
                assert_eq!(
                    ConditionalWriteResult::Applied,
                    attempt
                        .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                        .await?
                );
                started
                    .send(())
                    .map_err(|_| StorageError::internal("cancellation observer stopped"))?;
                resume.notified().await;
                if should_commit {
                    Ok(())
                } else {
                    Err(callback_failure())
                }
            })
        });
        tokio::pin!(caller);
        tokio::select! {
            result = &mut caller => { result?; return Err("held native callback settled before caller cancellation".into()); }
            result = tokio::time::timeout(std::time::Duration::from_secs(5), observed) => result??,
        }
    }
    release.notify_one();
    await_native_settlement(port).await?;
    let observed = load_records(port, vec![target]).await?;
    assert_eq!(should_commit, observed[0].is_some());
    if let Some(record) = &observed[0] {
        assert_eq!(b"settled", record.bytes.as_ref());
    }
    Ok(())
}

async fn await_native_settlement<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture::with_records(port, AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                let existing = attempt
                    .load_records(
                        &[key(1, b"settlement-barrier".to_vec())?],
                        ReadProtection::Conflict,
                        fixture::limits()?,
                    )
                    .await?;
                assert_eq!(
                    b"existing",
                    existing[0]
                        .as_ref()
                        .expect("known settlement row")
                        .bytes
                        .as_ref()
                );
                Ok(())
            })
        }),
    )
    .await??;
    Ok(())
}

fn callback_failure() -> StorageError {
    InfrastructureResultKind::ProtectedConflict {
        context: InfrastructureContext::AttemptSettlement,
    }
    .into()
}

async fn verify_atomic_conflict<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let original = key(1, b"original".to_vec())?;
    let new = key(1, b"new".to_vec())?;
    seed_records(port, vec![insert(original.clone(), b"original")]).await?;
    verify_point_loads(port, original.clone()).await?;
    let changes = vec![
        insert(new.clone(), b"partial"),
        insert(original.clone(), b"wrong"),
    ];
    let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            attempt
                .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                .await
        })
    })
    .await?;
    assert!(matches!(result, ConditionalWriteResult::Conflict { .. }));
    let values = load_records(port, vec![new, original.clone()]).await?;
    assert!(values[0].is_none());
    assert_eq!(
        b"original",
        values[1]
            .as_ref()
            .ok_or("conflict lost original")?
            .bytes
            .as_ref()
    );
    let changes = vec![
        RecordChange::Delete {
            expected: 0.into(),
            key: original.clone(),
        },
        insert(original.clone(), b"duplicate"),
    ];
    assert!(
        matches!((RecordBatch::try_new(&changes, fixture::limits()?)).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { .. })))
    );
    for (expected, revision) in [
        (RecordExpectation::Revision(0.into()), 2),
        (RecordExpectation::Revision(0.into()), 0),
        (RecordExpectation::Absent, 1),
    ] {
        let invalid = [RecordChange::Store {
            expected,
            record: DurableRecord {
                key: original.clone(),
                revision: revision.into(),
                bytes: Arc::from(&b"invalid-revision"[..]),
            },
        }];
        assert!(
            matches!((RecordBatch::try_new(&invalid, fixture::limits()?)).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { context: InfrastructureContext::Revision })))
        );
    }
    assert_eq!(
        b"original",
        load_records(port, vec![original]).await?[0]
            .as_ref()
            .ok_or("invalid batch changed original")?
            .bytes
            .as_ref()
    );
    Ok(())
}

async fn verify_point_loads<P>(port: &P, first: DurableRecordKey) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let second = key(1, b"second".to_vec())?;
    let missing = key(1, b"missing".to_vec())?;
    seed_records(port, vec![insert(second.clone(), b"second")]).await?;
    let observed = load_records(port, vec![second, missing.clone(), first.clone(), first]).await?;
    assert_eq!(
        b"second",
        observed[0].as_ref().expect("second record").bytes.as_ref()
    );
    assert!(observed[1].is_none());
    for record in &observed[2..] {
        assert_eq!(
            b"original",
            record
                .as_ref()
                .expect("repeated first record")
                .bytes
                .as_ref()
        );
    }
    assert!(load_records(port, vec![]).await?.is_empty());
    let limits = fixture::limits()?;
    let small_key = infrastructure_provider::limits::InfrastructureLimits::try_new(
        NonZeroUsize::new(64).expect("key bound"),
        limits.max_record_bytes(),
        limits.max_bytes(),
        limits.max_records(),
    )?;
    let nonzero = |value| NonZeroUsize::new(value).expect("nonzero aggregate budget");
    let aggregate_limits = infrastructure_provider::limits::InfrastructureLimits::try_new(
        nonzero(64),
        nonzero(64),
        nonzero(128),
        nonzero(8),
    )?;
    let aggregate_key = DurableRecordKey::try_from(RecordAddress {
        namespace: "native".into(),
        resource: vec![1; 32],
        generation: u64::MAX,
        suffix: vec![2; 10],
    })?;
    assert_eq!(64, aggregate_limits.validate_key(&aggregate_key)?);
    let keys = vec![aggregate_key.clone(); 2];
    let equality = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            attempt
                .load_records(&keys, ReadProtection::Snapshot, aggregate_limits)
                .await
        })
    })
    .await?;
    assert_eq!(2, equality.len());
    assert!(equality.iter().all(Option::is_none));
    for (keys, limits) in [
        (vec![missing; limits.max_records().get() + 1], limits),
        (vec![key(1, vec![0; 64])?], small_key),
        (vec![aggregate_key; 3], aggregate_limits),
    ] {
        let effect = key(1, b"invalid-read-effect".to_vec())?;
        let staged = effect.clone();
        let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let changes = [insert(staged, b"must-roll-back")];
                assert_eq!(
                    ConditionalWriteResult::Applied,
                    attempt
                        .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                        .await?
                );
                attempt
                    .load_records(&keys, ReadProtection::Conflict, limits)
                    .await
            })
        })
        .await;
        assert!(matches!(result, Err(error) if matches!(error.as_ref(),
            StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. }))));
        assert!(load_records(port, vec![effect]).await?[0].is_none());
    }
    Ok(())
}

// N18: count/byte bounds, replayed exclusive continuations, and scoped cleanup.
async fn verify_bounded_pages<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let suffixes = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![1, 0],
        vec![254],
    ];
    let originals = suffixes
        .into_iter()
        .map(|suffix| key(1, suffix))
        .collect::<StorageResult<Vec<_>>>()?;
    let current = key(2, vec![1])?;
    let mut foreign_address = RecordAddress::from(key(1, vec![1])?);
    foreign_address.namespace = "Native-physical".into();
    let foreign = DurableRecordKey::try_from(foreign_address)?;
    let mut changes = originals
        .iter()
        .map(|key| insert(key.clone(), b"old"))
        .collect::<Vec<_>>();
    changes.extend([
        insert(current.clone(), b"current"),
        insert(foreign.clone(), b"foreign"),
    ]);
    seed_records(port, changes).await?;
    let range = BoundedRecordRange::try_new(
        key(1, vec![])?,
        key(1, vec![255])?,
        NonZeroUsize::new(2).expect("page count"),
    )?;
    assert_eq!(originals, collect_pages(port, range.clone()).await?);
    let tiny = RecordPageRequest {
        range: range.clone(),
        after: None,
        max_bytes: NonZeroUsize::MIN,
        protection: ReadProtection::Snapshot,
    };
    let rejected = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move { attempt.scan_records(tiny, fixture::limits()?).await })
    })
    .await;
    assert!(
        matches!((rejected).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. })))
    );
    let invalid = RecordPageRequest {
        range: range.clone(),
        after: Some(current.clone()),
        max_bytes: fixture::limits()?.max_bytes(),
        protection: ReadProtection::Conflict,
    };
    assert!(
        matches!((invalid.validate(fixture::limits()?)).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. })))
    );
    assert_eq!(originals.len(), delete_pages(port, range.clone()).await?);
    assert!(collect_pages(port, range.clone()).await?.is_empty());
    assert_eq!(0, delete_pages(port, range).await?);
    let retained = load_records(port, vec![current, foreign]).await?;
    assert_eq!(
        b"current",
        retained[0]
            .as_ref()
            .ok_or("stale cleanup deleted current generation")?
            .bytes
            .as_ref()
    );
    assert_eq!(
        b"foreign",
        retained[1]
            .as_ref()
            .ok_or("cleanup crossed namespace")?
            .bytes
            .as_ref()
    );
    Ok(())
}

// N20: all u64 generations survive point loads; BE suffixes page in numeric
// order.
async fn verify_unsigned_addresses<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let values = [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX];
    let addresses = values
        .map(|value| key(value, value.to_be_bytes().to_vec()))
        .into_iter()
        .collect::<StorageResult<Vec<_>>>()?;
    seed_records(
        port,
        addresses
            .iter()
            .map(|key| insert(key.clone(), b"generation"))
            .collect(),
    )
    .await?;
    for (expected, record) in values.into_iter().zip(load_records(port, addresses).await?) {
        let record = record.ok_or("unsigned generation narrowed")?;
        assert_eq!(expected, record.key.as_ref().generation);
        assert_eq!(Revision::from(0), record.revision);
    }
    let ordered = values
        .map(|value| key(7, value.to_be_bytes().to_vec()))
        .into_iter()
        .collect::<StorageResult<Vec<_>>>()?;
    seed_records(
        port,
        ordered
            .iter()
            .map(|key| insert(key.clone(), b"suffix"))
            .collect(),
    )
    .await?;
    let range = BoundedRecordRange::try_new(
        key(7, vec![])?,
        key(7, vec![255; 9])?,
        NonZeroUsize::new(2).expect("page count"),
    )?;
    assert_eq!(ordered, collect_pages(port, range).await?);
    let maximum = Revision::from(u64::MAX);
    assert!(
        matches!((maximum.checked_next()).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. })))
    );
    let target = key(9, b"overflow".to_vec())?;
    let overflow = vec![RecordChange::Store {
        expected: RecordExpectation::Revision(maximum),
        record: DurableRecord {
            key: target.clone(),
            revision: 0.into(),
            bytes: Arc::from(&b"wrapped"[..]),
        },
    }];
    assert!(
        matches!((RecordBatch::try_new(&overflow, fixture::limits()?)).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. })))
    );
    assert!(load_records(port, vec![target]).await?[0].is_none());
    Ok(())
}

async fn collect_pages<P>(
    port: &P,
    range: BoundedRecordRange,
) -> HarnessResult<Vec<DurableRecordKey>>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let next = page_request(range.clone(), after.clone());
        let replay = page_request(range.clone(), after);
        let page = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { attempt.scan_records(next, fixture::limits()?).await })
        })
        .await?;
        let repeated = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { attempt.scan_records(replay, fixture::limits()?).await })
        })
        .await?;
        let keys = page
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            repeated
                .records
                .iter()
                .map(|record| record.key.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(page.continuation, repeated.continuation);
        assert!(page.records.len() <= 2);
        let limits = fixture::limits()?;
        let bytes = page
            .records
            .iter()
            .map(|record| limits.validate_record(record))
            .collect::<StorageResult<Vec<_>>>()?;
        assert!(bytes.into_iter().sum::<usize>() <= 4096);
        seen.extend(keys);
        if page.continuation.is_none() {
            return Ok(seen);
        }
        after = Some(
            page.continuation
                .ok_or("nonterminal page lost continuation")?,
        );
        assert!(seen.len() <= 16, "continuation repeated records");
    }
}

async fn delete_pages<P>(port: &P, range: BoundedRecordRange) -> HarnessResult<usize>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let mut deleted = 0;
    loop {
        let mut request = page_request(range.clone(), None);
        request.protection = ReadProtection::Conflict;
        let page = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let page = attempt.scan_records(request, fixture::limits()?).await?;
                let changes = page
                    .records
                    .iter()
                    .map(|record| RecordChange::Delete {
                        key: record.key.clone(),
                        expected: record.revision,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    ConditionalWriteResult::Applied,
                    attempt
                        .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                        .await?
                );
                Ok(page)
            })
        })
        .await?;
        assert!(page.records.len() <= 2);
        deleted += page.records.len();
        if page.continuation.is_none() {
            return Ok(deleted);
        }
        assert!(deleted <= 16, "cleanup repeated records");
    }
}

fn page_request(range: BoundedRecordRange, after: Option<DurableRecordKey>) -> RecordPageRequest {
    RecordPageRequest {
        range,
        after,
        max_bytes: NonZeroUsize::new(4096).expect("page bytes"),
        protection: ReadProtection::Snapshot,
    }
}

fn key(generation: u64, suffix: Vec<u8>) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: "native-physical".into(),
        resource: b"resource\0one".to_vec(),
        generation,
        suffix,
    }
    .try_into()
}

fn insert(key: DurableRecordKey, bytes: &[u8]) -> RecordChange {
    RecordChange::Store {
        expected: RecordExpectation::Absent,
        record: DurableRecord {
            key,
            revision: 0.into(),
            bytes: Arc::from(bytes),
        },
    }
}

async fn seed_records<P>(port: &P, changes: Vec<RecordChange>) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            attempt
                .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                .await
        })
    })
    .await?;
    assert_eq!(ConditionalWriteResult::Applied, result);
    Ok(())
}

async fn load_records<P>(
    port: &P,
    keys: Vec<DurableRecordKey>,
) -> HarnessResult<Vec<Option<DurableRecord>>>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    Ok(
        fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(
                        &keys,
                        storage_types::ReadProtection::Snapshot,
                        fixture::limits()?,
                    )
                    .await
            })
        })
        .await?,
    )
}
