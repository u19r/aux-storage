//! Explicit IDs persist on fresh Single streams. Activation and exclusion of
//! rows from a previous stream generation remain higher-owner prerequisites.
//! Fixture bounds are passive physical inputs, not committed-prefix witnesses.

use std::num::NonZeroU32;

use storage_types::{
    AttemptMode, StorageError, StorageResult, StreamItemId, StreamName, TimestampMillis,
    UserStreamName,
};
use stream_provider::{
    ReadDirection, StreamAttempt, StreamDataType, StreamItem, StreamOperations, StreamPage,
    StreamPartitioningMode, StreamPort, StreamReadRequest, StreamRowKind,
};

use super::fixture;
use crate::HarnessResult;

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_sqlite_fresh_single_stream_when_explicit_rows_commit_or_abort_then_reopen_preserves_only_ordered_commits()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    let (name, rows) = store_explicit_rows(&fixture.store).await?;
    drop(fixture.store);
    let reopened = fixture::FileSqlite::open_store(&fixture.path).await?;
    verify_explicit_rows(&reopened, name, &rows).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_fresh_single_stream_when_explicit_rows_commit_or_abort_then_reopen_preserves_only_ordered_commits()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = async {
        let (name, rows) = store_explicit_rows(&fixture.store).await?;
        verify_explicit_rows(&fixture.open_independent_store().await?, name, &rows).await
    }
    .await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_fresh_single_stream_when_explicit_rows_commit_or_abort_then_reopen_preserves_only_ordered_commits()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    let (name, rows) = store_explicit_rows(&fixture.store).await?;
    drop(fixture.store);
    let reopened = super::turso::FileTurso::open_store(&fixture.path).await?;
    verify_explicit_rows(&reopened, name, &rows).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_fresh_single_stream_when_explicit_rows_commit_or_abort_then_reopen_preserves_only_ordered_commits()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    let (name, rows) = store_explicit_rows(&fixture.store).await?;
    drop(fixture.store);
    let reopened = super::rocksdb::RocksDb::open_store(&fixture.path).await?;
    verify_explicit_rows(&reopened, name, &rows).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_fresh_single_stream_when_explicit_rows_commit_or_abort_then_reopen_preserves_only_ordered_commits()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = async {
        let (name, rows) = store_explicit_rows(&fixture.store).await?;
        verify_explicit_rows(&fixture.open_independent_store().await?, name, &rows).await
    }
    .await;
    fixture.finish().await?;
    result
}

async fn store_explicit_rows<P: StreamPort + 'static>(
    port: &P,
) -> HarnessResult<(StreamName, Vec<StreamItem>)> {
    let name = port
        .create_stream(
            UserStreamName::new("explicit-native-rows"),
            None,
            StreamPartitioningMode::Single,
        )
        .await?;
    assert!(
        port.read_forward(StreamRowKind::User(name.clone()), None, 4)
            .await?
            .items
            .is_empty()
    );
    let rows = [0, 7, u64::MAX / 2, u64::MAX - 1, u64::MAX]
        .into_iter()
        .map(|index| explicit_row(&name, index, 1))
        .collect::<HarnessResult<Vec<_>>>()?;
    let staged = rows.clone();
    port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            for index in [4, 0, 3, 1, 2] {
                let row = staged[index].clone();
                attempt.store_user_item(row.clone()).await?;
                let observed = point(attempt, &row)
                    .await?
                    .ok_or_else(|| StorageError::internal("staged explicit row missing"))?;
                assert_row(&observed, &row);
            }
            Ok(())
        })
    })
    .await?;
    let aborted = explicit_row(&name, 7, 3)?;
    let rollback: StorageResult<()> = port
        .with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                attempt.store_user_item(aborted.clone()).await?;
                let observed = point(attempt, &aborted)
                    .await?
                    .ok_or_else(|| StorageError::internal("aborted staged row missing"))?;
                assert_row(&observed, &aborted);
                Err(StorageError::internal("explicit stream fixture rollback"))
            })
        })
        .await;
    assert!(
        !rollback
            .expect_err("fixture callback unexpectedly committed")
            .is_commit_unknown()
    );
    verify_explicit_rows(port, name.clone(), &rows).await?;
    Ok((name, rows))
}

async fn verify_explicit_rows<P: StreamPort + 'static>(
    port: &P,
    name: StreamName,
    expected: &[StreamItem],
) -> HarnessResult<()> {
    let through = expected[4].id;
    let request = page_request(None, ReadDirection::Forward, 2);
    let first = page(port, name.clone(), request.clone(), through).await?;
    assert_eq!(2, first.items.len());
    assert!(first.has_more);
    assert_rows(&first.items, &expected[..2]);
    assert_eq!(Some(expected[1].id), first.last_evaluated_key);
    let next = page(
        port,
        name.clone(),
        page_request(first.last_evaluated_key, ReadDirection::Forward, 4),
        through,
    )
    .await?;
    assert_rows(&next.items, &expected[2..]);
    assert!(!next.has_more);
    let reverse = page(
        port,
        name.clone(),
        page_request(Some(through), ReadDirection::Backward, 4),
        through,
    )
    .await?;
    assert_eq!(4, reverse.items.len());
    assert!(!reverse.has_more);
    for (observed, expected) in reverse.items.iter().zip(expected[..4].iter().rev()) {
        assert_row(observed, expected);
    }
    let bounded = page(
        port,
        name.clone(),
        page_request(None, ReadDirection::Forward, 4),
        expected[2].id,
    )
    .await?;
    assert_rows(&bounded.items, &expected[..3]);
    assert!(!bounded.has_more);
    let reverse_bound = page(
        port,
        name.clone(),
        page_request(Some(through), ReadDirection::Backward, 4),
        expected[2].id,
    )
    .await?;
    for (observed, expected) in reverse_bound.items.iter().zip(expected[..3].iter().rev()) {
        assert_row(observed, expected);
    }
    assert_eq!(3, reverse_bound.items.len());
    let foreign = port
        .create_stream(
            UserStreamName::from(format!(
                "other-explicit-generation-{}",
                uuid::Uuid::new_v4()
            )),
            None,
            StreamPartitioningMode::Single,
        )
        .await?;
    verify_point_bounds(port, name, foreign, expected.to_vec()).await
}

async fn verify_point_bounds<P: StreamPort + 'static>(
    port: &P,
    name: StreamName,
    foreign: StreamName,
    expected: Vec<StreamItem>,
) -> HarnessResult<()> {
    let absent = explicit_row(&name, 7, 3)?;
    port.with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            assert!(
                point(attempt, &absent).await?.is_none(),
                "rolled-back explicit row persisted"
            );
            for row in &expected {
                let observed = point(attempt, row)
                    .await?
                    .ok_or_else(|| StorageError::internal("committed explicit row missing"))?;
                assert_row(&observed, row);
            }
            assert!(
                attempt
                    .read_user_item_through(&name, expected[3].id, expected[2].id)
                    .await?
                    .is_none()
            );
            assert!(
                attempt
                    .read_user_item_through(&foreign, expected[2].id, expected[4].id)
                    .await?
                    .is_none()
            );
            let request = page_request(None, ReadDirection::Forward, 4);
            let foreign_page = attempt
                .read_user_page_through(&foreign, &request, expected[4].id)
                .await?;
            assert!(
                foreign_page
                    .finish(&request, &StreamRowKind::User(foreign.clone()))?
                    .items
                    .is_empty()
            );
            let page = attempt
                .read_user_page_through(&name, &request, expected[4].id)
                .await?;
            assert!(
                page.finish(&request, &StreamRowKind::User(foreign))
                    .is_err(),
                "foreign incarnation accepted actual rows"
            );
            Ok(())
        })
    })
    .await?;
    Ok(())
}

async fn page<P: StreamPort + 'static>(
    port: &P,
    name: StreamName,
    request: StreamReadRequest,
    through: StreamItemId,
) -> HarnessResult<StreamPage> {
    Ok(port
        .with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                attempt
                    .read_user_page_through(&name, &request, through)
                    .await?
                    .finish(&request, &StreamRowKind::User(name))
            })
        })
        .await?)
}

fn page_request(
    start: Option<StreamItemId>,
    direction: ReadDirection,
    limit: u32,
) -> StreamReadRequest {
    StreamReadRequest {
        partitioning_mode: StreamPartitioningMode::Single,
        exclusive_start: start,
        direction,
        limit: NonZeroU32::new(limit).expect("fixture limit is nonzero"),
    }
}

async fn point<A: StreamAttempt>(
    attempt: &mut A,
    row: &StreamItem,
) -> StorageResult<Option<StreamItem>> {
    let name = row
        .stream_name
        .as_ref()
        .ok_or_else(|| StorageError::internal("fixture row has no stream"))?;
    attempt.read_user_item_through(name, row.id, row.id).await
}

fn assert_rows(observed: &[StreamItem], expected: &[StreamItem]) {
    assert_eq!(expected.len(), observed.len());
    for (observed, expected) in observed.iter().zip(expected) {
        assert_row(observed, expected);
    }
}

fn explicit_row(name: &StreamName, log_index: u64, ordinal: u32) -> HarnessResult<StreamItem> {
    let mut bytes = [0; 12];
    bytes[..8].copy_from_slice(&log_index.to_be_bytes());
    bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
    Ok(StreamItem {
        id: StreamItemId::try_from(bytes.as_slice())?,
        stream_name: Some(name.clone()),
        data: vec![log_index as u8, ordinal as u8, 0, 255],
        data_type: StreamDataType::Binary,
        created_at: TimestampMillis::from_timestamp(1_800_000_000_000 + i64::from(ordinal)),
    })
}

fn assert_row(observed: &StreamItem, expected: &StreamItem) {
    assert_eq!(expected.id, observed.id);
    assert_eq!(expected.stream_name, observed.stream_name);
    assert_eq!(expected.data, observed.data);
    assert_eq!(expected.data_type, observed.data_type);
    assert_eq!(expected.created_at, observed.created_at);
}
