//! Postgres user streams share one catalog row across concurrent writers.

use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::{FutureExt as _, future::join_all};
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, TimestampMillis, UserStreamName};
use stream_provider::{
    CursorName, CursorPosition, StreamAttempt, StreamDataType, StreamPartitioningMode,
    StreamRowValue,
};

use super::test_support::TestStore;

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_one_stream_when_many_writers_append_then_cursor_reads_every_unique_item() {
    let store = TestStore::new_postgres().await;
    let outcome = AssertUnwindSafe(async {
        let name = UserStreamName::new("concurrent-user-stream");
        let internal_id = store
            .stream_operations
            .create_stream(name.clone(), None, StreamPartitioningMode::Single)
            .await
            .expect("create user stream");
        let stream = internal_id;
        let cursors = store.stream_operations.clone();
        cursors
            .create_cursor(
                stream.clone().into(),
                CursorName::new("reader"),
                CursorPosition::Head,
            )
            .await
            .expect("create head cursor");

        let writes = (0_u8..32).map(|value| {
            let operations = store.stream_operations.clone();
            let name = name.clone();
            async move {
                let id = operations
                    .append_item(name, &[value], None)
                    .await
                    .expect("append concurrently");
                (id, value)
            }
        });
        let committed = join_all(writes).await;
        let unique_ids = committed.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
        assert_eq!(unique_ids.len(), committed.len());

        store
            .wait_until_user_item_is_readable(*unique_ids.iter().max().unwrap())
            .await;
        let page = cursors
            .read_from_cursor(stream.clone().into(), CursorName::new("reader"), 32)
            .await
            .expect("consume committed rows");
        assert_eq!(page.items.len(), committed.len());
        let seen = page
            .items
            .iter()
            .map(|item| item.data[0])
            .collect::<HashSet<_>>();
        assert_eq!(seen.len(), committed.len());
        let exhausted = cursors
            .read_from_cursor(stream.into(), CursorName::new("reader"), 32)
            .await
            .expect("read exhausted cursor");
        assert!(exhausted.items.is_empty());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_earlier_uncommitted_insert_when_later_insert_commits_then_cursor_does_not_skip() {
    let store = TestStore::new_postgres().await;
    let outcome = AssertUnwindSafe(async {
        let name = UserStreamName::new("out-of-order-user-stream");
        let internal_id = store
            .stream_operations
            .create_stream(name.clone(), None, StreamPartitioningMode::Single)
            .await
            .expect("create user stream");
        let stream = internal_id.clone();
        let cursor_name = CursorName::new("reader");
        store
            .stream_operations
            .create_cursor(
                stream.clone().into(),
                cursor_name.clone(),
                CursorPosition::Head,
            )
            .await
            .expect("create head cursor");

        let backend = sql_backend::postgres::PostgresBackend::initialize(
            &store.postgres_dsn(),
            2,
            false,
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .expect("open separate writer pool");
        let writer = sql_backend::SqlStore::initialize_schema(
            backend,
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .expect("open separate writer store");
        let row = StreamRowValue::new(
            internal_id.clone(),
            b"earlier".to_vec(),
            StreamDataType::Text,
            TimestampMillis::now(),
        );
        let (inserted_tx, inserted_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let earlier_task = tokio::spawn(async move {
            stream_provider::StreamPort::with_attempt(
                &writer,
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
                    storage_types::AttemptDeadlineSource::attempt_deadlines(&writer),
                ),
                move |attempt| {
                    Box::pin(async move {
                        let id = attempt.append_attempt_allocated_user_item(row).await?;
                        inserted_tx.send(id).expect("reader waits for earlier ID");
                        finish_rx.await.expect("release earlier transaction");
                        Ok(())
                    })
                },
            )
            .await
            .expect("commit earlier stream append");
        });
        let earlier = inserted_rx
            .await
            .expect("earlier append reached commit barrier");

        let later = store
            .stream_operations
            .append_item(name, b"later", None)
            .await
            .expect("commit later row while earlier transaction is open");
        assert!(earlier < later);
        let blocked = store
            .stream_operations
            .read_from_cursor(stream.clone().into(), cursor_name.clone(), 10)
            .await
            .expect("read before earlier commit");
        assert!(
            blocked.items.is_empty(),
            "cursor must not pass an active earlier XID"
        );

        finish_tx.send(()).expect("release earlier transaction");
        earlier_task.await.expect("earlier append task completes");
        store.wait_until_user_item_is_readable(later).await;
        let visible = store
            .stream_operations
            .read_from_cursor(stream.into(), cursor_name, 10)
            .await
            .expect("read both committed rows");
        assert_eq!(visible.items.len(), 2);
        assert_eq!(visible.items[0].id, earlier);
        assert_eq!(visible.items[1].id, later);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
