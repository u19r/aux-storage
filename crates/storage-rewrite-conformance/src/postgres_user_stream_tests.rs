//! Postgres user streams share one catalog row across concurrent writers.

use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::{FutureExt as _, future::join_all};
use storage_types::{AttemptMode, TimestampMillis, UserStreamName};
use stream_provider::{
    CursorName, CursorPosition, StreamAttempt, StreamDataType, StreamOperations,
    StreamPartitioningMode, StreamPort, StreamRowKind, StreamRowValue,
};

use super::test_support::TestStore;

#[tokio::test]
async fn given_one_stream_when_many_writers_append_then_cursor_reads_every_unique_item() {
    let store = TestStore::new_postgres().await;
    let outcome = AssertUnwindSafe(async {
        let name = UserStreamName::new("concurrent-user-stream");
        let internal_id = store
            .stream_operations
            .create_stream(name.clone(), None, StreamPartitioningMode::Single)
            .await
            .expect("create user stream");
        let stream = StreamRowKind::User(internal_id);
        let cursors = store.stream_operations.clone();
        cursors
            .create_cursor(
                stream.clone(),
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

        await_visible_items(cursors.as_ref(), stream.clone(), committed.len()).await;
        let page = cursors
            .read_from_cursor(stream.clone(), CursorName::new("reader"), 32)
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
            .read_from_cursor(stream, CursorName::new("reader"), 32)
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
async fn given_earlier_uncommitted_insert_when_later_insert_commits_then_cursor_does_not_skip() {
    let store = TestStore::new_postgres().await;
    let outcome = AssertUnwindSafe(async {
        let name = UserStreamName::new("out-of-order-user-stream");
        let internal_id = store
            .stream_operations
            .create_stream(name.clone(), None, StreamPartitioningMode::Single)
            .await
            .expect("create user stream");
        let stream = StreamRowKind::User(internal_id.clone());
        let cursor_name = CursorName::new("reader");
        store
            .stream_operations
            .create_cursor(stream.clone(), cursor_name.clone(), CursorPosition::Head)
            .await
            .expect("create head cursor");

        let backend =
            sql_backend::postgres::PostgresBackend::initialize(&store.postgres_dsn(), 2, false)
                .await
                .expect("open separate writer pool");
        let writer = sql_backend::SqlStore::initialize_schema(backend)
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
            writer
                .with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let id = attempt.append_attempt_allocated_user_item(row).await?;
                        inserted_tx.send(id).expect("reader waits for earlier ID");
                        finish_rx.await.expect("release earlier transaction");
                        Ok(())
                    })
                })
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
            .read_from_cursor(stream.clone(), cursor_name.clone(), 10)
            .await
            .expect("read before earlier commit");
        assert!(
            blocked.items.is_empty(),
            "cursor must not pass an active earlier XID"
        );

        finish_tx.send(()).expect("release earlier transaction");
        earlier_task.await.expect("earlier append task completes");
        await_visible_items(store.stream_operations.as_ref(), stream.clone(), 2).await;
        let visible = store
            .stream_operations
            .read_from_cursor(stream, cursor_name, 10)
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

async fn await_visible_items(
    operations: &dyn StreamOperations,
    stream: StreamRowKind,
    expected: usize,
) {
    // The snapshot floor is cluster-wide; another test's older write can
    // delay this stream after all of its own rows have committed.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let page = operations
                .read_forward(stream.clone(), None, expected as u32)
                .await
                .expect("read committed stream rows");
            if page.items.len() == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("older cluster transaction released the visibility floor");
}
