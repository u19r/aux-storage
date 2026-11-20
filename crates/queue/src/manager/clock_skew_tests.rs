//! Two nodes whose clocks differ share one store (W2 D14, hole H1 of
//! `quint/queue_claim.qnt`): the node whose clock is ahead must not claim a
//! message inside the lease that the other node gave.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use queue_provider::{
    Queue, QueueOperations as _, QueuePort, SendMessageRequest, queue_message_for_send,
};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::{AttemptRequest, Clock, DurationSeconds, StorageResult, TimestampMillis};

/// One node: the shared store seen through this node's clock.
struct Node {
    store: Arc<SqlStore<SqliteBackend>>,
    ahead_millis: i64,
}

impl Clock for Node {
    fn now(&self) -> TimestampMillis {
        self.store.now() + self.ahead_millis
    }
}

impl storage_types::AttemptDeadlineSource for Node {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        storage_types::AttemptDeadlines::default()
    }
}

impl QueuePort for Node {
    type Attempt<'native>
        = <SqlStore<SqliteBackend> as QueuePort>::Attempt<'native>
    where Self: 'native;

    async fn with_attempt<R, F>(&self, request: AttemptRequest, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> Pin<
                Box<dyn Future<Output = StorageResult<R>> + Send + 'borrow>,
            > + Send
            + 'static,
    {
        self.store.with_attempt(request, operation).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_clocks_400ms_apart_when_the_fast_node_receives_inside_the_lease_then_it_gets_nothing()
 {
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    let store = Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap(),
    );
    let slow = Node {
        store: Arc::clone(&store),
        ahead_millis: 0,
    };
    let fast = Node {
        store,
        ahead_millis: 400,
    };
    let url = slow
        .create_queue(Queue {
            queue_name: "skew".into(),
            queue_url: "http://localhost/queue/skew".into(),
            attributes: Default::default(),
            created_at: slow.now(),
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    slow.send_message(queue_message_for_send(
        SendMessageRequest {
            queue_url: url.clone(),
            message_body: "leased".into(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        },
        slow.now(),
    ))
    .await
    .unwrap();
    let lease = DurationSeconds::from(1);
    let held = slow
        .receive_messages(&url, 1, Some(lease), None)
        .await
        .unwrap();
    assert_eq!(held.messages.len(), 1);

    // True time 0.7 s: the fast clock reads 1.1 s, after the lease end in the
    // slow node's clock, but the holder's lease runs until 1.0 s.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let early = fast
        .receive_messages(&url, 1, Some(lease), None)
        .await
        .unwrap();
    assert!(early.messages.is_empty(), "two holders inside one lease");
    // The guarded lease is the next due message: lease end 1.0 s plus the
    // 0.5 s margin, seen from the fast clock at 1.1 s, is about 0.4 s away.
    let next_due_in = early.next_due_in.expect("the guarded lease is known");
    assert!(
        (Duration::from_millis(300)..=Duration::from_millis(410)).contains(&next_due_in),
        "next due in {next_due_in:?}"
    );

    // True time 1.3 s: the lease ended in both clocks; the margin has passed.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let late = fast
        .receive_messages(&url, 1, Some(lease), None)
        .await
        .unwrap();
    assert_eq!(late.messages.len(), 1);
}
