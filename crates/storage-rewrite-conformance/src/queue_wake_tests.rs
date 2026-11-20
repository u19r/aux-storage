//! Queue wake (N1) on the provider that `STORAGE_TEST_BACKEND` selects: the
//! wake costs one blind write key and no read, and a send or catalog change
//! on one store reaches a waiter on another store of the same database.
//!
//! Tests assert counts, effects, and slot generations, never elapsed times.

use std::{collections::HashMap, sync::Arc, time::Duration};

use queue_provider::{
    Queue, QueueWakeSlot, ReceiptHandle, ReceiveMessageRequest, SendMessageRequest,
    queue_message_for_send,
};
use storage_types::DurationSeconds;

use super::{
    native_census_test_support::{CensusCounts, NativeCensus, SqlLocks},
    queue_claim_tests::concurrent_backend,
    test_support::TestStore,
};

const TEST_BUDGET: Duration = Duration::from_secs(10);
/// Setup of the 10,000 waiters of the idle test.
const SETUP_BUDGET: Duration = Duration::from_secs(120);
const WAKE_FAMILY: &str = "~q/w";

async fn queue(store: &TestStore, label: &str) -> String {
    let name = format!("{label}_{}", uuid::Uuid::new_v4().simple());
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("http://localhost/000000000000/{name}"),
            queue_name: name,
            attributes: HashMap::new(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .expect("create queue")
        .queue_url
}

async fn send(store: &TestStore, queue_url: &str, body: &str) {
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.to_owned(),
                message_body: body.to_owned(),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .expect("send message");
}

async fn change_visibility(store: &TestStore, queue_url: &str, receipt: &str, seconds: u32) {
    store
        .queue_operations
        .change_message_visibility(
            queue_url,
            ReceiptHandle::from(receipt),
            DurationSeconds::from(seconds),
        )
        .await
        .expect("change visibility");
}

fn receive_request(queue_url: &str, wait_time_seconds: i64) -> ReceiveMessageRequest {
    ReceiveMessageRequest {
        queue_url: queue_url.to_owned(),
        max_number_of_messages: Some(1),
        visibility_timeout: None,
        wait_time_seconds: Some(wait_time_seconds),
        attribute_names: None,
        message_attribute_names: None,
    }
}

async fn until<F: FnMut() -> bool>(budget: Duration, what: &str, mut condition: F) {
    tokio::time::timeout(budget, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} never happened"));
}

/// Waves, statements, keys, ranges, read conflicts, and locks.
fn read_shape(counts: &CensusCounts) -> (u64, u64, u64, u64, u64, u64, SqlLocks) {
    (
        counts.waves,
        counts.statements,
        counts.keys,
        counts.ranges,
        counts.conflicts.read_keys,
        counts.conflicts.read_ranges,
        counts.locks,
    )
}

/// Gaining coverage costs each queue one re-probe; wait until the store's
/// attempt count is stable across 500 ms.
async fn settle_begins(census: &NativeCensus) {
    tokio::time::timeout(SETUP_BUDGET, async {
        loop {
            let before = census.snapshot().begins;
            tokio::time::sleep(Duration::from_millis(500)).await;
            if census.snapshot().begins == before {
                break;
            }
        }
    })
    .await
    .expect("the coverage re-probes never ended");
}

async fn spawn_waiter(
    manager: &Arc<queue::QueueManager>,
    queue_url: &str,
) -> tokio::task::JoinHandle<
    Result<queue_provider::ReceiveMessageResponse, queue_provider::QueueError>,
> {
    let manager = Arc::clone(manager);
    let request = receive_request(queue_url, 20);
    tokio::spawn(async move { manager.receive_message(request).await })
}

#[tokio::test]
async fn given_visibility_lowered_or_raised_when_measured_then_the_wake_costs_one_blind_write_key()
{
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let queue_url = queue(&store, "wake_cost").await;
    for index in 0..4 {
        send(&store, &queue_url, &format!("m{index}")).await;
    }
    let claimed = store
        .queue_operations
        .receive_messages(&queue_url, 4, Some(DurationSeconds::from(30)), None)
        .await
        .expect("claim messages");
    assert_eq!(claimed.messages.len(), 4);
    let receipts = claimed
        .messages
        .iter()
        .map(|message| message.receipt_handle.clone())
        .collect::<Vec<_>>();

    // Warm the statement and binding caches of both paths.
    change_visibility(&store, &queue_url, &receipts[0], 60).await;
    change_visibility(&store, &queue_url, &receipts[1], 0).await;

    census.reset();
    change_visibility(&store, &queue_url, &receipts[2], 60).await;
    let raised = census.snapshot();
    census.reset();
    change_visibility(&store, &queue_url, &receipts[3], 0).await;
    let lowered = census.snapshot();

    assert_eq!(
        read_shape(&raised),
        read_shape(&lowered),
        "raised {raised}\nlowered {lowered}"
    );
    let foundationdb = TestStore::selected_backend() == "foundationdb";
    if foundationdb {
        assert_eq!(lowered.rows_written, raised.rows_written + 1);
        assert_eq!(
            lowered.conflicts.write_keys,
            raised.conflicts.write_keys + 1
        );
        assert!(lowered.touched_written.contains(WAKE_FAMILY), "{lowered}");
        assert!(!raised.touched_written.contains(WAKE_FAMILY), "{raised}");
    } else {
        assert_eq!(lowered.rows_written, raised.rows_written);
        assert_eq!(lowered.conflicts.write_keys, raised.conflicts.write_keys);
    }
    store.finish().await;
}

#[tokio::test]
async fn given_send_when_measured_then_the_wake_key_is_written_and_never_read() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let queue_url = queue(&store, "wake_send").await;
    send(&store, &queue_url, "warm").await;
    census.reset();
    send(&store, &queue_url, "measured").await;
    let counts = census.snapshot();
    let backend = TestStore::selected_backend();
    println!("N1_SEND_CENSUS {backend} {counts}");
    if backend == "foundationdb" {
        assert!(counts.touched_written.contains(WAKE_FAMILY), "{counts}");
    }
    assert!(!counts.touched_read.contains(WAKE_FAMILY), "{counts}");
    store.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_waiter_on_store_b_when_store_a_sends_then_it_returns_before_the_probe_interval() {
    assert!(concurrent_backend(), "select postgres or foundationdb");
    let census = NativeCensus::new();
    let a = TestStore::new().await;
    let b = a.open_same_database_with_census(Arc::clone(&census)).await;
    let b_manager = Arc::new(
        queue::QueueManager::new(b.queue_operations.clone())
            .with_receive_probe_interval(Duration::from_secs(60)),
    );
    let queue_url = queue(&a, "wake_cross").await;
    let slot = QueueWakeSlot::of_queue(&queue_url);
    let board = b
        .queue_operations
        .queue_wake_board()
        .expect("shared providers have a board");

    for (round, body) in ["from-a", "from-a-again"].into_iter().enumerate() {
        let waiter = spawn_waiter(&b_manager, &queue_url).await;
        until(TEST_BUDGET, "the waiter to wait", || {
            b_manager.waiting_receives() == 1
        })
        .await;
        until(TEST_BUDGET, "the slot to be covered", || {
            board.mark(slot).covered
        })
        .await;
        // The re-probe that coverage causes would find the message without a
        // wake: let it end before the send.
        settle_begins(&census).await;

        send(&a, &queue_url, body).await;
        // No 60 s interval probe can have run: only the wake delivers.
        let response = tokio::time::timeout(TEST_BUDGET, waiter)
            .await
            .unwrap_or_else(|_| panic!("round {round}: the waiter did not return in time"))
            .unwrap()
            .unwrap();
        assert_eq!(response.messages.len(), 1);
        assert_eq!(response.messages[0].body, body);
    }

    drop(b_manager);
    drop(b);
    a.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_purge_and_delete_on_store_a_when_store_b_watches_then_its_slot_advances() {
    assert!(concurrent_backend(), "select postgres or foundationdb");
    let a = TestStore::new().await;
    let b = a.open_same_database().await;
    let queue_url = queue(&a, "wake_catalog").await;
    let slot = QueueWakeSlot::of_queue(&queue_url);
    let board = b
        .queue_operations
        .queue_wake_board()
        .expect("shared providers have a board");

    let interest = board.register(slot);
    until(TEST_BUDGET, "the slot to be covered", || {
        board.mark(slot).covered
    })
    .await;

    let before_purge = board.mark(slot).generation;
    a.queue_operations
        .purge_queue(&queue_url)
        .await
        .expect("purge");
    until(TEST_BUDGET, "the purge to advance the slot", || {
        board.mark(slot).generation > before_purge
    })
    .await;

    let before_delete = board.mark(slot).generation;
    a.queue_operations
        .delete_queue(&queue_url)
        .await
        .expect("delete");
    until(TEST_BUDGET, "the delete to advance the slot", || {
        board.mark(slot).generation > before_delete
    })
    .await;

    drop(interest);
    drop(b);
    a.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=foundationdb"]
async fn given_1000_idle_queues_with_10000_waiters_on_store_b_when_3_seconds_pass_then_no_queue_attempt()
 {
    // N1d 5.5 row 5 names FoundationDB only; Postgres has a different setup
    // cost (10,000 first probes over a connection pool) and is not measured.
    if TestStore::selected_backend() != "foundationdb" {
        println!("skipped: the idle-waiter census runs on foundationdb only");
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new().await;
    let b = a.open_same_database_with_census(Arc::clone(&census)).await;
    let b_manager = Arc::new(queue::QueueManager::new(b.queue_operations.clone()));
    let board = b
        .queue_operations
        .queue_wake_board()
        .expect("foundationdb has a board");

    let mut queues = Vec::new();
    for index in 0..1_000 {
        queues.push(queue(&a, &format!("idle{index}")).await);
    }
    let waiters = (0..10_000)
        .map(|index| {
            let manager = Arc::clone(&b_manager);
            let request = receive_request(&queues[index % queues.len()], 20);
            tokio::spawn(async move { manager.receive_message(request).await })
        })
        .collect::<Vec<_>>();
    let slots = queues
        .iter()
        .map(|url| QueueWakeSlot::of_queue(url))
        .collect::<Vec<_>>();
    until(SETUP_BUDGET, "10,000 waiting receives", || {
        b_manager.waiting_receives() == 10_000
    })
    .await;
    until(SETUP_BUDGET, "every queue slot to be covered", || {
        slots.iter().all(|slot| board.mark(*slot).covered)
    })
    .await;

    settle_begins(&census).await;
    census.reset();
    tokio::time::sleep(Duration::from_secs(3)).await;
    // The feed's own read-only transaction is not a store attempt.
    assert_eq!(census.snapshot().begins, 0, "idle waiters probed queues");

    waiters.iter().for_each(tokio::task::JoinHandle::abort);
    drop(b_manager);
    drop(b);
    a.finish().await;
}

/// G2: the Postgres listener ends (its backend is terminated), the slots go
/// uncovered, the listener reconnects and covers them, and a send from A
/// still reaches a waiter on B.
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_listener_session_terminated_when_it_reconnects_then_a_send_from_store_a_still_wakes_store_b()
 {
    if TestStore::selected_backend() != "postgres" {
        println!("skipped: the listener reconnect path is Postgres only");
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new().await;
    let b = a.open_same_database_with_census(Arc::clone(&census)).await;
    let b_manager = Arc::new(
        queue::QueueManager::new(b.queue_operations.clone())
            .with_receive_probe_interval(Duration::from_secs(60)),
    );
    let queue_url = queue(&a, "wake_listener").await;
    let slot = QueueWakeSlot::of_queue(&queue_url);
    let board = b
        .queue_operations
        .queue_wake_board()
        .expect("postgres has a board");

    let interest = board.register(slot);
    until(TEST_BUDGET, "the slot to be covered", || {
        board.mark(slot).covered
    })
    .await;
    let recorded = board.mark(slot).generation;
    a.execute_postgres(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE \
         'LISTEN%aux_queue_wake%' AND datname = current_database()",
    )
    .await;
    // Uncovered (+1) and covered again (+1).
    until(
        TEST_BUDGET,
        "the listener to reconnect and cover again",
        || {
            let mark = board.mark(slot);
            mark.covered && mark.generation >= recorded + 2
        },
    )
    .await;
    drop(interest);

    let waiter = spawn_waiter(&b_manager, &queue_url).await;
    until(TEST_BUDGET, "the waiter to wait", || {
        b_manager.waiting_receives() == 1
    })
    .await;
    until(TEST_BUDGET, "the slot to be covered", || {
        board.mark(slot).covered
    })
    .await;
    settle_begins(&census).await;
    send(&a, &queue_url, "after-reconnect").await;
    let response = tokio::time::timeout(TEST_BUDGET, waiter)
        .await
        .expect("the waiter did not return in time")
        .unwrap()
        .unwrap();
    assert_eq!(response.messages[0].body, "after-reconnect");

    drop(b_manager);
    drop(b);
    a.finish().await;
}

/// N1 W1: the wake of a send publishes from the task that owns the attempt,
/// after its native outcome. A caller dropped while the commit is held after
/// its acknowledgement still gets the wake; a caller dropped before the
/// commit is submitted, whose attempt then expires, publishes none.
/// Cross-node boards (Postgres, FoundationDB) publish through a feed and are
/// not covered here.
fn local_wake_board(
    store: &TestStore,
    queue_url: &str,
) -> (Arc<queue_provider::QueueWakeBoard>, QueueWakeSlot) {
    let board = store
        .queue_operations
        .queue_wake_board()
        .expect("every provider has a wake board");
    (board, QueueWakeSlot::of_queue(queue_url))
}

fn spawn_send(
    store: &TestStore,
    queue_url: &str,
) -> tokio::task::JoinHandle<queue_provider::QueueResult<queue_provider::MessageId>> {
    let operations = Arc::clone(&store.queue_operations);
    let message = queue_message_for_send(
        SendMessageRequest {
            queue_url: queue_url.to_owned(),
            message_body: "dropped".to_owned(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        },
        store.now(),
    );
    tokio::spawn(async move { operations.send_message(message).await })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_caller_dropped_after_send_commit_submitted_when_released_then_the_wake_still_publishes()
 {
    use super::commit_mark_tests::{Plan, probed_store};
    if concurrent_backend() {
        eprintln!("skipped: cross-node boards publish through a feed");
        return;
    }
    let (probe, store) = probed_store().await;
    let queue_url = queue(&store, "wake_drop_commit").await;
    let (board, slot) = local_wake_board(&store, &queue_url);
    let before = board.mark(slot).generation;
    probe.arm(Plan::HoldAfterAcknowledgement);
    let caller = spawn_send(&store, &queue_url);
    probe.wait_reached().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(
        board.mark(slot).generation,
        before,
        "the wake publishes after the native outcome, not before"
    );
    probe.released.notify_one();
    until(TEST_BUDGET, "the dropped send to publish its wake", || {
        board.mark(slot).generation > before
    })
    .await;
    assert_eq!(probe.marks(), 1, "a dropped caller does not replay");
    let received = store
        .queue_operations
        .receive_messages(&queue_url, 1, Some(DurationSeconds::from(30)), None)
        .await
        .expect("receive");
    assert_eq!(received.messages.len(), 1, "the committed send is visible");
    store.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_caller_dropped_before_send_commit_when_the_attempt_expires_then_no_wake_publishes() {
    use super::commit_mark_tests::{Plan, WRITE_BUDGET, probed_store_with_write_budget};
    if concurrent_backend() {
        eprintln!("skipped: cross-node boards publish through a feed");
        return;
    }
    let (probe, store) = probed_store_with_write_budget(WRITE_BUDGET).await;
    let queue_url = queue(&store, "wake_drop_abort").await;
    let (board, slot) = local_wake_board(&store, &queue_url);
    let before = board.mark(slot).generation;
    probe.arm(Plan::HoldBeforeSubmit);
    let caller = spawn_send(&store, &queue_url);
    probe.wait_reached().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    // The attempt outlives its caller; its deadline (a Tokio instant, which
    // the test clock does not move) passes while held, so the commit is
    // refused at the submission mark and the attempt aborts.
    tokio::time::sleep(WRITE_BUDGET + Duration::from_millis(500)).await;
    probe.released.notify_one();
    until(TEST_BUDGET, "the expired attempt to roll back", || {
        probe.aborts() > 0
    })
    .await;
    assert_eq!(
        board.mark(slot).generation,
        before,
        "an aborted send publishes no wake"
    );
    assert_eq!(probe.marks(), 1, "a known deadline is not replayed");
    let received = store
        .queue_operations
        .receive_messages(&queue_url, 1, Some(DurationSeconds::from(30)), None)
        .await
        .expect("receive");
    assert!(received.messages.is_empty(), "nothing was committed");
    store.finish().await;
}
