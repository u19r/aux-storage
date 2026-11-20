//! W2 claim contract on the provider that `STORAGE_TEST_BACKEND` selects:
//! concurrent claims deliver each message once with no error, a claim that
//! loses every conflict returns an empty receive, counts stop at their bound,
//! and stale or malformed receipts give the SQS answers.
//!
//! Each test counts native calls through a fault probe, so it asserts claim
//! attempts and conflict retries, not throughput.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::future::BoxFuture;
use queue_provider::{
    MessageResponse, Queue, QueueError, QueueValidationKind, ReceiptHandle, ReceivedMessages,
    SendMessageRequest, queue_message_for_send,
};
use storage_types::{DurationSeconds, StorageResult};
use tokio::sync::Notify;

use super::test_support::TestStore;

/// Counts claim attempts and native calls, and can fail or hold commits.
#[derive(Default)]
pub(super) struct ClaimProbe {
    claims: AtomicUsize,
    calls: AtomicUsize,
    /// Message point reads: SQL reads by `message_id`, KV batched gets.
    point_reads: AtomicUsize,
    /// Native transactions that reached their commit. `calls` counts SQL
    /// statements and KV read waves; KV staged writes are not round trips.
    commits: AtomicUsize,
    /// Delivery state rows written: SQL write statements on the state table;
    /// KV logical mutations / 2 (a state change deletes and puts one key).
    state_writes: AtomicUsize,
    /// SQL write statements on the payload table (W3S R1).
    payload_writes: AtomicUsize,
    /// KV logical mutations of all staged writes.
    mutations: AtomicUsize,
    fail_commits: AtomicBool,
    /// The next native read fails with a native conflict, as a FoundationDB
    /// read with a read version that is too old or in the future does.
    fail_next_read: AtomicBool,
    /// When set to `n + 1`, the commit after `n` more commits fails with
    /// `NativeCapacityRejected` (finding 4); 0 is off.
    pub(super) reject_capacity_after: AtomicUsize,
    pub(super) hold_next_commit: AtomicBool,
    pub(super) held: Notify,
    pub(super) release: Notify,
    /// Commits wait here until this many attempts reach their commit.
    commit_barrier: std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

impl ClaimProbe {
    pub(super) fn reset(&self) {
        self.claims.store(0, Ordering::SeqCst);
        self.calls.store(0, Ordering::SeqCst);
        self.point_reads.store(0, Ordering::SeqCst);
        self.commits.store(0, Ordering::SeqCst);
        self.state_writes.store(0, Ordering::SeqCst);
        self.payload_writes.store(0, Ordering::SeqCst);
        self.mutations.store(0, Ordering::SeqCst);
    }

    pub(super) fn state_writes(&self) -> usize {
        self.state_writes.load(Ordering::SeqCst)
    }

    pub(super) fn payload_writes(&self) -> usize {
        self.payload_writes.load(Ordering::SeqCst)
    }

    pub(super) fn mutations(&self) -> usize {
        self.mutations.load(Ordering::SeqCst)
    }

    pub(super) fn commits(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }

    pub(super) fn point_reads(&self) -> usize {
        self.point_reads.load(Ordering::SeqCst)
    }

    pub(super) fn claims(&self) -> usize {
        self.claims.load(Ordering::SeqCst)
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn at_commit(&self) -> StorageResult<()> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if self.fail_commits.load(Ordering::SeqCst) {
            return Err(storage_types::StorageEnum::Attempt(
                storage_types::AttemptFailure::Conflict(storage_types::ConflictCause::Logical(
                    "test conflict at commit",
                )),
            )
            .into());
        }
        let countdown = self.reject_capacity_after.load(Ordering::SeqCst);
        if countdown > 0 {
            self.reject_capacity_after
                .store(countdown - 1, Ordering::SeqCst);
            if countdown == 1 {
                return Err(storage_types::AttemptFailure::CAPACITY.into());
            }
        }
        let barrier = self.commit_barrier.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                .await
                .map_err(|_| storage_types::StorageError::internal("commits did not overlap"))?;
        }
        if self.hold_next_commit.swap(false, Ordering::SeqCst) {
            self.held.notify_one();
            tokio::time::timeout(Duration::from_secs(20), self.release.notified())
                .await
                .map_err(|_| storage_types::StorageError::internal("held commit timed out"))?;
        }
        Ok(())
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
impl sql_backend::SqlFaultProbe for ClaimProbe {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        // The written table: `UPDATE t`, `INSERT INTO t`, `DELETE FROM t`.
        let target = match call.sql.split_whitespace().next() {
            Some("UPDATE") => call.sql.split_whitespace().nth(1),
            Some("INSERT" | "DELETE") => call.sql.split_whitespace().nth(2),
            _ => None,
        };
        if target.is_some_and(|table| table.contains("sys_pubsub_delivery_state")) {
            self.state_writes.fetch_add(1, Ordering::SeqCst);
        }
        if target.is_some_and(|table| table.contains("sys_pubsub_delivery_payload")) {
            self.payload_writes.fetch_add(1, Ordering::SeqCst);
        }
        if call.operation == sql_backend::SqlFaultOperation::Query
            && call.sql.starts_with("SELECT incarnation")
            && call.sql.contains("receipt_handle IS NULL OR due_at <=")
        {
            self.claims.fetch_add(1, Ordering::SeqCst);
        }
        if call.operation == sql_backend::SqlFaultOperation::Query
            && call.sql.starts_with("SELECT incarnation, message_id")
            && call.sql.contains("AND message_id =")
        {
            self.point_reads.fetch_add(1, Ordering::SeqCst);
        }
        if call.operation == sql_backend::SqlFaultOperation::Query
            && self.fail_next_read.swap(false, Ordering::SeqCst)
        {
            return Some(read_conflict());
        }
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == sql_backend::SqlCommitBoundary::BeforeCommit {
                self.at_commit().await?;
            }
            Ok(())
        })
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for ClaimProbe {
    fn fail(&self, call: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        // A staged mutation is local until the commit; only reads are
        // sequential round trips (waves).
        if !matches!(
            call.operation,
            kv_backend::KvFaultOperation::Stage | kv_backend::KvFaultOperation::StageVersionstamped
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        } else {
            self.mutations.fetch_add(call.mutations, Ordering::SeqCst);
            self.state_writes
                .fetch_add(call.mutations / 2, Ordering::SeqCst);
        }
        if call.operation == kv_backend::KvFaultOperation::ScanBatch {
            self.claims.fetch_add(1, Ordering::SeqCst);
        }
        if call.operation == kv_backend::KvFaultOperation::GetBatch {
            self.point_reads.fetch_add(1, Ordering::SeqCst);
        }
        if !matches!(
            call.operation,
            kv_backend::KvFaultOperation::Stage | kv_backend::KvFaultOperation::StageVersionstamped
        ) && self.fail_next_read.swap(false, Ordering::SeqCst)
        {
            return Some(read_conflict());
        }
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == kv_backend::KvCommitBoundary::BeforeCommit {
                self.at_commit().await?;
            }
            Ok(())
        })
    }
}

fn read_conflict() -> storage_types::StorageError {
    storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
        storage_types::ConflictCause::Logical("test conflict at a native read"),
    ))
    .into()
}

pub(super) async fn probed_store() -> (TestStore, Arc<ClaimProbe>) {
    let probe = Arc::new(ClaimProbe::default());
    let store = TestStore::new_with_native_probes(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    (store, probe)
}

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

async fn send(store: &TestStore, queue_url: &str, body: &str, delay: Option<u32>) {
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.to_owned(),
                message_body: body.to_owned(),
                delay_seconds: delay.map(i64::from),
                message_attributes: None,
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .expect("send message");
}

async fn send_many(store: &TestStore, queue_url: &str, count: usize) {
    let messages = (0..count)
        .map(|index| {
            queue_message_for_send(
                SendMessageRequest {
                    queue_url: queue_url.to_owned(),
                    message_body: format!("m{index}"),
                    delay_seconds: None,
                    message_attributes: None,
                    ..Default::default()
                },
                store.now(),
            )
        })
        .collect::<Vec<_>>();
    // The provider takes a send batch of any size; large batches keep the
    // setup to a few commits on providers that sync each commit (Turso).
    for chunk in messages.chunks(250) {
        for result in store
            .queue_operations
            .send_messages(chunk.to_vec())
            .await
            .expect("send batch")
        {
            result.expect("send entry");
        }
    }
}

async fn receive(store: &TestStore, queue_url: &str, max: u32) -> ReceivedMessages {
    store
        .queue_operations
        .receive_messages(queue_url, max, Some(DurationSeconds::from(30)), None)
        .await
        .expect("receive")
}

fn is_receipt_handle_invalid(error: &QueueError) -> bool {
    matches!(
        error,
        QueueError::Validation {
            kind: QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
            ..
        }
    ) && error.aws_query_status_code() == 404
}

/// Test 1: 16 receivers drain 200 messages. Every response is `Ok`, each
/// message is delivered once, and the final receive is empty. On Postgres
/// (`SKIP LOCKED`) and the serialized writers there is no conflict retry. On
/// FoundationDB a first attempt takes a random slot while two or more full
/// slots are due (W2-F6), so only the last messages make first attempts
/// collide (about 30 to 60 retries in 280 to 310 claims; 221 in 470 before).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_200_messages_when_16_receivers_drain_then_each_is_delivered_once_without_error() {
    let (store, probe) = probed_store().await;
    let store = Arc::new(store);
    let queue_url = queue(&store, "claim_drain").await;
    send_many(&store, &queue_url, 200).await;
    probe.reset();
    let receives = Arc::new(AtomicUsize::new(0));
    let consumers = (0..16)
        .map(|_| {
            let store = Arc::clone(&store);
            let queue_url = queue_url.clone();
            let receives = Arc::clone(&receives);
            tokio::spawn(async move {
                let mut taken = Vec::new();
                let mut empty = 0;
                while empty < 3 {
                    receives.fetch_add(1, Ordering::SeqCst);
                    let received = receive(&store, &queue_url, 1).await;
                    if received.messages.is_empty() {
                        empty += 1;
                    } else {
                        empty = 0;
                        taken.extend(received.messages);
                    }
                }
                taken
            })
        })
        .collect::<Vec<_>>();
    let mut ids = HashSet::new();
    for consumer in consumers {
        // A hang guard, not a speed bound: the assertions count deliveries and
        // conflicts, and a loaded machine or slow disk syncs must not fail it.
        for message in tokio::time::timeout(Duration::from_secs(60), consumer)
            .await
            .expect("consumers hung")
            .unwrap()
        {
            assert!(ids.insert(message.message_id), "message delivered twice");
        }
    }
    assert_eq!(ids.len(), 200, "a message was not delivered");
    assert!(receive(&store, &queue_url, 10).await.messages.is_empty());
    let receives = receives.load(Ordering::SeqCst) + 1;
    let retries = probe.claims().saturating_sub(receives);
    eprintln!(
        "claim counters: backend={} receives={receives} claims={} retries={retries}",
        TestStore::selected_backend(),
        probe.claims()
    );
    let backend = TestStore::selected_backend();
    if backend != "foundationdb" {
        assert_eq!(retries, 0, "{backend}: {retries} conflict retries");
    }
    Arc::into_inner(store).unwrap().finish().await;
}

/// Test 7: on a deep queue (more due entries than the claim window) each
/// claim takes a random aligned slot, so 16 receivers conflict on less than
/// half of their claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_deep_queue_when_16_receivers_claim_then_conflict_retries_are_under_half() {
    let (store, probe) = probed_store().await;
    let store = Arc::new(store);
    let queue_url = queue(&store, "claim_deep").await;
    send_many(
        &store,
        &queue_url,
        queue_provider::QUEUE_CLAIM_WINDOW.get() as usize + 976,
    )
    .await;
    probe.reset();
    // Turso syncs its log on each commit (no Relaxed class), so its claims
    // serialize at one log sync each. Fewer receives there keep the test
    // short; the queue stays deeper than the window, and the other providers
    // keep 20 receives for each of the 16 receivers.
    let receives_each = if TestStore::selected_backend() == "turso" {
        5
    } else {
        20
    };
    let consumers = (0..16)
        .map(|_| {
            let store = Arc::clone(&store);
            let queue_url = queue_url.clone();
            tokio::spawn(async move {
                let mut taken = Vec::new();
                for _ in 0..receives_each {
                    taken.extend(receive(&store, &queue_url, 1).await.messages);
                }
                taken
            })
        })
        .collect::<Vec<_>>();
    let mut ids = HashSet::new();
    for consumer in consumers {
        // A hang guard, not a speed bound: the assertions count deliveries and
        // conflicts, and a loaded machine or slow disk syncs must not fail it.
        for message in tokio::time::timeout(Duration::from_secs(60), consumer)
            .await
            .expect("consumers hung")
            .unwrap()
        {
            assert!(ids.insert(message.message_id), "message delivered twice");
        }
    }
    let receives = 16 * receives_each;
    assert_eq!(ids.len(), receives, "a receive of a deep queue was empty");
    let retries = probe.claims().saturating_sub(receives);
    eprintln!(
        "deep claim counters: backend={} receives={receives} claims={} retries={retries}",
        TestStore::selected_backend(),
        probe.claims()
    );
    assert!(
        retries * 2 < probe.claims(),
        "{retries} conflict retries for {} claims",
        probe.claims()
    );
    Arc::into_inner(store).unwrap().finish().await;
}

/// One receive of one message is one claim attempt with a fixed number of
/// native calls; a second receive of the drained queue is one more attempt.
#[tokio::test]
async fn given_one_message_when_received_then_one_attempt_claims_it_and_reports_drained() {
    let (store, probe) = probed_store().await;
    let queue_url = queue(&store, "claim_once").await;
    send(&store, &queue_url, "only", None).await;
    send(&store, &queue_url, "later", Some(20)).await;
    probe.reset();
    let received = receive(&store, &queue_url, 1).await;
    assert_eq!(received.messages.len(), 1);
    assert!(received.drained, "no other message is due");
    assert_eq!(
        received.next_due_in,
        Some(Duration::from_secs(20)),
        "the delayed message is due before the 30 s lease that this claim gave"
    );
    assert_eq!(probe.claims(), 1);
    // Catalog read, the claim wave, and the claim write (SQL: 1 + 3 + 1
    // statements; KV: catalog reads, one scan batch, message reads, staging).
    let calls = probe.calls();
    eprintln!(
        "one receive: backend={} attempts={} native_calls={calls}",
        TestStore::selected_backend(),
        probe.claims()
    );
    let again = receive(&store, &queue_url, 1).await;
    assert!(again.messages.is_empty() && again.drained);
    assert_eq!(
        again.next_due_in,
        Some(Duration::from_secs(20)),
        "an empty claim keeps the known due time"
    );
    assert_eq!(probe.claims(), 2);
    assert!(calls <= 8, "{calls} native calls for one receive");
    store.finish().await;
}

/// W7-A: a claim without a dead-letter move is not budgeted, so ten due 1 MB
/// messages are all claimed in one commit (each writes about 150 B of state).
/// On FoundationDB this is the proof that ten 1 MB claims commit.
#[tokio::test]
async fn given_ten_one_mb_messages_when_ten_are_received_then_all_ten_are_claimed() {
    let (store, _probe) = probed_store().await;
    let queue_url = queue(&store, "claim_budget").await;
    let body = "m".repeat(1_000_000);
    for _ in 0..10 {
        send(&store, &queue_url, &body, None).await;
    }
    let first = receive(&store, &queue_url, 10).await;
    assert_eq!(first.messages.len(), 10);
    assert!(first.messages.iter().all(|m| m.body.len() == 1_000_000));
    let second = receive(&store, &queue_url, 10).await;
    assert!(second.messages.is_empty() && second.drained);
    store.finish().await;
}

/// Test 8 (D4): a claim that loses every conflict retry returns an empty,
/// undrained receive, and the message stays receivable.
#[tokio::test]
async fn given_every_claim_commit_conflicts_when_received_then_answer_is_empty_and_message_stays() {
    let (store, probe) = probed_store().await;
    let queue_url = queue(&store, "claim_contention").await;
    send(&store, &queue_url, "contended", None).await;
    probe.fail_commits.store(true, Ordering::SeqCst);
    let received = receive(&store, &queue_url, 1).await;
    assert!(received.messages.is_empty());
    assert!(!received.drained, "a lost conflict is not a drained queue");
    probe.fail_commits.store(false, Ordering::SeqCst);
    let again = receive(&store, &queue_url, 1).await;
    assert_eq!(again.messages.len(), 1);
    assert_eq!(again.messages[0].body, "contended");
    store.finish().await;
}

/// Test 9 (D6): each count region stops at its bound.
#[tokio::test]
async fn given_25_visible_and_25_delayed_when_counted_with_bound_10_then_each_region_stops_at_10() {
    let store = TestStore::new().await;
    let queue_url = queue(&store, "claim_counts").await;
    for index in 0..25 {
        send(&store, &queue_url, &format!("v{index}"), None).await;
        send(&store, &queue_url, &format!("d{index}"), Some(60)).await;
    }
    let bounded = (store.bounded_queue_counts)(queue_url.clone(), NonZeroU32::new(10).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (bounded.visible, bounded.not_visible, bounded.delayed),
        (10, 0, 10)
    );
    let exact = (store.bounded_queue_counts)(queue_url, NonZeroU32::new(1_000).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (exact.visible, exact.not_visible, exact.delayed),
        (25, 0, 25)
    );
    store.finish().await;
}

/// Test 10 (D7, C2): a stale receipt deletes nothing and succeeds; a stale
/// visibility change is `InvalidParameterValue`; a malformed handle is
/// `ReceiptHandleIsInvalid`; batch entries follow the same rules.
#[tokio::test]
async fn given_stale_and_malformed_receipts_when_used_then_sqs_answers_and_current_claim_stays() {
    let store = TestStore::new().await;
    let queue_url = queue(&store, "claim_receipts").await;
    send(&store, &queue_url, "held", None).await;
    let first = receive(&store, &queue_url, 1).await.messages;
    store
        .queue_operations
        .change_message_visibility(
            &queue_url,
            ReceiptHandle::from(first[0].receipt_handle.as_str()),
            DurationSeconds::from(0),
        )
        .await
        .unwrap();
    let second: Vec<MessageResponse> = receive(&store, &queue_url, 1).await.messages;
    assert_eq!(second.len(), 1, "visibility zero redelivers at once");
    let stale = ReceiptHandle::from(first[0].receipt_handle.as_str());
    let malformed = ReceiptHandle::from("not-a-receipt");

    store
        .queue_operations
        .delete_message(&queue_url, stale.clone())
        .await
        .expect("a stale delete succeeds");
    let visibility = store
        .queue_operations
        .change_message_visibility(&queue_url, stale.clone(), DurationSeconds::from(5))
        .await
        .unwrap_err();
    assert_eq!(visibility.aws_query_error_type(), "InvalidParameterValue");
    assert_eq!(visibility.aws_query_status_code(), 400);
    assert_eq!(
        visibility.aws_query_message(),
        "Message does not exist or is not available for visibility timeout change"
    );
    let malformed_delete = store
        .queue_operations
        .delete_message(&queue_url, malformed.clone())
        .await
        .unwrap_err();
    assert!(is_receipt_handle_invalid(&malformed_delete));

    let batch = store
        .queue_operations
        .delete_messages(&queue_url, vec![stale.clone(), malformed.clone()])
        .await
        .unwrap();
    assert!(batch[0].is_ok());
    assert!(is_receipt_handle_invalid(batch[1].as_ref().unwrap_err()));
    let batch = store
        .queue_operations
        .change_message_visibilities(
            &queue_url,
            vec![
                (stale, DurationSeconds::from(5)),
                (malformed, DurationSeconds::from(5)),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        batch[0].as_ref().unwrap_err().aws_query_error_type(),
        "InvalidParameterValue"
    );
    assert!(is_receipt_handle_invalid(batch[1].as_ref().unwrap_err()));

    // The current claim is untouched: its own receipt still deletes it.
    store
        .queue_operations
        .delete_message(
            &queue_url,
            ReceiptHandle::from(second[0].receipt_handle.as_str()),
        )
        .await
        .unwrap();
    let counts = (store.bounded_queue_counts)(queue_url, NonZeroU32::new(100).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (counts.visible, counts.not_visible, counts.delayed),
        (0, 0, 0)
    );
    store.finish().await;
}

pub(super) fn concurrent_backend() -> bool {
    matches!(
        TestStore::selected_backend().as_str(),
        "postgres" | "foundationdb"
    )
}

/// Test 2 (R4): a send that commits while a claim waits at its commit does
/// not abort the claim (no lock or conflict range on the due index).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_claim_held_at_commit_when_a_send_commits_then_the_claim_commits_without_retry() {
    assert!(concurrent_backend(), "select postgres or foundationdb");
    let (store, probe) = probed_store().await;
    let store = Arc::new(store);
    let queue_url = queue(&store, "claim_send").await;
    send(&store, &queue_url, "first", None).await;
    probe.reset();
    probe.hold_next_commit.store(true, Ordering::SeqCst);
    let claim = {
        let store = Arc::clone(&store);
        let queue_url = queue_url.clone();
        tokio::spawn(async move { receive(&store, &queue_url, 1).await })
    };
    probe.held.notified().await;
    send(&store, &queue_url, "second", None).await;
    probe.release.notify_one();
    let claimed = claim.await.unwrap();
    assert_eq!(claimed.messages.len(), 1);
    assert_eq!(probe.claims(), 1, "the send aborted the claim");
    Arc::into_inner(store).unwrap().finish().await;
}

/// Test 3 (R4, D18 H4): two receivers race for one message while one waits
/// at its commit: one gets the message, the other is empty, neither fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_two_receivers_and_one_message_when_one_is_held_at_commit_then_one_delivery_and_no_error()
 {
    assert!(concurrent_backend(), "select postgres or foundationdb");
    let (store, probe) = probed_store().await;
    let store = Arc::new(store);
    let queue_url = queue(&store, "claim_race").await;
    send(&store, &queue_url, "one", None).await;
    probe.hold_next_commit.store(true, Ordering::SeqCst);
    let held = {
        let store = Arc::clone(&store);
        let queue_url = queue_url.clone();
        tokio::spawn(async move { receive(&store, &queue_url, 1).await })
    };
    probe.held.notified().await;
    let other = receive(&store, &queue_url, 1).await;
    probe.release.notify_one();
    let held = held.await.unwrap();
    assert_eq!(
        held.messages.len() + other.messages.len(),
        1,
        "the message was delivered twice or not at all"
    );
    Arc::into_inner(store).unwrap().finish().await;
}

/// Test 4 (R3, D1): sends read the catalog without a lock, so two sends to one
/// queue are in their transactions at the same time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_two_sends_to_one_queue_when_both_reach_commit_then_their_transactions_overlap() {
    assert!(concurrent_backend(), "select postgres or foundationdb");
    let (store, probe) = probed_store().await;
    let store = Arc::new(store);
    let queue_url = queue(&store, "send_overlap").await;
    *probe.commit_barrier.lock().unwrap() = Some(Arc::new(tokio::sync::Barrier::new(2)));
    let sends = (0..2)
        .map(|index| {
            let store = Arc::clone(&store);
            let queue_url = queue_url.clone();
            tokio::spawn(async move {
                store
                    .queue_operations
                    .send_message(queue_message_for_send(
                        SendMessageRequest {
                            queue_url,
                            message_body: format!("overlap-{index}"),
                            delay_seconds: None,
                            message_attributes: None,
                            ..Default::default()
                        },
                        store.now(),
                    ))
                    .await
            })
        })
        .collect::<Vec<_>>();
    for send in sends {
        send.await.unwrap().expect("overlapping send");
    }
    *probe.commit_barrier.lock().unwrap() = None;
    assert_eq!(receive(&store, &queue_url, 10).await.messages.len(), 2);
    Arc::into_inner(store).unwrap().finish().await;
}

/// Ordered-key stores compare the stored bytes of a message with the bytes of
/// the record that the attempt read. Message attributes are a hash map, so the
/// stored order must not depend on one decode: ten receives of a message with
/// eight attributes each take exactly one attempt.
#[tokio::test]
async fn given_message_with_attributes_when_received_repeatedly_then_each_claim_is_one_attempt() {
    let (store, probe) = probed_store().await;
    let queue_url = queue(&store, "claim_attributes").await;
    let attributes = (0..8)
        .map(|index| {
            (
                format!("name{index}"),
                queue_provider::MessageAttributeValue {
                    data_type: "String".to_owned(),
                    string_value: Some(format!("value{index}")),
                    binary_value: None,
                },
            )
        })
        .collect();
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "attributes".to_owned(),
                delay_seconds: None,
                message_attributes: Some(attributes),
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .unwrap();
    probe.reset();
    for _ in 0..10 {
        let received = store
            .queue_operations
            .receive_messages(&queue_url, 1, Some(DurationSeconds::from(0)), None)
            .await
            .unwrap();
        assert_eq!(received.messages.len(), 1);
    }
    assert_eq!(probe.claims(), 10, "a claim retried a conflict");
    store.finish().await;
}

/// Defect 2 of the FoundationDB simulation: a catalog read that fails with a
/// native conflict runs again under the native retry; `GetQueueUrl` never
/// gives the client a retryable native error.
#[tokio::test]
async fn given_a_catalog_read_fails_with_a_native_conflict_when_queue_url_is_read_then_the_read_is_retried()
 {
    let (store, probe) = probed_store().await;
    let name = format!("read_conflict_{}", uuid::Uuid::new_v4().simple());
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("http://localhost/000000000000/{name}"),
            queue_name: name.clone(),
            attributes: HashMap::new(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .expect("create queue");
    probe.fail_next_read.store(true, Ordering::SeqCst);
    let url = store
        .queue_operations
        .get_queue_url_by_name(&name)
        .await
        .expect("a retryable native error reached the caller");
    assert!(
        !probe.fail_next_read.load(Ordering::SeqCst),
        "no native read was injected"
    );
    assert_eq!(
        url.as_deref().map(queue_provider::queue_name_of),
        Some(name.as_str())
    );
}
