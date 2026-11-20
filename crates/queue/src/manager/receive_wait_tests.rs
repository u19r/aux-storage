//! Waiting receives over a real in-memory SQLite store whose attempts are
//! counted by mode.
//!
//! Tests that prove a local wake use a probe interval far longer than the
//! test budget, so a probe cannot deliver the message in their place.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use queue_provider::{
    CreateQueueRequest, DeleteQueueRequest, QueueError, QueuePort, ReceiveMessageRequest,
    ReceiveMessageResponse, SendMessageBatchRequest, SendMessageBatchRequestEntry,
    SendMessageRequest, SetQueueAttributesRequest,
};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::{AttemptMode, AttemptRequest, Clock, StorageResult, TimestampMillis};

use super::QueueManager;
use crate::constants::{RECEIVE_PROBE_INTERVAL, RECEIVE_WAKE_SPACING};

/// Longer than any test budget: only a local wake can deliver in time.
const NO_PROBE_INTERVAL: Duration = Duration::from_secs(60);
const TEST_BUDGET: Duration = Duration::from_secs(10);

type Hook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Counts native attempts by mode, tracks overlapping write attempts, and can
/// run one hook after the first read-only probe commits, before its result
/// reaches the waiter.
struct CountingPort {
    inner: SqlStore<SqliteBackend>,
    read_only: AtomicUsize,
    read_write: AtomicUsize,
    writes_in_flight: AtomicUsize,
    max_writes_in_flight: AtomicUsize,
    after_first_probe: OnceLock<Hook>,
    hook_ran: AtomicBool,
    /// Relaxed write attempts (claims, receipts) that fail with a native
    /// conflict before they start.
    conflicting_writes: AtomicUsize,
    /// Tokio milliseconds that each read-only probe takes (a slow store).
    probe_delay_ms: AtomicU64,
    /// `None`: the store's own board is reported (no override). `Some(board)`
    /// replaces it, `Some(None)` removes it (interval rule).
    board: Option<Option<Arc<queue_provider::QueueWakeBoard>>>,
    /// Attempts inside `with_attempt` now (real I/O may be running).
    in_flight: AtomicUsize,
    /// Write attempts that report commit-unknown after the inner attempt
    /// returned Ok (the row is kept).
    commit_unknown_writes: AtomicUsize,
}

struct InFlight<'a>(&'a AtomicUsize);

impl<'a> InFlight<'a> {
    fn enter(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Deadlines that virtual time cannot expire while real I/O runs.
fn wide_deadlines() -> storage_types::AttemptDeadlines {
    let day = Duration::from_secs(86_400);
    storage_types::AttemptDeadlines::try_new([day; storage_types::DeadlineClass::COUNT], day)
        .unwrap()
}

impl CountingPort {
    fn counts(&self) -> (usize, usize) {
        (
            self.read_only.load(Ordering::SeqCst),
            self.read_write.load(Ordering::SeqCst),
        )
    }

    fn reset(&self) {
        self.read_only.store(0, Ordering::SeqCst);
        self.read_write.store(0, Ordering::SeqCst);
        self.max_writes_in_flight.store(0, Ordering::SeqCst);
    }
}

impl Clock for CountingPort {
    fn now(&self) -> TimestampMillis {
        self.inner.now()
    }
}

impl storage_types::AttemptDeadlineSource for CountingPort {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        wide_deadlines()
    }
}

impl QueuePort for CountingPort {
    type Attempt<'native>
        = <SqlStore<SqliteBackend> as QueuePort>::Attempt<'native>
    where Self: 'native;

    fn queue_wake_board(&self) -> Option<Arc<queue_provider::QueueWakeBoard>> {
        match &self.board {
            Some(board) => board.clone(),
            None => None,
        }
    }

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
        let _in_flight = InFlight::enter(&self.in_flight);
        if request.mode == AttemptMode::ReadOnly {
            self.read_only.fetch_add(1, Ordering::SeqCst);
            let delay = self.probe_delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        } else {
            self.read_write.fetch_add(1, Ordering::SeqCst);
            if request.durability == storage_types::AttemptDurability::Relaxed
                && self
                    .conflicting_writes
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(storage_types::StorageEnum::Attempt(
                    storage_types::AttemptFailure::Conflict(storage_types::ConflictCause::Logical(
                        "test conflict",
                    )),
                )
                .into());
            }
            let in_flight = self.writes_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_writes_in_flight
                .fetch_max(in_flight, Ordering::SeqCst);
        }
        let result = self.inner.with_attempt(request, operation).await;
        if request.mode == AttemptMode::ReadOnly {
            if let Some(hook) = self.after_first_probe.get()
                && !self.hook_ran.swap(true, Ordering::SeqCst)
            {
                hook().await;
            }
        } else {
            self.writes_in_flight.fetch_sub(1, Ordering::SeqCst);
            if result.is_ok()
                && self
                    .commit_unknown_writes
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(storage_types::StorageError::commit_unknown(
                    "test commit unknown",
                ));
            }
        }
        result
    }
}

struct Fixture {
    port: Arc<CountingPort>,
    manager: Arc<QueueManager>,
    /// A second process over the same store, with its own waiters.
    other_node: Arc<QueueManager>,
}

async fn fixture(probe_interval: Duration, max_waiting: Option<usize>) -> Fixture {
    build_fixture(probe_interval, max_waiting, None).await
}

/// Replaces the store's board with `board` (`None`: no board, interval rule).
async fn fixture_with_board(
    probe_interval: Duration,
    board: Option<Arc<queue_provider::QueueWakeBoard>>,
) -> Fixture {
    build_fixture(probe_interval, None, Some(board)).await
}

/// The SQLite store's own (local) board.
async fn fixture_with_local_board(probe_interval: Duration) -> Fixture {
    let driver = SqliteBackend::initialize(":memory:", wide_deadlines())
        .await
        .unwrap()
        .without_deadline_timers();
    let inner =
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap();
    let board = inner.queue_wake_board();
    assert!(board.is_some(), "SQLite reports a local board");
    assemble(inner, probe_interval, None, Some(board))
}

async fn build_fixture(
    probe_interval: Duration,
    max_waiting: Option<usize>,
    board: Option<Option<Arc<queue_provider::QueueWakeBoard>>>,
) -> Fixture {
    // Paused Tokio time must not expire the real blocking SQLite wait.
    let driver = SqliteBackend::initialize(":memory:", wide_deadlines())
        .await
        .unwrap()
        .without_deadline_timers();
    let inner =
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap();
    assemble(inner, probe_interval, max_waiting, board)
}

fn assemble(
    inner: SqlStore<SqliteBackend>,
    probe_interval: Duration,
    max_waiting: Option<usize>,
    board: Option<Option<Arc<queue_provider::QueueWakeBoard>>>,
) -> Fixture {
    let port = Arc::new(CountingPort {
        inner,
        read_only: AtomicUsize::new(0),
        read_write: AtomicUsize::new(0),
        writes_in_flight: AtomicUsize::new(0),
        max_writes_in_flight: AtomicUsize::new(0),
        after_first_probe: OnceLock::new(),
        hook_ran: AtomicBool::new(false),
        conflicting_writes: AtomicUsize::new(0),
        probe_delay_ms: AtomicU64::new(0),
        board,
        in_flight: AtomicUsize::new(0),
        commit_unknown_writes: AtomicUsize::new(0),
    });
    let mut manager = QueueManager::new(port.clone()).with_receive_probe_interval(probe_interval);
    if let Some(max_waiting) = max_waiting {
        manager = manager.with_max_waiting_receives(max_waiting);
    }
    Fixture {
        other_node: Arc::new(QueueManager::new(port.clone())),
        port,
        manager: Arc::new(manager),
    }
}

impl Fixture {
    async fn queue(&self, name: &str, attributes: &[(&str, &str)]) -> String {
        let attributes = (!attributes.is_empty()).then(|| {
            attributes
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect::<HashMap<_, _>>()
        });
        self.manager
            .create_queue(CreateQueueRequest {
                queue_name: name.to_owned(),
                attributes,
                ..Default::default()
            })
            .await
            .unwrap()
            .queue_url
    }

    async fn send_through(manager: &QueueManager, queue_url: &str, body: &str, delay: Option<u32>) {
        manager
            .send_message(SendMessageRequest {
                queue_url: queue_url.to_owned(),
                message_body: body.to_owned(),
                delay_seconds: delay.map(i64::from),
                message_attributes: None,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    async fn send(&self, queue_url: &str, body: &str, delay: Option<u32>) {
        Self::send_through(&self.manager, queue_url, body, delay).await;
    }

    fn try_receive(
        &self,
        queue_url: &str,
        max_messages: u32,
        wait_time_seconds: Option<u32>,
    ) -> tokio::task::JoinHandle<Result<ReceiveMessageResponse, QueueError>> {
        let manager = Arc::clone(&self.manager);
        let request = ReceiveMessageRequest {
            queue_url: queue_url.to_owned(),
            max_number_of_messages: Some(max_messages.into()),
            visibility_timeout: None,
            wait_time_seconds: wait_time_seconds.map(i64::from),
            attribute_names: None,
            message_attribute_names: None,
        };
        tokio::spawn(async move { manager.receive_message(request).await })
    }

    async fn receive(
        &self,
        queue_url: &str,
        max_messages: u32,
        wait_time_seconds: Option<u32>,
    ) -> ReceiveMessageResponse {
        tokio::time::timeout(
            TEST_BUDGET,
            self.try_receive(queue_url, max_messages, wait_time_seconds),
        )
        .await
        .expect("receive exceeded the test budget")
        .unwrap()
        .unwrap()
    }

    async fn until_waiting(&self, waiting: usize) {
        tokio::time::timeout(TEST_BUDGET, async {
            while self.manager.waiting_receives() != waiting {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("waiting receives never reached {waiting}"));
    }
}

async fn within_budget<T>(task: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(TEST_BUDGET, task)
        .await
        .expect("task exceeded the test budget")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_200_waiters_on_empty_queues_when_wait_expires_then_no_write_and_one_probe_schedule_per_queue()
 {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let mut queues = Vec::new();
    for index in 0..4 {
        queues.push(fixture.queue(&format!("idle-{index}"), &[]).await);
    }
    fixture.port.reset();
    let started = Instant::now();
    let waiters = (0..200)
        .map(|index| fixture.try_receive(&queues[index % queues.len()], 10, Some(1)))
        .collect::<Vec<_>>();
    for waiter in waiters {
        assert!(within_budget(waiter).await.unwrap().messages.is_empty());
    }
    let elapsed = started.elapsed();
    let (read_only, read_write) = fixture.port.counts();
    assert_eq!(read_write, 0, "waiting receives opened write attempts");
    let per_queue = (elapsed.as_millis() / RECEIVE_PROBE_INTERVAL.as_millis()) as usize + 1;
    assert!(
        read_only <= queues.len() * per_queue,
        "{read_only} probes for {} queues in {elapsed:?}",
        queues.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_empty_queue_when_short_polled_in_a_loop_then_no_write_and_probes_are_spaced() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("short", &[]).await;
    fixture.port.reset();
    let started = Instant::now();
    for _ in 0..100 {
        assert!(
            fixture
                .receive(&queue_url, 10, Some(0))
                .await
                .messages
                .is_empty()
        );
    }
    let elapsed = started.elapsed();
    let (read_only, read_write) = fixture.port.counts();
    assert_eq!(read_write, 0);
    let bound = (elapsed.as_millis() / RECEIVE_PROBE_INTERVAL.as_millis()) as usize + 1;
    assert!(read_only <= bound, "{read_only} probes in {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_queue_wait_default_when_request_omits_wait_then_receive_waits_without_write() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture
        .queue("default-wait", &[("ReceiveMessageWaitTimeSeconds", "20")])
        .await;
    fixture.port.reset();
    let waiter = fixture.try_receive(&queue_url, 1, None);
    fixture.until_waiting(1).await;
    assert_eq!(fixture.port.counts().1, 0);
    waiter.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_waiting_receive_when_same_process_sends_then_local_wake_delivers_it() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("wake", &[]).await;
    fixture.port.reset();
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    fixture.until_waiting(1).await;
    fixture.send(&queue_url, "woken", None).await;
    let response = within_budget(waiter).await.unwrap();
    assert_eq!(response.messages[0].body, "woken");
    // One probe before the wait, then the send and one claim.
    assert_eq!(fixture.port.counts(), (1, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_send_commits_after_empty_probe_before_wait_then_wakeup_is_not_missed() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("race", &[]).await;
    let hook: Hook = {
        let manager = Arc::clone(&fixture.manager);
        let queue_url = queue_url.clone();
        Arc::new(move || {
            let manager = Arc::clone(&manager);
            let queue_url = queue_url.clone();
            // The probe saw an empty queue; this commit lands before the waiter
            // records that observation and starts to wait.
            Box::pin(async move {
                Fixture::send_through(&manager, &queue_url, "raced", None).await;
            })
        })
    };
    let _ = fixture.port.after_first_probe.set(hook);
    fixture.port.reset();
    let response = fixture.receive(&queue_url, 1, Some(20)).await;
    assert_eq!(response.messages[0].body, "raced");
    assert_eq!(fixture.port.counts().0, 1, "delivery needed another probe");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_delayed_message_when_waiting_then_waiter_claims_once_at_due_time() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("delayed", &[]).await;
    fixture.port.reset();
    let waiter = fixture.try_receive(&queue_url, 1, Some(5));
    fixture.until_waiting(1).await;
    fixture.send(&queue_url, "later", Some(1)).await;
    let response = within_budget(waiter).await.unwrap();
    assert_eq!(response.messages[0].body, "later");
    // An early claim would be one more write attempt.
    assert_eq!(fixture.port.counts(), (1, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_queue_receive_attributes_when_request_omits_them_then_queue_values_apply() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture
        .queue(
            "defaults",
            &[
                ("VisibilityTimeout", "1"),
                ("ReceiveMessageWaitTimeSeconds", "1"),
            ],
        )
        .await;

    let started = Instant::now();
    assert!(
        fixture
            .receive(&queue_url, 1, None)
            .await
            .messages
            .is_empty()
    );
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "queue wait ignored"
    );

    fixture.send(&queue_url, "lease", None).await;
    assert_eq!(fixture.receive(&queue_url, 1, None).await.messages.len(), 1);
    assert!(
        fixture
            .receive(&queue_url, 1, Some(0))
            .await
            .messages
            .is_empty(),
        "message visible inside its lease"
    );
    // A 30 second default lease would outlast this 5 second wait.
    let again = fixture.receive(&queue_url, 1, Some(5)).await;
    assert_eq!(again.messages[0].body, "lease");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_waiting_bound_reached_when_receive_finds_a_message_then_it_still_gets_it() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, Some(1)).await;
    let parked = fixture.queue("parked", &[]).await;
    let busy = fixture.queue("busy", &[]).await;
    let first = fixture.try_receive(&parked, 1, Some(20));
    fixture.until_waiting(1).await;
    Fixture::send_through(&fixture.other_node, &busy, "available", None).await;
    let response = fixture.receive(&busy, 1, Some(20)).await;
    assert_eq!(response.messages[0].body, "available");
    // Over the bound an empty queue returns at once instead of waiting.
    let started = Instant::now();
    assert!(
        fixture
            .receive(&busy, 1, Some(20))
            .await
            .messages
            .is_empty()
    );
    assert!(started.elapsed() < Duration::from_secs(20));
    first.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_dropped_waiting_receive_when_bound_is_one_then_slot_is_released() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, Some(1)).await;
    let queue_url = fixture.queue("dropped", &[]).await;
    let first = fixture.try_receive(&queue_url, 1, Some(20));
    fixture.until_waiting(1).await;
    first.abort();
    fixture.until_waiting(0).await;
    let second = fixture.try_receive(&queue_url, 1, Some(20));
    fixture.until_waiting(1).await;
    second.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_busy_queue_when_last_claim_filled_its_page_then_next_receive_claims_without_probe() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("busy", &[]).await;
    for index in 0..3 {
        fixture.send(&queue_url, &format!("m{index}"), None).await;
    }
    fixture.port.reset();
    assert_eq!(
        fixture.receive(&queue_url, 1, Some(0)).await.messages.len(),
        1
    );
    assert_eq!(fixture.port.counts(), (1, 1));
    assert_eq!(
        fixture
            .receive(&queue_url, 1, Some(20))
            .await
            .messages
            .len(),
        1
    );
    assert_eq!(fixture.port.counts(), (1, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_50_waiters_when_one_message_arrives_then_one_claim_delivers_it() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("herd", &[]).await;
    fixture.port.reset();
    let waiters = (0..50)
        .map(|_| fixture.try_receive(&queue_url, 10, Some(2)))
        .collect::<Vec<_>>();
    fixture.until_waiting(50).await;
    fixture.send(&queue_url, "only", None).await;
    let mut delivered = 0;
    for waiter in waiters {
        delivered += within_budget(waiter).await.unwrap().messages.len();
    }
    assert_eq!(delivered, 1);
    let (_, read_write) = fixture.port.counts();
    assert!(read_write <= 3, "{read_write} write attempts");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_50_waiters_when_deep_queue_wakes_them_then_full_pages_are_claimed_in_parallel() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("deep", &[]).await;
    let waiters = (0..50)
        .map(|_| fixture.try_receive(&queue_url, 10, Some(5)))
        .collect::<Vec<_>>();
    fixture.until_waiting(50).await;
    // Another node fills the queue without waking anyone here.
    for batch in 0..50 {
        fixture
            .other_node
            .send_message_batch(SendMessageBatchRequest {
                queue_url: queue_url.clone(),
                entries: (0..10)
                    .map(|index| SendMessageBatchRequestEntry {
                        id: format!("e{index}"),
                        message_body: format!("b{batch}-{index}"),
                        delay_seconds: None,
                        message_attributes: None,
                        ..Default::default()
                    })
                    .collect(),
            })
            .await
            .unwrap();
    }
    fixture.port.reset();
    // One local send wakes the waiters.
    fixture.send(&queue_url, "wake", None).await;
    let mut bodies = HashSet::new();
    for waiter in waiters {
        let messages = within_budget(waiter).await.unwrap().messages;
        assert_eq!(messages.len(), 10);
        for message in messages {
            assert!(bodies.insert(message.body), "message delivered twice");
        }
    }
    assert!(
        fixture.port.max_writes_in_flight.load(Ordering::SeqCst) > 1,
        "claims on a deep queue were serialised"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_two_nodes_when_one_sends_then_a_waiter_on_the_other_receives_by_probe() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("nodes", &[]).await;
    let waiter = fixture.try_receive(&queue_url, 1, Some(5));
    fixture.until_waiting(1).await;
    fixture.port.reset();
    Fixture::send_through(&fixture.other_node, &queue_url, "remote", None).await;
    let response = within_budget(waiter).await.unwrap();
    assert_eq!(response.messages[0].body, "remote");
    assert!(fixture.port.counts().0 >= 1, "no probe found the message");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_waiters_when_queue_is_deleted_then_one_probe_ends_them_with_not_found() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("deleted", &[]).await;
    let waiters = (0..10)
        .map(|_| fixture.try_receive(&queue_url, 1, Some(20)))
        .collect::<Vec<_>>();
    fixture.until_waiting(10).await;
    fixture.port.reset();
    fixture
        .manager
        .delete_queue(DeleteQueueRequest {
            queue_url: queue_url.clone(),
        })
        .await
        .unwrap();
    for waiter in waiters {
        assert!(matches!(
            within_budget(waiter).await,
            Err(QueueError::ResourceNotFound {
                resource_type: "queue",
                ..
            })
        ));
    }
    assert_eq!(fixture.port.counts().0, 1, "each waiter probed alone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_probe_in_flight_when_attributes_change_then_its_stale_defaults_are_not_recorded() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture
        .queue("forget", &[("ReceiveMessageWaitTimeSeconds", "20")])
        .await;
    let hook: Hook = {
        let manager = Arc::clone(&fixture.manager);
        let queue_url = queue_url.clone();
        Arc::new(move || {
            let manager = Arc::clone(&manager);
            let queue_url = queue_url.clone();
            // The probe already read a 20 second default.
            Box::pin(async move {
                manager
                    .set_queue_attributes(SetQueueAttributesRequest {
                        queue_url,
                        attributes: HashMap::from([(
                            "ReceiveMessageWaitTimeSeconds".to_owned(),
                            "0".to_owned(),
                        )]),
                    })
                    .await
                    .unwrap();
            })
        })
    };
    let _ = fixture.port.after_first_probe.set(hook);
    assert!(
        fixture
            .receive(&queue_url, 1, Some(0))
            .await
            .messages
            .is_empty()
    );
    fixture.port.reset();
    // The stale 20 second default would make this receive wait past the budget.
    assert!(
        fixture
            .receive(&queue_url, 1, None)
            .await
            .messages
            .is_empty()
    );
    assert_eq!(fixture.port.counts(), (1, 0));
}

/// W2 D12/D15: with `MaxNumberOfMessages` 1 a successful claim is a full
/// page, but it drained the queue, so one message does not wake every waiter
/// into a write claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_50_single_message_waiters_when_one_message_arrives_then_at_most_3_write_attempts() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("herd-one", &[]).await;
    fixture.port.reset();
    let waiters = (0..50)
        .map(|_| fixture.try_receive(&queue_url, 1, Some(2)))
        .collect::<Vec<_>>();
    fixture.until_waiting(50).await;
    fixture.send(&queue_url, "only", None).await;
    let mut delivered = 0;
    for waiter in waiters {
        delivered += within_budget(waiter).await.unwrap().messages.len();
    }
    assert_eq!(delivered, 1);
    let (_, read_write) = fixture.port.counts();
    // The send and at most two claims.
    assert!(read_write <= 3, "{read_write} write attempts");
}

/// W2 D12/D15: a claim of one message that leaves more due marks the queue
/// deep, so single-message claims of 50 waiters overlap in time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_50_single_message_waiters_when_500_messages_arrive_then_claims_overlap() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("deep-one", &[]).await;
    let waiters = (0..50)
        .map(|_| fixture.try_receive(&queue_url, 1, Some(5)))
        .collect::<Vec<_>>();
    fixture.until_waiting(50).await;
    for batch in 0..50 {
        fixture
            .other_node
            .send_message_batch(SendMessageBatchRequest {
                queue_url: queue_url.clone(),
                entries: (0..10)
                    .map(|index| SendMessageBatchRequestEntry {
                        id: format!("e{index}"),
                        message_body: format!("b{batch}-{index}"),
                        delay_seconds: None,
                        message_attributes: None,
                        ..Default::default()
                    })
                    .collect(),
            })
            .await
            .unwrap();
    }
    fixture.port.reset();
    fixture.send(&queue_url, "wake", None).await;
    let mut bodies = HashSet::new();
    for waiter in waiters {
        let messages = within_budget(waiter).await.unwrap().messages;
        assert_eq!(messages.len(), 1);
        assert!(
            bodies.insert(messages[0].body.clone()),
            "message delivered twice"
        );
    }
    assert!(
        fixture.port.max_writes_in_flight.load(Ordering::SeqCst) > 1,
        "claims on a deep queue were serialised"
    );
}

/// W2 D17 (hole H8): due evidence from a local commit expires after one
/// probe interval. Long after another node took the message, short polls of
/// the idle queue probe and open no write attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_old_local_signal_when_100_short_polls_run_on_an_idle_queue_then_no_write_attempt() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("old-signal", &[]).await;
    // A local waiting receive creates the queue's wait state.
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    fixture.until_waiting(1).await;
    waiter.abort();
    fixture.until_waiting(0).await;
    // A delayed local send leaves due evidence that becomes due later.
    fixture.send(&queue_url, "taken elsewhere", Some(1)).await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let taken = fixture
        .other_node
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.clone(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(60),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert_eq!(taken.messages.len(), 1);
    fixture.port.reset();
    for _ in 0..100 {
        assert!(
            fixture
                .receive(&queue_url, 1, Some(0))
                .await
                .messages
                .is_empty()
        );
    }
    let (read_only, read_write) = fixture.port.counts();
    assert_eq!(read_write, 0, "an old signal opened a write claim");
    assert!(read_only >= 1);
}

/// W2 D4/D15: a claim that loses every conflict retry is empty and not
/// drained; the waiter claims again after the short backoff and receives the
/// message within its wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_claim_loses_every_conflict_when_waiting_then_it_claims_again_after_the_backoff() {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("contended", &[]).await;
    let waiter = fixture.try_receive(&queue_url, 1, Some(5));
    fixture.until_waiting(1).await;
    fixture.port.reset();
    // The provider makes five claim attempts; all five fail.
    fixture.port.conflicting_writes.store(5, Ordering::SeqCst);
    fixture.send(&queue_url, "contended", None).await;
    let started = Instant::now();
    let response = within_budget(waiter).await.unwrap();
    assert_eq!(response.messages[0].body, "contended");
    let (_, read_write) = fixture.port.counts();
    // The send, five failed claim attempts, and one claim after the backoff.
    assert_eq!(read_write, 7, "{read_write} write attempts");
    assert!(started.elapsed() < Duration::from_secs(5));
}

// W12 (design D10): a dead-letter move is a send to the target, so it wakes
// a receive that waits on the target, as a local send does; with a 60 s probe
// interval only the local signal can deliver it in time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_waiter_on_dead_letter_queue_when_a_claim_moves_a_message_then_the_waiter_receives_it()
 {
    let fixture = fixture(NO_PROBE_INTERVAL, None).await;
    let dlq = fixture.queue("wake_dlq", &[]).await;
    let arn = format!("arn:aws:sqs:us-east-1:000000000000:{dlq}");
    let policy = format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":1}}"#);
    let source = fixture
        .queue(
            "wake_source",
            &[("RedrivePolicy", &policy), ("VisibilityTimeout", "0")],
        )
        .await;
    fixture.send(&source, "poison", None).await;
    assert_eq!(fixture.receive(&source, 1, Some(0)).await.messages.len(), 1);
    let waiter = fixture.try_receive(&dlq, 1, Some(20));
    fixture.until_waiting(1).await;
    let started = Instant::now();
    assert!(
        fixture
            .receive(&source, 1, Some(0))
            .await
            .messages
            .is_empty()
    );
    let response = within_budget(waiter).await.unwrap();
    assert_eq!(response.messages[0].body, "poison");
    assert!(started.elapsed() < Duration::from_secs(5), "no local wake");
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

/// Scenario: a slow probe still delivers. Paused Tokio time: each probe takes
/// 300 ms of simulated time, longer than the probe interval. The request
/// uses the result of its own probe, so it claims the due message.
#[tokio::test(start_paused = true)]
async fn given_each_probe_takes_300_ms_when_short_polled_then_the_due_message_is_received() {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("slow-probe", &[]).await;
    // Another node commits, so this process has no local signal.
    Fixture::send_through(&fixture.other_node, &queue_url, "slow", None).await;
    fixture.port.probe_delay_ms.store(300, Ordering::SeqCst);
    fixture.port.reset();
    let response = fixture
        .manager
        .receive_message(receive_request(&queue_url, 0))
        .await
        .unwrap();
    assert_eq!(response.messages[0].body, "slow");
    // One probe and one claim, as on a fast store.
    assert_eq!(fixture.port.counts(), (1, 1));
}

/// Scenario: old shared evidence causes a new probe, not an empty answer.
/// Request A probed the empty queue 500 ms ago; another node then committed
/// a message that is due 100 ms before request B starts a long poll on a
/// slow store. B probes, then claims on its own evidence.
#[tokio::test(start_paused = true)]
async fn given_old_shared_evidence_when_a_long_poll_starts_then_it_probes_again_and_returns_the_message()
 {
    let fixture = fixture(RECEIVE_PROBE_INTERVAL, None).await;
    let queue_url = fixture.queue("old-evidence", &[]).await;
    let first = fixture
        .manager
        .receive_message(receive_request(&queue_url, 0))
        .await
        .unwrap();
    assert!(first.messages.is_empty());
    tokio::time::advance(Duration::from_millis(400)).await;
    Fixture::send_through(&fixture.other_node, &queue_url, "due", None).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    fixture.port.probe_delay_ms.store(300, Ordering::SeqCst);
    fixture.port.reset();
    let started = tokio::time::Instant::now();
    let response = fixture
        .manager
        .receive_message(receive_request(&queue_url, 1))
        .await
        .unwrap();
    assert_eq!(response.messages[0].body, "due");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the wait ended first"
    );
    assert_eq!(fixture.port.counts(), (1, 1));
}

/// Longer than any virtual wait of the tests below; a logical hang trips it.
const VIRTUAL_GUARD: Duration = Duration::from_secs(600);

async fn settled<T>(task: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(VIRTUAL_GUARD, task)
        .await
        .expect("task never finished")
        .unwrap()
}

async fn until_waiting_virtual(manager: &QueueManager, waiting: usize) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if manager.waiting_receives() == waiting {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("waiting receives never reached {waiting}");
}

/// Polls `future` while yielding, so the paused clock cannot auto-advance
/// across real I/O.
async fn busy<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    loop {
        tokio::select! {
            biased;
            value = &mut future => return value,
            () = tokio::task::yield_now() => {}
        }
    }
}

/// Runs every ready task until no attempt is in flight and 100 more yields
/// found none.
async fn quiesce(port: &CountingPort) {
    loop {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        if port.in_flight.load(Ordering::SeqCst) == 0 {
            return;
        }
    }
}

/// Idle long polls cost no per-queue reads once a queue was probed.
#[tokio::test(start_paused = true)]
async fn given_2000_idle_queues_with_10000_long_polls_when_two_rounds_pass_then_each_queue_is_probed_once()
 {
    let fixture = fixture_with_local_board(Duration::from_millis(200)).await;
    let mut queues = Vec::new();
    for index in 0..2_000 {
        queues.push(fixture.queue(&format!("idle-{index}"), &[]).await);
    }
    fixture.port.reset();
    for _round in 0..2 {
        let waiters = (0..10_000)
            .map(|index| fixture.try_receive(&queues[index % queues.len()], 1, Some(20)))
            .collect::<Vec<_>>();
        until_waiting_virtual(&fixture.manager, 10_000).await;
        quiesce(&fixture.port).await;
        for waiter in waiters {
            assert!(settled(waiter).await.unwrap().messages.is_empty());
        }
    }
    let (read_only, read_write) = fixture.port.counts();
    assert_eq!(read_write, 0);
    assert_eq!(read_only, 2_000, "only the first probe of each queue reads");
}

#[tokio::test(start_paused = true)]
async fn given_waiter_on_node_b_when_node_a_sends_then_it_returns_before_the_probe_interval() {
    let fixture = fixture_with_local_board(NO_PROBE_INTERVAL).await;
    let queue_url = fixture.queue("cross-node", &[]).await;
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    until_waiting_virtual(&fixture.manager, 1).await;
    quiesce(&fixture.port).await;
    // Past the wake spacing, so the wake probe is not deferred by design.
    tokio::time::advance(RECEIVE_WAKE_SPACING + Duration::from_millis(1)).await;
    quiesce(&fixture.port).await;
    fixture.port.reset();
    let sent = tokio::time::Instant::now();
    busy(Fixture::send_through(
        &fixture.other_node,
        &queue_url,
        "from-a",
        None,
    ))
    .await;
    let response = busy(settled(waiter)).await.unwrap();
    let elapsed = sent.elapsed();
    assert_eq!(response.messages[0].body, "from-a");
    assert!(fixture.port.counts().0 <= 1, "more than the wake probe ran");
    assert!(
        elapsed < Duration::from_secs(1),
        "returned after {elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_short_polls_on_a_covered_idle_queue_when_100_run_then_one_probe() {
    let fixture = fixture_with_local_board(RECEIVE_PROBE_INTERVAL).await;
    let queue_url = fixture.queue("covered-short", &[]).await;
    fixture.port.reset();
    for _ in 0..100 {
        let response = fixture
            .manager
            .receive_message(receive_request(&queue_url, 0))
            .await
            .unwrap();
        assert!(response.messages.is_empty());
    }
    assert_eq!(fixture.port.counts(), (1, 0));
}

/// Models the FoundationDB report: the feed board does not advance on a
/// local commit; only the test reports the change.
#[tokio::test(start_paused = true)]
async fn given_controlled_feed_board_when_a_send_is_not_reported_then_the_waiter_waits_until_the_board_advances()
 {
    let board = queue_provider::QueueWakeBoard::feed(false);
    let fixture = fixture_with_board(NO_PROBE_INTERVAL, Some(board.clone())).await;
    let queue_url = fixture.queue("controlled", &[]).await;
    let slot = queue_provider::QueueWakeSlot::of_queue("controlled");
    board.cover(slot);
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    until_waiting_virtual(&fixture.manager, 1).await;
    fixture.port.reset();
    busy(Fixture::send_through(
        &fixture.other_node,
        &queue_url,
        "unreported",
        None,
    ))
    .await;
    tokio::time::advance(Duration::from_secs(5)).await;
    quiesce(&fixture.port).await;
    assert!(
        !waiter.is_finished(),
        "the waiter returned without a report"
    );
    assert_eq!(fixture.port.counts().0, 0);
    board.advance(slot);
    let response = busy(settled(waiter)).await.unwrap();
    assert_eq!(response.messages[0].body, "unreported");
    assert_eq!(fixture.port.counts().0, 1);
}

#[tokio::test(start_paused = true)]
async fn given_controlled_feed_board_when_coverage_is_lost_then_interval_probes_resume() {
    let board = queue_provider::QueueWakeBoard::feed(false);
    let fixture = fixture_with_board(Duration::from_millis(200), Some(board.clone())).await;
    let queue_url = fixture.queue("uncovered", &[]).await;
    let slot = queue_provider::QueueWakeSlot::of_queue("uncovered");
    board.cover(slot);
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    until_waiting_virtual(&fixture.manager, 1).await;
    quiesce(&fixture.port).await;
    fixture.port.reset();
    board.uncover(slot);
    for _ in 0..10 {
        tokio::time::advance(Duration::from_millis(100)).await;
        quiesce(&fixture.port).await;
    }
    let (read_only, read_write) = fixture.port.counts();
    assert!((4..=6).contains(&read_only), "{read_only} probes in 1 s");
    assert_eq!(read_write, 0);
    waiter.abort();
}

/// A hot slot costs a neighbour queue at most the spacing rule: one probe
/// per 200 ms.
#[tokio::test(start_paused = true)]
async fn given_hot_slot_when_1000_advances_in_2_seconds_then_neighbour_probes_at_most_11() {
    let board = queue_provider::QueueWakeBoard::feed(false);
    let fixture = fixture_with_board(NO_PROBE_INTERVAL, Some(board.clone())).await;
    let queue_url = fixture.queue("q2", &[]).await;
    let slot = queue_provider::QueueWakeSlot::of_queue("q2");
    board.cover(slot);
    fixture.port.reset();
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    until_waiting_virtual(&fixture.manager, 1).await;
    for _ in 0..1_000 {
        board.advance(slot);
        tokio::time::advance(Duration::from_millis(2)).await;
        quiesce(&fixture.port).await;
    }
    let (read_only, read_write) = fixture.port.counts();
    assert!(read_only <= 11, "{read_only} probes");
    assert_eq!(read_write, 0);
    waiter.abort();
}

#[tokio::test(start_paused = true)]
async fn given_waiters_when_they_end_by_deadline_drop_and_delete_then_no_interest_remains() {
    let fixture = fixture_with_local_board(NO_PROBE_INTERVAL).await;
    let board = fixture.port.queue_wake_board().unwrap();
    let ended_by_deadline = fixture.queue("by-deadline", &[]).await;
    let dropped = fixture.queue("by-drop", &[]).await;
    let deleted = fixture.queue("by-delete", &[]).await;

    let deadline_waiter = fixture.try_receive(&ended_by_deadline, 1, Some(1));
    let drop_waiter = fixture.try_receive(&dropped, 1, Some(20));
    let delete_waiter = fixture.try_receive(&deleted, 1, Some(20));
    until_waiting_virtual(&fixture.manager, 3).await;
    assert_eq!(board.interested_slots().len(), 3);
    quiesce(&fixture.port).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    quiesce(&fixture.port).await;
    assert!(
        busy(settled(deadline_waiter))
            .await
            .unwrap()
            .messages
            .is_empty()
    );

    drop_waiter.abort();
    assert!(busy(drop_waiter).await.unwrap_err().is_cancelled());

    fixture.port.reset();
    busy(fixture.other_node.delete_queue(DeleteQueueRequest {
        queue_url: deleted.clone(),
    }))
    .await
    .unwrap();
    assert!(matches!(
        busy(settled(delete_waiter)).await,
        Err(QueueError::ResourceNotFound {
            resource_type: "queue",
            ..
        })
    ));
    assert!(
        fixture.port.counts().0 <= 1,
        "more than one probe ended the wait"
    );
    assert!(board.interested_slots().is_empty());
    assert_eq!(fixture.manager.waiting_receives(), 0);
}

#[tokio::test(start_paused = true)]
async fn given_short_poll_when_it_returns_then_it_never_registered() {
    let fixture = fixture_with_local_board(RECEIVE_PROBE_INTERVAL).await;
    let board = fixture.port.queue_wake_board().unwrap();
    let queue_url = fixture.queue("never-registers", &[]).await;
    let response = fixture
        .manager
        .receive_message(receive_request(&queue_url, 0))
        .await
        .unwrap();
    assert!(response.messages.is_empty());
    assert!(board.interested_slots().is_empty());
    // A registration would have stored an `interest_added` permit.
    assert!(
        tokio::time::timeout(Duration::ZERO, board.interest_added())
            .await
            .is_err()
    );
}

/// W1: a send whose commit outcome is unknown (the row is kept) still wakes
/// the waiters of its queue.
#[tokio::test(start_paused = true)]
async fn given_send_with_unknown_commit_when_a_waiter_waits_then_it_receives_the_message() {
    let fixture = fixture_with_local_board(NO_PROBE_INTERVAL).await;
    let queue_url = fixture.queue("commit-unknown", &[]).await;
    let waiter = fixture.try_receive(&queue_url, 1, Some(20));
    until_waiting_virtual(&fixture.manager, 1).await;
    quiesce(&fixture.port).await;
    tokio::time::advance(RECEIVE_WAKE_SPACING + Duration::from_millis(1)).await;
    quiesce(&fixture.port).await;
    fixture.port.reset();
    fixture
        .port
        .commit_unknown_writes
        .store(1, Ordering::SeqCst);
    let sent = busy(fixture.other_node.send_message(SendMessageRequest {
        queue_url: queue_url.clone(),
        message_body: "unknown".to_owned(),
        delay_seconds: None,
        message_attributes: None,
        ..Default::default()
    }))
    .await;
    assert!(sent.is_err(), "the send reported commit-unknown");
    let response = busy(settled(waiter)).await.unwrap();
    assert_eq!(response.messages[0].body, "unknown");
    assert!(fixture.port.counts().0 <= 1);
}
