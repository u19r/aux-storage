//! The commit-submission mark of a native write attempt, seen through
//! `put_item` on every provider: before `submit_commit` an expired or lost
//! caller is a known abort; after it the outcome is unknown or committed,
//! never a clean abort and never a replay.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::json;
use storage_types::{
    AttemptDeadlines, AttemptMode, AttemptPhase, DEFAULT_COMMIT_GRACE_MS, DeadlineClass,
    GetItemRequest, PutItemRequest, StorageResult, TableName,
};

use super::test_support::TestStore;

/// A wait for a state gives up after this long (seconds).
const WAIT_SECONDS: u64 = 10;

/// The write budget of a test whose deadline passes in real time.
pub(super) const WRITE_BUDGET: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Plan {
    Idle,
    /// Hold the next commit before submission until released.
    HoldBeforeSubmit,
    /// Hold the next commit after its durable acknowledgement until released.
    HoldAfterAcknowledgement,
    /// Withhold the answer of the next submitted commit (an unknown outcome).
    FailAfterAcknowledgement,
}

/// Plans act on write attempts only: read attempts also pass the commit
/// boundaries, and the boundary's `AttemptMode` tells them apart.
pub(super) struct CommitMarkProbe {
    plan: Mutex<Plan>,
    reached: tokio::sync::Notify,
    pub(super) released: tokio::sync::Notify,
    /// Write attempts that reached the submission mark.
    marks: AtomicUsize,
    aborts: AtomicUsize,
}

impl Default for CommitMarkProbe {
    fn default() -> Self {
        Self {
            plan: Mutex::new(Plan::Idle),
            reached: tokio::sync::Notify::new(),
            released: tokio::sync::Notify::new(),
            marks: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
        }
    }
}

impl CommitMarkProbe {
    /// Arms `plan` for the next write commit and clears the attempt counters.
    pub(super) fn arm(&self, plan: Plan) {
        self.marks.store(0, Ordering::SeqCst);
        self.aborts.store(0, Ordering::SeqCst);
        *self.plan.lock().unwrap() = plan;
    }

    /// Write attempts that reached the submission mark since `arm`; a replay
    /// reaches it again.
    pub(super) fn marks(&self) -> usize {
        self.marks.load(Ordering::SeqCst)
    }

    pub(super) fn aborts(&self) -> usize {
        self.aborts.load(Ordering::SeqCst)
    }

    pub(super) async fn wait_reached(&self) {
        tokio::time::timeout(Duration::from_secs(WAIT_SECONDS), self.reached.notified())
            .await
            .expect("the commit never reached the armed boundary");
    }

    fn at_boundary(
        &self,
        before_submit: bool,
        mode: AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        if mode == AttemptMode::ReadOnly {
            return Box::pin(async { Ok(()) });
        }
        if before_submit {
            self.marks.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move {
            let plan = {
                let mut plan = self.plan.lock().unwrap();
                let matches = match *plan {
                    Plan::Idle => false,
                    Plan::HoldBeforeSubmit => before_submit,
                    Plan::HoldAfterAcknowledgement | Plan::FailAfterAcknowledgement => {
                        !before_submit
                    }
                };
                if !matches {
                    return Ok(());
                }
                std::mem::replace(&mut *plan, Plan::Idle)
            };
            if plan == Plan::FailAfterAcknowledgement {
                return Err(storage_types::StorageError::internal(
                    "test: answer withheld after commit",
                ));
            }
            self.reached.notify_one();
            self.released.notified().await;
            Ok(())
        })
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
impl sql_backend::SqlFaultProbe for CommitMarkProbe {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        mode: AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        self.at_boundary(
            boundary == sql_backend::SqlCommitBoundary::BeforeCommit,
            mode,
        )
    }

    fn observe(&self, event: sql_backend::SqlNativeEvent) {
        if event == sql_backend::SqlNativeEvent::Rollback {
            self.aborts.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for CommitMarkProbe {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        mode: AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        self.at_boundary(boundary == kv_backend::KvCommitBoundary::BeforeCommit, mode)
    }

    fn observe(&self, event: kv_backend::KvNativeEvent) {
        if event == kv_backend::KvNativeEvent::Abort {
            self.aborts.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// A store whose commits the returned probe can hold or fail.
pub(super) async fn probed_store() -> (Arc<CommitMarkProbe>, TestStore) {
    probed_store_with_deadlines(None).await
}

/// As `probed_store`; a write attempt's deadline passes `write_budget` after
/// it starts, in real time (the attempt deadline is a Tokio instant, which
/// the test clock does not move). Other classes keep long budgets.
pub(super) async fn probed_store_with_write_budget(
    write_budget: Duration,
) -> (Arc<CommitMarkProbe>, TestStore) {
    let mut budgets = [Duration::from_secs(120); DeadlineClass::COUNT];
    budgets[DeadlineClass::Write.index()] = write_budget;
    let deadlines =
        AttemptDeadlines::try_new(budgets, Duration::from_millis(DEFAULT_COMMIT_GRACE_MS)).unwrap();
    probed_store_with_deadlines(Some(deadlines)).await
}

async fn probed_store_with_deadlines(
    deadlines: Option<AttemptDeadlines>,
) -> (Arc<CommitMarkProbe>, TestStore) {
    let probe = Arc::new(CommitMarkProbe::default());
    let store = TestStore::new_with_native_probes_and_deadlines(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
        deadlines,
    )
    .await;
    (probe, store)
}

async fn fixture() -> (Arc<CommitMarkProbe>, TestStore, TableName) {
    let (probe, store) = probed_store().await;
    let table = store.create_standard_table().await;
    (probe, store, table)
}

fn put_request(table: &TableName) -> PutItemRequest {
    PutItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Item": {"pk": {"S": "marked"}, "v": {"S": "value"}}
    }))
    .unwrap()
}

async fn item_exists(store: &TestStore, table: &TableName) -> bool {
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "marked"}}
    }))
    .unwrap();
    let response = serde_json::to_value(store.operations.get_item(request).await.unwrap()).unwrap();
    response.get("Item").is_some()
}

/// Waits for the detached attempt of a dropped caller to leave its item.
async fn wait_for_item(store: &TestStore, table: &TableName) {
    let give_up = tokio::time::Instant::now() + Duration::from_secs(WAIT_SECONDS);
    while !item_exists(store, table).await {
        assert!(
            tokio::time::Instant::now() < give_up,
            "the detached attempt never committed its item"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_deadline_passes_before_commit_submission_when_released_then_known_deadline_and_nothing_written()
 {
    let (probe, store) = probed_store_with_write_budget(WRITE_BUDGET).await;
    let table = store.create_standard_table().await;
    probe.arm(Plan::HoldBeforeSubmit);
    let put = store.operations.put_item(put_request(&table));
    let expire = async {
        probe.wait_reached().await;
        tokio::time::sleep(WRITE_BUDGET + Duration::from_millis(500)).await;
        probe.released.notify_one();
    };
    let (result, ()) = tokio::join!(put, expire);
    let error = result.expect_err("an expired attempt must not commit");
    // Postgres races its attempt deadline timer against the whole callback,
    // final COMMIT wave included: the timer stops the held attempt before
    // `submit_commit`, and an unsubmitted write reports the statement phase
    // (`PostgresAttempt::expired_error`). The other providers reach
    // `submit_commit`, which refuses it as a settle deadline.
    let phase = if TestStore::selected_backend() == "postgres" {
        AttemptPhase::Statement
    } else {
        AttemptPhase::Settle
    };
    assert_eq!(
        error.deadline_phase(),
        Some(phase),
        "refused before the submission mark as a known deadline"
    );
    assert!(!error.is_commit_unknown(), "nothing was submitted");
    assert_eq!(probe.marks(), 1, "a known deadline is not replayed");
    assert!(!item_exists(&store, &table).await, "no write is visible");
    store.finish().await;
}

#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_answer_withheld_after_commit_submission_when_put_then_commit_unknown_without_abort_or_replay()
 {
    let (probe, store, table) = fixture().await;
    probe.arm(Plan::FailAfterAcknowledgement);
    let error = store
        .operations
        .put_item(put_request(&table))
        .await
        .expect_err("the withheld answer must surface");
    assert!(error.is_commit_unknown(), "a submitted commit is unknown");
    assert_eq!(error.deadline_phase(), None);
    assert_eq!(probe.marks(), 1, "an unknown outcome is never replayed");
    assert_eq!(probe.aborts(), 0, "a submitted commit is never rolled back");
    assert!(
        item_exists(&store, &table).await,
        "the submitted commit applied"
    );
    store.finish().await;
}

#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_caller_dropped_after_commit_submission_when_released_then_commit_stands_without_abort_or_replay()
 {
    let (probe, store, table) = fixture().await;
    probe.arm(Plan::HoldAfterAcknowledgement);
    let operations = Arc::clone(&store.operations);
    let request = put_request(&table);
    let caller = tokio::spawn(async move { operations.put_item(request).await });
    probe.wait_reached().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    probe.released.notify_one();
    wait_for_item(&store, &table).await;
    assert_eq!(probe.marks(), 1, "a dropped caller does not replay");
    assert_eq!(probe.aborts(), 0, "a submitted commit is never rolled back");
    store.finish().await;
}

#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_caller_dropped_before_commit_submission_when_released_then_detached_attempt_settles_once()
 {
    // The supervisor owns the attempt: dropping the waiter does not abandon
    // it, so the held commit is still submitted once it is released.
    let (probe, store, table) = fixture().await;
    probe.arm(Plan::HoldBeforeSubmit);
    let operations = Arc::clone(&store.operations);
    let request = put_request(&table);
    let caller = tokio::spawn(async move { operations.put_item(request).await });
    probe.wait_reached().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    probe.released.notify_one();
    wait_for_item(&store, &table).await;
    assert_eq!(probe.marks(), 1, "a dropped caller does not replay");
    assert_eq!(probe.aborts(), 0);
    store.finish().await;
}
