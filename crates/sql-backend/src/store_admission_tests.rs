//! The admission lane of a store value applies to every lender, once per native
//! attempt, on SQLite.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use storage_common::native_admission::{
    AdmittedStore, AttemptAdmission, NativeAdmission, NativeAdmissionBinding,
    NativeAdmissionFuture, NativeAdmissionOutcome, NativeAdmissionPermit, NativeWork,
};
use storage_provider::{DynamoOperations as _, DynamoPort, RequestTableBindings};
use storage_types::{
    AttemptMode::{ReadOnly, ReadWrite, ReadWriteItems},
    AttemptPlan, Clock as _, DeadlineClass, MaxIndexers, StorageEnum, StorageResult, TableName,
    UpdateTableRequest, WriteRetryPolicy,
};
use stream_provider::StreamPort;

use crate::{SqlStore, sqlite::SqliteBackend};

struct RecordingPermit;

impl NativeAdmissionPermit for RecordingPermit {
    fn complete(self: Box<Self>, _outcome: NativeAdmissionOutcome<'_>, _elapsed: Duration) {}
}

/// Records the work class of each acquisition.
#[derive(Default)]
struct RecordingAdmission {
    works: Mutex<Vec<&'static str>>,
}

impl RecordingAdmission {
    fn recorded(&self) -> Vec<&'static str> {
        self.works.lock().expect("works lock").clone()
    }
}

impl NativeAdmission for RecordingAdmission {
    fn connection_id(&self) -> &str {
        "test"
    }

    fn require_reserved_attempts(&self, _required: std::num::NonZeroUsize) -> StorageResult<()> {
        Ok(())
    }

    fn acquire(&self, work: NativeWork) -> NativeAdmissionFuture<'_> {
        let name = match work {
            NativeWork::Recovery => "recovery",
            NativeWork::PointRead => "point_read",
            NativeWork::RangeRead => "range_read",
            NativeWork::Write => "write",
            NativeWork::Background => "background",
        };
        self.works.lock().expect("works lock").push(name);
        let permit: Box<dyn NativeAdmissionPermit> = Box::new(RecordingPermit);
        Box::pin(async move { Ok(permit) })
    }

    fn drain_native(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

fn binding(admission: &Arc<RecordingAdmission>) -> Arc<NativeAdmissionBinding> {
    Arc::new(NativeAdmissionBinding::bound(admission.clone()))
}

async fn store(root: &tempfile::TempDir) -> SqlStore<SqliteBackend> {
    let path = root.path().join("store.sqlite");
    let backend = SqliteBackend::initialize(path.to_str().unwrap(), Default::default())
        .await
        .unwrap();
    SqlStore::initialize_schema(backend, storage_types::InitialStreamLineages::ONE, true)
        .await
        .unwrap()
}

/// Seven scoped attempts of the two ports, each with a step that does no I/O.
async fn run_every_lender(store: &SqlStore<SqliteBackend>) {
    let budgets = storage_types::AttemptDeadlineSource::attempt_deadlines(store);
    DynamoPort::with_attempt(
        store,
        AttemptPlan::new(ReadOnly, DeadlineClass::PointRead).start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
    DynamoPort::with_attempt(
        store,
        AttemptPlan::new(ReadOnly, DeadlineClass::PointRead).start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
    DynamoPort::with_attempt(
        store,
        AttemptPlan::new(ReadWriteItems, DeadlineClass::Write)
            .scoped(storage_types::AttemptScope::ItemWrite)
            .start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
    DynamoPort::with_attempt(
        store,
        AttemptPlan::new(ReadWrite, DeadlineClass::SchemaChange)
            .scoped(storage_types::AttemptScope::Schema)
            .start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
    StreamPort::with_attempt(
        store,
        AttemptPlan::new(ReadOnly, DeadlineClass::PointRead).start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
    StreamPort::with_committed_stream_attempt(
        store,
        AttemptPlan::new(ReadWrite, DeadlineClass::Write).start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
    StreamPort::with_attempt(
        store,
        AttemptPlan::new(ReadWrite, DeadlineClass::SchemaChange)
            .scoped(storage_types::AttemptScope::Topology)
            .start(budgets),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn given_background_store_when_each_lender_runs_then_each_attempt_takes_one_background_permit_on_sqlite()
 {
    let root = tempfile::tempdir().unwrap();
    let admission = Arc::new(RecordingAdmission::default());
    let store = store(&root)
        .await
        .admitted(AttemptAdmission::Background(binding(&admission)));

    run_every_lender(&store).await;

    assert_eq!(admission.recorded(), ["background"; 7]);
}

#[tokio::test]
async fn given_caller_store_when_lenders_run_then_no_permit_is_taken_on_sqlite() {
    let root = tempfile::tempdir().unwrap();
    let unbound = Arc::new(RecordingAdmission::default());
    let store = store(&root).await;

    run_every_lender(&store).await;

    assert!(unbound.recorded().is_empty());
}

#[tokio::test]
async fn given_background_store_when_recovery_work_is_named_then_the_recovery_reserve_is_taken_on_sqlite()
 {
    let root = tempfile::tempdir().unwrap();
    let admission = Arc::new(RecordingAdmission::default());
    let store = store(&root)
        .await
        .admitted(AttemptAdmission::Background(binding(&admission)));

    DynamoPort::with_attempt(
        &store,
        AttemptPlan::new(ReadWrite, DeadlineClass::Write)
            .work(NativeWork::Recovery)
            .start(storage_types::AttemptDeadlineSource::attempt_deadlines(
                &store,
            )),
        |_| Box::pin(async { Ok(()) }),
    )
    .await
    .unwrap();

    assert_eq!(admission.recorded(), ["recovery"]);
}

#[tokio::test]
async fn given_background_store_when_a_maintenance_page_retries_once_then_two_permits_are_taken_on_sqlite()
 {
    let root = tempfile::tempdir().unwrap();
    let admission = Arc::new(RecordingAdmission::default());
    let store = store(&root)
        .await
        .admitted(AttemptAdmission::Background(binding(&admission)));

    DynamoPort::run(
        &store,
        AttemptPlan::new(ReadWrite, DeadlineClass::BackgroundPage)
            .retrying(WriteRetryPolicy::new(3, Duration::from_millis(1))),
        Arc::new(AtomicU32::new(0)),
        |_, calls, _| {
            Box::pin(async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(
                        StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                            storage_types::ConflictCause::Logical("test"),
                        ))
                        .into(),
                    )
                } else {
                    Ok(())
                }
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(admission.recorded(), ["background", "background"]);
}

#[tokio::test]
async fn given_background_store_when_update_table_once_runs_then_one_background_permit_is_taken_on_sqlite()
 {
    let root = tempfile::tempdir().unwrap();
    let admission = Arc::new(RecordingAdmission::default());
    let caller = store(&root).await;
    let create: storage_types::CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "lane_target", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    }))
    .unwrap();
    caller.create_table(create).await.unwrap();
    let background = caller.admitted(AttemptAdmission::Background(binding(&admission)));

    let _ = DynamoPort::update_table_once(
        &background,
        AttemptPlan::new(ReadWrite, DeadlineClass::SchemaChange).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(&background),
        ),
        UpdateTableRequest {
            max_indexers: Some(MaxIndexers::try_new(6).expect("six indexers are supported")),
            ..UpdateTableRequest::new(TableName::new("lane_target"))
        },
        background.now(),
        RequestTableBindings::default(),
    )
    .await;

    assert_eq!(admission.recorded(), ["background"]);
}

/// Waits `wait` before every grant.
struct DelayedAdmission {
    wait: Duration,
}

impl NativeAdmission for DelayedAdmission {
    fn connection_id(&self) -> &str {
        "test"
    }

    fn require_reserved_attempts(&self, _required: std::num::NonZeroUsize) -> StorageResult<()> {
        Ok(())
    }

    fn acquire(&self, _work: NativeWork) -> NativeAdmissionFuture<'_> {
        let wait = self.wait;
        let permit: Box<dyn NativeAdmissionPermit> = Box::new(RecordingPermit);
        Box::pin(async move {
            tokio::time::sleep(wait).await;
            Ok(permit)
        })
    }

    fn drain_native(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// A foreground store whose point-read budget is 200 ms and whose admission
/// waits `wait` before each grant.
async fn delayed_foreground_store(
    root: &tempfile::TempDir,
    wait: Duration,
) -> SqlStore<SqliteBackend> {
    let mut budgets = storage_types::DEFAULT_ATTEMPT_BUDGETS_MS.map(Duration::from_millis);
    budgets[DeadlineClass::PointRead.index()] = Duration::from_millis(200);
    let deadlines = storage_types::AttemptDeadlines::try_new(budgets, Duration::from_secs(2))
        .expect("positive budgets");
    let path = root.path().join("delayed.sqlite");
    let backend = SqliteBackend::initialize(path.to_str().unwrap(), deadlines)
        .await
        .unwrap();
    SqlStore::initialize_schema(backend, storage_types::InitialStreamLineages::ONE, true)
        .await
        .unwrap()
        .admitted(AttemptAdmission::Foreground(Arc::new(
            NativeAdmissionBinding::bound(Arc::new(DelayedAdmission { wait })),
        )))
}

#[tokio::test]
async fn given_foreground_wait_longer_than_the_budget_when_a_one_try_stream_read_runs_then_acquire_deadline()
 {
    use stream_provider::StreamOperations as _;
    let root = tempfile::tempdir().unwrap();
    let store = delayed_foreground_store(&root, Duration::from_millis(400)).await;

    let error = store
        .get_stream(storage_types::UserStreamName::new("absent"))
        .await
        .expect_err("the admission wait spent the operation budget");

    assert_eq!(
        error.into_storage_error().deadline_phase(),
        Some(storage_types::AttemptPhase::Acquire)
    );
}

#[tokio::test]
async fn given_foreground_wait_inside_the_budget_when_a_one_try_stream_read_runs_then_it_answers() {
    use stream_provider::StreamOperations as _;
    let root = tempfile::tempdir().unwrap();
    let store = delayed_foreground_store(&root, Duration::ZERO).await;

    let stream = store
        .get_stream(storage_types::UserStreamName::new("absent"))
        .await
        .expect("an unwaited read answers");

    assert!(stream.is_none());
}
