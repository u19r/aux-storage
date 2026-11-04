//! Real process death after SQLite commits atomic work, pending input or
//! retirement.

use std::{
    future::pending,
    num::NonZeroU16,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy, HandlerFuture,
    JobCatalog, JobHandler, JobId, JobKind, JobOutcome, JobSpec, LifecycleObservation, Namespace,
    OutcomeCode, OwnerId, QueueName, RetryPreset, RetrySelection, Timestamp,
    runtime::{JobSystem, JobSystemBuilder},
    store::{
        CommitOutcome, JobStore, Mutation, RangePage, RangeRequest, StoreCapabilities, StoreKey,
        StoreTransaction,
    },
    test_support::{
        TestProfile,
        codec::{KeyCodec, KeyTag},
    },
};

use super::test_support::TestStore;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Boundary {
    Admission,
    Completion,
    Retirement,
    AtomicAdmission,
    AtomicCompletion,
    AppliedInput,
}

#[derive(Clone)]
struct InterruptedStore<S> {
    inner: S,
    boundary: Boundary,
    ready: PathBuf,
    armed: Arc<AtomicBool>,
    force_pending: Arc<AtomicBool>,
}

impl<S: JobStore> JobStore for InterruptedStore<S> {
    type Error = S::Error;
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, Self::Error> {
        self.inner.get(key).await
    }
    async fn get_many(&self, keys: &[StoreKey]) -> Result<Vec<Option<Bytes>>, Self::Error> {
        self.inner.get_many(keys).await
    }
    async fn read_range(&self, request: RangeRequest) -> Result<RangePage, Self::Error> {
        self.inner.read_range(request).await
    }
    async fn transact(&self, input: &StoreTransaction) -> Result<CommitOutcome, Self::Error> {
        if self.armed.load(Ordering::SeqCst) && self.force_pending.swap(false, Ordering::SeqCst) {
            // Force an unapplied ambiguous atomic attempt. The next CAS must
            // durably accept input, preserving the pending-work death tests.
            return Ok(CommitOutcome::Unknown);
        }
        let result = self.inner.transact(input).await?;
        let interrupts = input.mutations().iter().any(|mutation| {
            let tag = KeyCodec::decode_borrowed(&namespace(), mutation.key()).map(|key| key.tag);
            matches!(
                (self.boundary, mutation, tag),
                (
                    Boundary::Admission | Boundary::Completion,
                    Mutation::Put(_, _),
                    Ok(KeyTag::CommitInput)
                ) | (
                    Boundary::Retirement,
                    Mutation::Delete(_),
                    Ok(KeyTag::CommitInput)
                ) | (
                    Boundary::AtomicAdmission | Boundary::AtomicCompletion,
                    Mutation::Put(_, _),
                    Ok(KeyTag::Commit)
                ) | (Boundary::AppliedInput, Mutation::Delete(_), Ok(KeyTag::Job))
            )
        });
        if result == CommitOutcome::Committed
            && interrupts
            && self.armed.swap(false, Ordering::SeqCst)
        {
            // The native commit has returned; the producer never receives it.
            std::fs::write(&self.ready, b"committed").unwrap();
            return pending().await;
        }
        Ok(result)
    }
}

struct CompletionHandler;
impl JobHandler for CompletionHandler {
    fn execute<'a>(&'a self, _: ExecutionContext<'a>, _: &'a [u8]) -> HandlerFuture<'a> {
        panic!("recovery must apply produced completion input without invoking a handler")
    }
}

fn namespace() -> Namespace {
    Namespace::try_new("sqlite-process-recovery").unwrap()
}

fn system<S: JobStore>(store: S) -> JobSystem<S> {
    let mut catalog = JobCatalog::new();
    catalog
        .register(
            JobSpec::unit(
                JobKind::try_new("process-recovery").unwrap(),
                NonZeroU16::MIN,
                QueueName::try_new("default").unwrap(),
                CompletionHandler,
            )
            .policy(
                EffectMode::Fenced,
                FairnessPolicy::low_volume(),
                RetrySelection::Preset(RetryPreset::Maintenance),
                Duration::from_secs(30),
            ),
        )
        .unwrap();
    JobSystemBuilder::new(store)
        .catalog(catalog)
        .namespace(namespace())
        .owner(OwnerId::generate().unwrap())
        .scheduler(Arc::new(TestProfile::new()))
        .build()
        .unwrap()
}

async fn enqueue<S: JobStore>(system: &JobSystem<S>, id: u8) {
    system
        .ensure_seed(
            scheduled_bg_jobs::JobSeed::new(
                JobKind::try_new("process-recovery").unwrap(),
                NonZeroU16::MIN,
                Bytes::new(),
                EnqueueOptions::new(FairnessClass::low_volume()).job_id(JobId::from([id; 16])),
                Timestamp::ZERO,
            )
            .unwrap(),
            Timestamp::ZERO,
        )
        .await
        .unwrap();
}

struct Producer(Child);
impl Drop for Producer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn kill_after_native_commit(path: &Path, ready: &Path, mode: &str) {
    let mut producer = Producer(Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "scheduled_job_process_tests::given_a_parent_process_when_the_child_runs_then_it_stops_after_native_commit", "--nocapture"])
        .env("JOB_PROCESS_SQLITE_PATH", path)
        .env("JOB_PROCESS_READY_PATH", ready)
        .env("JOB_PROCESS_BOUNDARY", mode)
        .stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        assert!(
            producer.0.try_wait().unwrap().is_none(),
            "producer exited before native commit"
        );
        assert!(
            Instant::now() < deadline,
            "producer never reached native commit"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    producer.0.kill().unwrap();
    assert!(!producer.0.wait().unwrap().success());
}

#[tokio::test]
async fn given_a_parent_process_when_the_child_runs_then_it_stops_after_native_commit() {
    let Ok(path) = std::env::var("JOB_PROCESS_SQLITE_PATH") else {
        return;
    };
    let ready = std::env::var_os("JOB_PROCESS_READY_PATH").unwrap().into();
    let boundary = match std::env::var("JOB_PROCESS_BOUNDARY").unwrap().as_str() {
        "admission" => Boundary::Admission,
        "completion" => Boundary::Completion,
        "retirement" => Boundary::Retirement,
        "atomic-admission" => Boundary::AtomicAdmission,
        "atomic-completion" => Boundary::AtomicCompletion,
        "applied-input" => Boundary::AppliedInput,
        _ => panic!("unknown process boundary"),
    };
    let fixture = TestStore::open_sqlite_file(Path::new(&path)).await;
    let store = InterruptedStore {
        inner: fixture.scheduled_job_store,
        boundary,
        ready,
        armed: Arc::new(AtomicBool::new(false)),
        force_pending: Arc::new(AtomicBool::new(matches!(
            boundary,
            Boundary::Admission
                | Boundary::Completion
                | Boundary::Retirement
                | Boundary::AppliedInput
        ))),
    };
    let engine = system(store.clone());
    if matches!(boundary, Boundary::Admission | Boundary::AtomicAdmission) {
        store.armed.store(true, Ordering::SeqCst);
        enqueue(&engine, 1).await;
    } else {
        let id = if matches!(
            boundary,
            Boundary::Completion | Boundary::AtomicCompletion | Boundary::AppliedInput
        ) {
            1
        } else {
            2
        };
        let claim = engine
            .claim(JobId::from([id; 16]), Timestamp::ZERO)
            .await
            .unwrap();
        store.armed.store(true, Ordering::SeqCst);
        engine
            .complete(
                claim,
                JobOutcome::Complete(OutcomeCode::try_new("completed").unwrap()),
                Timestamp::from_millis(1),
            )
            .await
            .unwrap();
    }
    panic!("the parent must kill the producer before acknowledgement");
}

#[tokio::test]
async fn given_os_killed_producers_when_sqlite_reopens_then_accepted_work_and_retirement_recover_without_them()
 {
    let directory = tempfile::tempdir().unwrap();
    for modes in [
        &["admission", "completion", "retirement"][..],
        &["atomic-admission", "atomic-completion"][..],
        &["applied-input"][..],
    ] {
        let path = directory.path().join(format!("{}.sqlite", modes[0]));
        {
            let fixture = TestStore::open_sqlite_file(&path).await;
            fixture.scheduled_job_store.bootstrap().await.unwrap();
            if modes[0] == "applied-input" {
                enqueue(&system(fixture.scheduled_job_store), 1).await;
            }
        }
        for &mode in modes {
            let ready = directory.path().join(mode);
            kill_after_native_commit(&path, &ready, mode).await;
            let fixture = TestStore::open_sqlite_file(&path).await;
            let engine = system(fixture.scheduled_job_store);
            engine.recover_commits().await.unwrap();
            if mode.contains("admission") {
                assert!(matches!(
                    engine
                        .observe_lifecycle(JobId::from([1; 16]))
                        .await
                        .unwrap(),
                    LifecycleObservation::Current { .. }
                ));
            } else {
                let id = if mode.contains("completion") || mode == "applied-input" {
                    1
                } else {
                    2
                };
                assert!(matches!(
                    engine
                        .observe_lifecycle(JobId::from([id; 16]))
                        .await
                        .unwrap(),
                    LifecycleObservation::Missing
                ));
            }
            if mode == "completion" {
                enqueue(&engine, 2).await;
            }
        }
    }
}
