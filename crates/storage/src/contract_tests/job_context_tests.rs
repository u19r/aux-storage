use std::{
    num::{NonZeroU16, NonZeroU64},
    sync::{Arc, Mutex},
    time::Duration,
};

use bg_job_store::{ScheduledJobStore, ScheduledJobStoreSource};
use bg_jobs_tokio::{
    JobRuntime,
    test_support::{TestClock, runtime_builder},
};
use scheduled_bg_jobs::{
    DriveBudget, DriveBudgetInput, EffectMode, EnqueueOptions, ExecutionContext, ExecutionId,
    FairnessClass, FairnessPolicy, HandlerFuture, JobCatalog, JobHandler, JobId, JobKind,
    JobOutcome, JobSpec, Namespace, OutcomeCode, OwnerId, QueueName, QueueSelection, RetryPreset,
    RetrySelection, Timestamp, runtime::MonotonicDeadline,
};

#[derive(Clone)]
struct ScopeObserver {
    invocations: Arc<Mutex<Vec<ClaimObservation>>>,
}

struct ClaimObservation {
    namespace: Namespace,
    job: JobId,
    execution: ExecutionId,
    fence: NonZeroU64,
}

impl JobHandler for ScopeObserver {
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        _payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            self.invocations
                .lock()
                .expect("test observation")
                .push(ClaimObservation {
                    namespace: context.namespace().clone(),
                    job: context.job_id(),
                    execution: context.execution_id(),
                    fence: context.fence(),
                });
            Ok(JobOutcome::Complete(
                OutcomeCode::try_new("complete").expect("test outcome"),
            ))
        })
    }
}

fn build_runtime(
    store: ScheduledJobStore,
    namespace: Namespace,
    observer: ScopeObserver,
) -> JobRuntime<ScheduledJobStore> {
    let now = Timestamp::from_millis(1_800_000_000_000);
    let mut catalog = JobCatalog::new();
    catalog
        .register_seeded(
            JobSpec::unit(
                JobKind::try_new("scope-observation").expect("test kind"),
                NonZeroU16::MIN,
                QueueName::try_new("scope-observation").expect("test queue"),
                observer,
            )
            .policy(
                EffectMode::Fenced,
                FairnessPolicy::low_volume(),
                RetrySelection::Preset(RetryPreset::Maintenance),
                Duration::from_secs(1),
            ),
            (),
            EnqueueOptions::new(FairnessClass::low_volume())
                .job_id(JobId::from([4; 16]))
                .scheduled_at(now),
            now,
        )
        .expect("test catalog");
    let budget = DriveBudget::try_from(DriveBudgetInput {
        queues: QueueSelection::all(),
        max_claims: 1,
        max_concurrency: 1,
        max_store_operations: 128,
        stop_claiming_after: Duration::from_secs(1),
        finish_after: Duration::from_secs(2),
    })
    .expect("test budget");
    runtime_builder(store, catalog, budget)
        .namespace(namespace)
        .owner(OwnerId::from([7; 16]))
        .clock(Arc::new(TestClock::new(now, MonotonicDeadline::ZERO)))
        .build()
        .expect("native test runtime")
}

async fn verify_claim_scope(store: ScheduledJobStore) {
    let namespaces = [
        Namespace::try_new("first-scheduler").expect("first namespace"),
        Namespace::try_new("second-scheduler").expect("second namespace"),
    ];
    let observer = ScopeObserver {
        invocations: Arc::new(Mutex::new(Vec::new())),
    };
    for namespace in &namespaces {
        let runtime = build_runtime(store.clone(), namespace.clone(), observer.clone());
        runtime.ensure_seeds().await.expect("native enqueue");
        runtime
            .drive_once()
            .await
            .expect("native claim, handler and completion");
        runtime
            .drive_once()
            .await
            .expect("completed work is not dispatched again");
    }
    let invocations = observer.invocations.lock().expect("test observations");
    assert_eq!(invocations.len(), 2);
    for (invocation, namespace) in invocations.iter().zip(&namespaces) {
        assert_eq!(&invocation.namespace, namespace);
        assert_eq!(invocation.job, JobId::from([4; 16]));
        assert_eq!(invocation.fence, NonZeroU64::MIN);
    }
    assert_ne!(invocations[0].execution, invocations[1].execution);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_memory_sqlite_jobs_in_distinct_namespaces_when_driven_then_actual_claim_scope_is_preserved()
 {
    let backend = sql_backend::sqlite::SqliteBackend::initialize(":memory:")
        .await
        .expect("memory SQLite");
    let store = sql_backend::SqlStore::initialize_schema(backend)
        .await
        .expect("ordinary SQLite store");
    verify_claim_scope(
        store
            .scheduled_job_store(Default::default())
            .expect("real job adapter"),
    )
    .await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocksdb_jobs_in_distinct_namespaces_when_driven_then_actual_claim_scope_is_preserved()
 {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("test clock")
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("job-context-{}-{nonce}", std::process::id()));
    let backend =
        kv_backend::rocksdb::RocksDbBackend::initialize(directory.clone()).expect("RocksDB");
    let store = kv_backend::KvStore::initialize_format(backend)
        .await
        .expect("ordinary KV store");
    verify_claim_scope(
        store
            .scheduled_job_store(Default::default())
            .expect("real job adapter"),
    )
    .await;
    drop(store);
    std::fs::remove_dir_all(directory).expect("remove owned RocksDB fixture");
}
