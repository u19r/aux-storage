// Effect and completion races. The probes hold one commit at its pre-commit
// boundary so the other operation runs inside the window.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{FutureExt as _, future::BoxFuture};
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, FairnessClass, FairnessPolicy, JobCatalog, JobId, JobKind,
    JobOutcome, JobSpec, Namespace, OutcomeCode, OwnerId, QueueName, RetryPreset, RetrySelection,
    Timestamp,
    runtime::JobSystemBuilder,
    store::{EffectReceiptGroup, RangeDirection, RangeRequest, RangeRequestInput},
};
use serde_json::json;
use storage_types::StorageResult;
use tokio::sync::Notify;

use crate::test_support::TestStore;

struct NeverRuns;

impl scheduled_bg_jobs::JobHandler for NeverRuns {
    fn execute<'a>(
        &'a self,
        _: scheduled_bg_jobs::ExecutionContext<'a>,
        _: &'a [u8],
    ) -> scheduled_bg_jobs::HandlerFuture<'a> {
        panic!("the race tests complete the claim directly")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Trigger {
    /// A KV range clear is staged (the completion's receipt clear).
    #[cfg(feature = "foundationdb")]
    KvClearRange,
    /// A KV write to the job data keyspace inside the group prefix.
    #[cfg(feature = "foundationdb")]
    KvGroupWrite,
    /// No hold: records every access after `arm`.
    #[cfg(feature = "foundationdb")]
    KvCount,
    /// A SQL wave inserts into the scheduled-job table.
    #[cfg(feature = "turso")]
    SqlJobInsert,
}

/// Holds the first commit whose attempt matched `trigger` after `arm`.
struct RaceProbe {
    trigger: Trigger,
    #[cfg(feature = "foundationdb")]
    group_prefix: std::sync::Mutex<Vec<u8>>,
    #[cfg(feature = "foundationdb")]
    accesses: std::sync::Mutex<Vec<(bool, Vec<u8>)>>,
    armed: AtomicBool,
    matched: AtomicBool,
    held: Notify,
    release: Notify,
}

impl RaceProbe {
    fn new(trigger: Trigger) -> Arc<Self> {
        Arc::new(Self {
            trigger,
            #[cfg(feature = "foundationdb")]
            group_prefix: std::sync::Mutex::new(Vec::new()),
            #[cfg(feature = "foundationdb")]
            accesses: std::sync::Mutex::new(Vec::new()),
            armed: AtomicBool::new(false),
            matched: AtomicBool::new(false),
            held: Notify::new(),
            release: Notify::new(),
        })
    }

    #[cfg(feature = "foundationdb")]
    fn set_group_prefix(&self, prefix: &[u8]) {
        *self.group_prefix.lock().unwrap() = prefix.to_vec();
    }

    /// A job-data key (`\x7fj/d/`) that contains the group prefix.
    #[cfg(feature = "foundationdb")]
    fn in_group(&self, key: &[u8]) -> bool {
        let prefix = self.group_prefix.lock().unwrap();
        key.starts_with(b"\x7fj/d/")
            && !prefix.is_empty()
            && key
                .windows(prefix.len())
                .any(|window| window == prefix.as_slice())
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn wait_held(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.held.notified())
            .await
            .expect("the probe held a commit");
    }

    fn match_access(&self) {
        if self.armed.load(Ordering::SeqCst) {
            self.matched.store(true, Ordering::SeqCst);
        }
    }

    fn hold(&self) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if self.matched.swap(false, Ordering::SeqCst) {
                self.armed.store(false, Ordering::SeqCst);
                self.held.notify_one();
                tokio::time::timeout(Duration::from_secs(30), self.release.notified())
                    .await
                    .expect("the held commit was released");
            }
            Ok(())
        })
    }
}

#[cfg(feature = "foundationdb")]
impl kv_backend::KvFaultProbe for RaceProbe {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }

    fn observe_access(&self, access: kv_backend::KvAccess<'_>) {
        let matches = match self.trigger {
            Trigger::KvClearRange => {
                access.kind == kv_backend::KvAccessKind::ClearRange && self.in_group(access.key)
            }
            Trigger::KvGroupWrite => {
                access.kind == kv_backend::KvAccessKind::Write && self.in_group(access.key)
            }
            Trigger::KvCount => {
                if self.armed.load(Ordering::SeqCst) {
                    self.accesses.lock().unwrap().push((
                        access.kind == kv_backend::KvAccessKind::Read,
                        access.key.to_vec(),
                    ));
                }
                false
            }
            #[cfg(feature = "turso")]
            Trigger::SqlJobInsert => false,
        };
        if matches {
            self.match_access();
        }
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        if boundary == kv_backend::KvCommitBoundary::BeforeCommit {
            self.hold()
        } else {
            Box::pin(async { Ok(()) })
        }
    }
}

#[cfg(feature = "turso")]
impl sql_backend::SqlFaultProbe for RaceProbe {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }

    fn before_wave<'a>(
        &'a self,
        statements: &'a [sql_backend::SqlWaveStatement<'a>],
    ) -> BoxFuture<'a, StorageResult<()>> {
        if self.trigger == Trigger::SqlJobInsert
            && statements
                .iter()
                .any(|statement| statement.sql.contains("INSERT INTO sys_scheduled_jobs_v2"))
        {
            self.match_access();
        }
        Box::pin(async { Ok(()) })
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        if boundary == sql_backend::SqlCommitBoundary::BeforeCommit {
            self.hold()
        } else {
            Box::pin(async { Ok(()) })
        }
    }
}

struct Claimed {
    /// Completes the claim; the claim type is not public.
    complete: Box<dyn FnOnce() -> BoxFuture<'static, Result<(), String>> + Send>,
    authority: scheduled_bg_jobs::JobEffectAuthority,
    group: EffectReceiptGroup,
}

async fn claimed_job(store: &TestStore) -> Claimed {
    let kind = JobKind::try_new("receipt-race").unwrap();
    let mut catalog = JobCatalog::new();
    catalog
        .register(
            JobSpec::unit(
                kind.clone(),
                std::num::NonZeroU16::MIN,
                QueueName::try_new("default").unwrap(),
                NeverRuns,
            )
            .policy(
                EffectMode::Fenced,
                FairnessPolicy::low_volume(),
                RetrySelection::Preset(RetryPreset::Maintenance),
                Duration::from_secs(30),
            ),
        )
        .unwrap();
    let system = JobSystemBuilder::new(store.scheduled_job_store.clone())
        .catalog(catalog)
        .namespace(
            Namespace::try_new(format!("receipt-race-{}", uuid::Uuid::new_v4().simple())).unwrap(),
        )
        .owner(OwnerId::from([1; 16]))
        .scheduler(Arc::new(scheduled_bg_jobs::test_support::TestProfile::new()))
        .build()
        .unwrap();
    let now = Timestamp::from_millis(store.now().timestamp_millis());
    let job_id = JobId::from([1; 16]);
    system
        .ensure_seed(
            scheduled_bg_jobs::JobSeed::new(
                kind,
                std::num::NonZeroU16::MIN,
                bytes::Bytes::new(),
                EnqueueOptions::new(FairnessClass::low_volume())
                    .job_id(job_id)
                    .scheduled_at(now),
                now,
            )
            .unwrap(),
            now,
        )
        .await
        .unwrap();
    let claim = system.claim(job_id, now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let group = authority.receipt_group().unwrap();
    let system = Arc::new(system);
    let complete: Box<dyn FnOnce() -> BoxFuture<'static, Result<(), String>> + Send> =
        Box::new(move || {
            Box::pin(async move {
                system
                    .complete(claim, done(), now)
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("{error:?}"))
            })
        });
    Claimed {
        complete,
        authority,
        group,
    }
}

fn effect_request(table: &storage_types::TableName) -> storage_types::TransactWriteItemsRequest {
    serde_json::from_value(
        json!({"ClientRequestToken": "race", "TransactItems": [{"Put": {
            "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "effect"}}
        }}]}),
    )
    .unwrap()
}

async fn group_rows(store: &TestStore, group: &EffectReceiptGroup) -> usize {
    let request = RangeRequest::try_from(RangeRequestInput {
        start: Some(group.prefix().clone()),
        end: Some(group.end().clone()),
        direction: RangeDirection::Forward,
        item_limit: NonZeroU32::new(16).unwrap(),
        byte_limit: NonZeroUsize::new(1_048_576).unwrap(),
        continuation: None,
    })
    .unwrap();
    store
        .scheduled_job_store
        .read_range(request)
        .await
        .unwrap()
        .entries
        .len()
}

#[cfg(feature = "foundationdb")]
async fn destination_present(store: &TestStore, table: &storage_types::TableName) -> bool {
    let value = serde_json::to_value(
        store
            .operations
            .get_item(
                serde_json::from_value(
                    json!({"TableName": table, "Key": {"pk": {"S": "customer-1"}}}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    value.get("Item").is_some()
}

fn done() -> JobOutcome {
    JobOutcome::Complete(OutcomeCode::try_new("done").unwrap())
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn given_foundationdb_effect_racing_completion_when_completion_commits_last_then_no_receipt_survives()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let probe = RaceProbe::new(Trigger::KvClearRange);
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        store.scheduled_job_store.bootstrap().await.unwrap();
        let job = claimed_job(&store).await;
        probe.set_group_prefix(job.group.prefix().as_bytes());
        probe.arm();
        let completion = tokio::spawn((job.complete)());
        probe.wait_held().await;
        store
            .operations
            .transact_write_job_effect(effect_request(&table), job.authority.clone())
            .await
            .unwrap();
        probe.release.notify_one();
        completion.await.unwrap().unwrap();
        assert_eq!(group_rows(&store, &job.group).await, 0);
        assert!(
            store
                .operations
                .find_job_effect_receipt("race", job.authority)
                .await
                .unwrap()
                .is_none()
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn given_foundationdb_completion_racing_effect_when_completion_commits_first_then_effect_is_rejected()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    // The probe is installed at store creation, before the group exists;
    // it matches nothing until `set_group_prefix` runs.
    let probe = RaceProbe::new(Trigger::KvGroupWrite);
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        store.scheduled_job_store.bootstrap().await.unwrap();
        let job = claimed_job(&store).await;
        probe.set_group_prefix(job.group.prefix().as_bytes());
        probe.arm();
        let effect = {
            let operations = store.operations.clone();
            let authority = job.authority.clone();
            let request = effect_request(&table);
            tokio::spawn(async move {
                operations
                    .transact_write_job_effect(request, authority)
                    .await
            })
        };
        probe.wait_held().await;
        (job.complete)().await.unwrap();
        probe.release.notify_one();
        let error = effect.await.unwrap().unwrap_err();
        assert!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::ConditionalCheckFailed
        ));
        assert!(!destination_present(&store, &table).await);
        assert_eq!(group_rows(&store, &job.group).await, 0);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn given_foundationdb_job_effect_when_writing_then_one_receipt_read_is_in_the_group_and_none_in_the_receipt_table()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let probe = RaceProbe::new(Trigger::KvCount);
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        store.scheduled_job_store.bootstrap().await.unwrap();
        let job = claimed_job(&store).await;
        let mut request = effect_request(&table);
        request.client_request_token = Some("count".to_owned());
        probe.set_group_prefix(job.group.prefix().as_bytes());
        probe.arm();
        store
            .operations
            .transact_write_job_effect(request, job.authority.clone())
            .await
            .unwrap();
        probe.armed.store(false, Ordering::SeqCst);
        let accesses = probe.accesses.lock().unwrap();
        let group_reads = accesses
            .iter()
            .filter(|(is_read, key)| *is_read && probe.in_group(key))
            .count();
        let receipt_table = accesses
            .iter()
            .filter(|(_, key)| key.starts_with(b"\x7ft/"))
            .count();
        assert_eq!(group_reads, 1);
        assert_eq!(receipt_table, 0);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_effect_holding_the_writer_when_completion_waits_then_no_receipt_survives() {
    if TestStore::selected_backend() != "turso" {
        return;
    }
    let probe = RaceProbe::new(Trigger::SqlJobInsert);
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        store.scheduled_job_store.bootstrap().await.unwrap();
        let job = claimed_job(&store).await;
        probe.arm();
        let effect = {
            let operations = store.operations.clone();
            let authority = job.authority.clone();
            let request = effect_request(&table);
            tokio::spawn(async move {
                operations
                    .transact_write_job_effect(request, authority)
                    .await
            })
        };
        probe.wait_held().await;
        let completion = tokio::spawn((job.complete)());
        // Give the completion the chance to queue on the single writer
        // lease; the assertions below do not depend on its timing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        probe.release.notify_one();
        effect.await.unwrap().unwrap();
        completion.await.unwrap().unwrap();
        assert_eq!(group_rows(&store, &job.group).await, 0);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
