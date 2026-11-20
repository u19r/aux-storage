use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use kv_backend::{
    KvCommitBoundary, KvFaultCall, KvFaultOperation, KvFaultProbe, KvStore, rocksdb::RocksDbBackend,
};
use storage_provider::{DynamoOperations, DynamoPort, TableMetadataCache};
use storage_types::{
    AttemptDeadlines, AttemptMode, AttemptPlan, BoundTableInfo, CreateTableRequest, DeadlineClass,
    ProjectionType, StorageError, StorageResult,
};

use crate::table_atomicity_phases::{IndexStartResult, start_index, submit_index};

#[derive(Clone, Copy)]
enum Fault {
    LostAcknowledgment,
    BeforeWrite,
    ObservationConflict,
    ObservationUnknown,
}

struct StartProbe {
    fault: Fault,
    armed: AtomicBool,
    stages: AtomicUsize,
    commits: AtomicUsize,
}

impl StartProbe {
    fn new(fault: Fault) -> Self {
        Self {
            fault,
            armed: AtomicBool::new(false),
            stages: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
        }
    }
}

impl KvFaultProbe for StartProbe {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError> {
        if call.operation == KvFaultOperation::Stage {
            self.stages.fetch_add(1, Ordering::SeqCst);
        }
        let target = match self.fault {
            Fault::LostAcknowledgment => return None,
            Fault::BeforeWrite => KvFaultOperation::Stage,
            Fault::ObservationConflict | Fault::ObservationUnknown => KvFaultOperation::GetBatch,
        };
        if call.operation != target || !self.armed.swap(false, Ordering::SeqCst) {
            return None;
        }
        Some(match self.fault {
            Fault::ObservationConflict => {
                storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                    storage_types::ConflictCause::Logical("injected observation conflict"),
                ))
                .into()
            }
            _ => StorageError::commit_unknown("injected unconfirmed operation"),
        })
    }

    fn at_commit_boundary(
        &self,
        boundary: KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == KvCommitBoundary::AfterDurableAcknowledgement {
                self.commits.fetch_add(1, Ordering::SeqCst);
                if matches!(self.fault, Fault::LostAcknowledgment)
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    return Err(StorageError::internal(
                        "suppress actual durable acknowledgment",
                    ));
                }
            }
            Ok(())
        })
    }
}

fn expected_request() -> CreateTableRequest {
    serde_json::from_value(serde_json::json!({
        "TableName": "StartRecovery",
        "AttributeDefinitions": [{"AttributeName":"pk","AttributeType":"S"}, {"AttributeName":"category","AttributeType":"S"}],
        "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{"IndexName":"by_category", "KeySchema":[{"AttributeName":"category","KeyType":"HASH"}], "Projection":{"ProjectionType":"ALL"}}]
    })).unwrap()
}

async fn binding(store: &KvStore<RocksDbBackend>) -> Arc<BoundTableInfo> {
    let cache = TableMetadataCache::new();
    DynamoPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                storage_provider::resolve_table_in_attempt(
                    attempt,
                    &cache,
                    &expected_request().table_name,
                )
                .await
                .map(|(table, _)| table)
            })
        },
    )
    .await
    .unwrap()
}

async fn fixture(
    fault: Fault,
) -> (
    tempfile::TempDir,
    KvStore<RocksDbBackend>,
    Arc<StartProbe>,
    Arc<BoundTableInfo>,
) {
    let root = tempfile::tempdir().unwrap();
    let probe = Arc::new(StartProbe::new(fault));
    let driver =
        RocksDbBackend::initialize(root.path().join("store"), AttemptDeadlines::default(), 256)
            .unwrap()
            .with_fault_probe(probe.clone());
    let store = KvStore::initialize_format(driver, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let mut base = expected_request();
    base.global_secondary_indexes = None;
    base.attribute_definitions
        .retain(|attribute| attribute.attribute_name == "pk");
    store.create_table(base).await.unwrap();
    let original = binding(&store).await;
    probe.stages.store(0, Ordering::SeqCst);
    probe.commits.store(0, Ordering::SeqCst);
    (root, store, probe, original)
}

#[tokio::test]
async fn given_lost_index_start_ack_when_observed_then_original_build_is_resumed_without_write() {
    let (_root, store, probe, original) = fixture(Fault::LostAcknowledgment).await;
    probe.armed.store(true, Ordering::SeqCst);
    assert!(
        submit_index(&store, expected_request())
            .await
            .unwrap_err()
            .is_commit_unknown()
    );
    let committed = binding(&store).await;
    assert_eq!(committed.binding.gsis.len(), 1);
    let writes = probe.stages.load(Ordering::SeqCst);
    let commits = probe.commits.load(Ordering::SeqCst);
    assert_eq!(
        start_index(&store, &original, expected_request())
            .await
            .unwrap(),
        IndexStartResult::Existing
    );
    assert_eq!(probe.stages.load(Ordering::SeqCst), writes);
    assert_eq!(probe.commits.load(Ordering::SeqCst), commits);
    assert_eq!(binding(&store).await.binding, committed.binding);
}

#[tokio::test]
async fn given_unknown_start_without_commit_when_absence_is_observed_then_complete_request_is_submitted()
 {
    let (_root, store, probe, original) = fixture(Fault::BeforeWrite).await;
    probe.armed.store(true, Ordering::SeqCst);
    assert!(
        submit_index(&store, expected_request())
            .await
            .unwrap_err()
            .is_commit_unknown()
    );
    assert!(binding(&store).await.binding.gsis.is_empty());
    assert_eq!(probe.commits.load(Ordering::SeqCst), 0);
    assert_eq!(
        start_index(&store, &original, expected_request())
            .await
            .unwrap(),
        IndexStartResult::Submitted
    );
    let installed = binding(&store).await;
    assert_eq!(installed.binding.gsis.len(), 1);
    assert_eq!(
        installed.global_secondary_indexes.as_ref().unwrap()[0].key_schema,
        expected_request().global_secondary_indexes.unwrap()[0].key_schema
    );
}

#[tokio::test]
async fn given_failed_index_observation_when_start_is_scheduled_then_no_write_is_submitted() {
    for fault in [Fault::ObservationConflict, Fault::ObservationUnknown] {
        let (_root, store, probe, original) = fixture(fault).await;
        probe.armed.store(true, Ordering::SeqCst);
        let error = start_index(&store, &original, expected_request())
            .await
            .unwrap_err();
        match fault {
            Fault::ObservationConflict => assert_eq!(
                error.attempt_kind(),
                Some(storage_types::AttemptKind::Conflict)
            ),
            _ => assert!(error.is_commit_unknown()),
        }
        assert_eq!(probe.stages.load(Ordering::SeqCst), 0);
        assert_eq!(probe.commits.load(Ordering::SeqCst), 0);
        assert!(binding(&store).await.binding.gsis.is_empty());
        start_index(&store, &original, expected_request())
            .await
            .unwrap();
        assert_eq!(binding(&store).await.binding.gsis.len(), 1);
    }
}

#[tokio::test]
async fn given_different_durable_index_when_start_is_scheduled_then_definition_is_rejected_without_write()
 {
    let (_root, store, probe, original) = fixture(Fault::LostAcknowledgment).await;
    let mut different = expected_request();
    different.global_secondary_indexes.as_mut().unwrap()[0]
        .projection
        .projection_type = Some(ProjectionType::KeysOnly);
    start_index(&store, &original, different).await.unwrap();
    let stages = probe.stages.load(Ordering::SeqCst);
    let error = start_index(&store, &original, expected_request())
        .await
        .unwrap_err();
    assert!(crate::common::storage_error_detail(&error).contains("durable definition differs"));
    assert_eq!(probe.stages.load(Ordering::SeqCst), stages);
}

#[tokio::test]
async fn given_recreated_table_when_unknown_start_is_recovered_then_new_incarnation_is_rejected() {
    let (_root, store, probe, original) = fixture(Fault::LostAcknowledgment).await;
    store
        .delete_table(storage_types::DeleteTableRequest {
            table_name: expected_request().table_name,
        })
        .await
        .unwrap();
    let mut base = expected_request();
    base.global_secondary_indexes = None;
    base.attribute_definitions
        .retain(|attribute| attribute.attribute_name == "pk");
    store.create_table(base).await.unwrap();
    let stages = probe.stages.load(Ordering::SeqCst);
    let error = start_index(&store, &original, expected_request())
        .await
        .unwrap_err();
    assert!(crate::common::storage_error_detail(&error).contains("table incarnation changed"));
    assert_eq!(probe.stages.load(Ordering::SeqCst), stages);
    assert!(binding(&store).await.binding.gsis.is_empty());
}

#[derive(Default)]
struct InitialReadClock(AtomicUsize);

impl storage_types::Clock for InitialReadClock {
    fn now(&self) -> storage_types::TimestampMillis {
        storage_types::TimestampMillis::now()
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        assert!(!duration.is_zero());
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn given_initial_binding_conflict_when_start_observes_then_fresh_read_succeeds_after_clock_delay()
 {
    let (_root, store, probe, original) = fixture(Fault::ObservationConflict).await;
    let clock = InitialReadClock::default();
    probe.armed.store(true, Ordering::SeqCst);
    let observed = crate::table_atomicity::read_initial_table_binding(
        &store,
        expected_request().table_name,
        &clock,
    )
    .await
    .unwrap();
    assert_eq!(observed.history_id, original.history_id);
    assert_eq!(clock.0.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stages.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn given_initial_binding_unknown_when_start_observes_then_unknown_is_preserved_without_retry()
{
    let (_root, store, probe, _original) = fixture(Fault::ObservationUnknown).await;
    let clock = InitialReadClock::default();
    probe.armed.store(true, Ordering::SeqCst);
    let error = crate::table_atomicity::read_initial_table_binding(
        &store,
        expected_request().table_name,
        &clock,
    )
    .await
    .unwrap_err();
    assert!(error.is_commit_unknown());
    assert_eq!(clock.0.load(Ordering::SeqCst), 0);
    assert_eq!(probe.stages.load(Ordering::SeqCst), 0);
}
