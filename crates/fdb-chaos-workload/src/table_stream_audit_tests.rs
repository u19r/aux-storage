use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kv_backend::{
    KvCommitBoundary, KvFaultCall, KvFaultOperation, KvFaultProbe, KvStore, rocksdb::RocksDbBackend,
};
use storage_provider::{DynamoOperations, DynamoPort, TableMetadataCache};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BillingMode, Clock, CreateTableRequest,
    GetItemRequest, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, PutItemRequest,
    StorageEnum, StorageError, StorageResult, StreamSpecification, StreamViewType,
    TableHistoryTarget, TableName, TimestampMillis, WireItem,
};

use super::table_stream_audit::audit_stream_pointers;

enum AuditFault {
    Conflicts(AtomicUsize),
    Unknown,
}

impl KvFaultProbe for AuditFault {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError> {
        if call.operation != KvFaultOperation::GetBatch {
            return None;
        }
        match self {
            Self::Conflicts(left)
                if left
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok() =>
            {
                Some(
                    StorageEnum::TransactionConflict {
                        message: "injected audit abort".into(),
                    }
                    .into(),
                )
            }
            Self::Unknown => Some(StorageError::sync_submission_ambiguous(
                None,
                "injected unknown outcome",
            )),
            Self::Conflicts(_) => None,
        }
    }
}

#[derive(Default)]
struct AuditClock(AtomicUsize);

impl Clock for AuditClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::now()
    }

    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        assert!(!duration.is_zero());
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

async fn create_history_fixture(path: std::path::PathBuf) -> TableHistoryTarget {
    let store = KvStore::initialize_format(RocksDbBackend::initialize(path).unwrap())
        .await
        .unwrap();
    let table_name = TableName::new("PointerAudit");
    let mut request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    );
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    store.create_table(request).await.unwrap();
    for key in ["first", "second"] {
        let item = AttributeMap::from_iter([("pk".into(), AttributeValue::S(key.into()))]);
        store
            .put_item(
                PutItemRequest::builder()
                    .table_name(table_name.clone())
                    .item(WireItem::try_from(item).unwrap())
                    .build(),
            )
            .await
            .unwrap();
    }
    let cache = store.metadata_cache();
    let name = table_name.clone();
    let binding = store
        .with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                storage_provider::resolve_table_in_attempt(attempt, &cache, &name).await
            })
        })
        .await
        .unwrap()
        .0;
    TableHistoryTarget {
        table_name,
        history_id: binding.history_id,
    }
}

async fn audit_fault(fault: AuditFault) -> (StorageResult<(u64, u64)>, usize) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let target = create_history_fixture(path.clone()).await;
    let driver = RocksDbBackend::initialize(path)
        .unwrap()
        .with_fault_probe(Arc::new(fault));
    let clock = AuditClock::default();
    let result =
        audit_stream_pointers(driver, target, Arc::new(TableMetadataCache::new()), &clock).await;
    (result, clock.0.load(Ordering::SeqCst))
}

#[tokio::test]
async fn given_native_history_when_audit_aborts_twice_then_fresh_attempt_resolves_all_pointers() {
    let (result, sleeps) = audit_fault(AuditFault::Conflicts(AtomicUsize::new(2))).await;
    let (rows, decoupled) = result.unwrap();
    assert_eq!(rows, 2);
    assert!(decoupled > 0);
    assert_eq!(sleeps, 2);
}

#[tokio::test]
async fn given_persistent_native_conflicts_when_audited_then_exhaustion_remains_an_error() {
    let (result, sleeps) = audit_fault(AuditFault::Conflicts(AtomicUsize::new(100))).await;
    assert!(matches!(
        result.unwrap_err().as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
    assert_eq!(
        sleeps,
        storage_provider::DYNAMO_WRITE_RETRY.max_attempts() as usize - 1
    );
}

#[tokio::test]
async fn given_unknown_native_outcome_when_audited_then_it_is_preserved_without_retry() {
    let (result, sleeps) = audit_fault(AuditFault::Unknown).await;
    assert!(result.unwrap_err().is_commit_unknown());
    assert_eq!(sleeps, 0);
}

struct WriteAbort(Arc<std::sync::atomic::AtomicBool>);

impl KvFaultProbe for WriteAbort {
    fn fail(&self, _call: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: KvCommitBoundary,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == KvCommitBoundary::BeforeCommit && self.0.load(Ordering::SeqCst) {
                return Err(StorageError::internal("injected known-aborted write"));
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn given_native_write_abort_when_recorded_as_failed_then_current_and_history_have_no_effects()
{
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let target = create_history_fixture(path.clone()).await;
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backend = RocksDbBackend::initialize(path.clone())
        .unwrap()
        .with_fault_probe(Arc::new(WriteAbort(Arc::clone(&armed))));
    let clock = Arc::new(AuditClock::default());
    let store = KvStore::initialize_format_with_clock(backend, clock.clone())
        .await
        .unwrap();
    armed.store(true, Ordering::SeqCst);
    let changed = AttributeMap::from_iter([
        ("pk".into(), AttributeValue::S("first".into())),
        (
            "payload".into(),
            AttributeValue::S("must-not-publish".into()),
        ),
    ]);
    let error = store
        .put_item(
            PutItemRequest::builder()
                .table_name(target.table_name.clone())
                .item(WireItem::try_from(changed).unwrap())
                .build(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        super::table_atomicity_recording::FailureDisposition::from(&error),
        super::table_atomicity_recording::FailureDisposition::KnownAborted
    );
    assert_eq!(
        clock.0.load(Ordering::SeqCst),
        storage_provider::DYNAMO_WRITE_RETRY.max_attempts() as usize - 1
    );
    armed.store(false, Ordering::SeqCst);
    let mut request = GetItemRequest::new(
        target.table_name.clone(),
        KeyAttributes::from([("pk".into(), AttributeValue::S("first".into()))]),
    );
    request.consistent_read = Some(true);
    let actual = store
        .get_item(request)
        .await
        .unwrap()
        .item
        .unwrap()
        .to_attribute_map()
        .unwrap();
    assert_eq!(
        actual,
        AttributeMap::from_iter([("pk".into(), AttributeValue::S("first".into()))])
    );
    drop(store);
    let driver = RocksDbBackend::initialize(path).unwrap();
    let (rows, _) = audit_stream_pointers(
        driver,
        target,
        Arc::new(TableMetadataCache::new()),
        clock.as_ref(),
    )
    .await
    .unwrap();
    assert_eq!(rows, 2, "aborted attempts published history");
}
