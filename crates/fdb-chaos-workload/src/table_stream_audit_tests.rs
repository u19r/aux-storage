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

use super::table_stream_audit::audit_stream_entries;

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
                    StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                        storage_types::ConflictCause::Logical("injected audit abort"),
                    ))
                    .into(),
                )
            }
            Self::Unknown => Some(StorageError::commit_unknown("injected unknown outcome")),
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
    let store = KvStore::initialize_format(
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap(),
        storage_types::InitialStreamLineages::ONE,
    )
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
    for key in ["first", "second", "first", "second"] {
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
    let binding = DynamoPort::with_attempt(
        &store,
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadOnly,
            storage_types::DeadlineClass::PointRead,
        )
        .start(storage_types::AttemptDeadlineSource::attempt_deadlines(
            &store,
        )),
        move |attempt| {
            Box::pin(async move {
                storage_provider::resolve_table_in_attempt(attempt, &cache, &name).await
            })
        },
    )
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
    let driver = RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256)
        .unwrap()
        .with_fault_probe(Arc::new(fault));
    let clock = AuditClock::default();
    let result =
        audit_stream_entries(driver, target, Arc::new(TableMetadataCache::new()), &clock).await;
    (result, clock.0.load(Ordering::SeqCst))
}

#[tokio::test]
async fn given_native_history_when_audit_aborts_twice_then_fresh_attempt_checks_all_entries() {
    let (result, sleeps) = audit_fault(AuditFault::Conflicts(AtomicUsize::new(2))).await;
    let (rows, comparisons) = result.unwrap();
    assert_eq!(rows, 4);
    assert_eq!(comparisons, 2);
    assert_eq!(sleeps, 2);
}

#[tokio::test]
async fn given_persistent_native_conflicts_when_audited_then_exhaustion_remains_an_error() {
    let (result, sleeps) = audit_fault(AuditFault::Conflicts(AtomicUsize::new(100))).await;
    assert!(matches!(
        result.unwrap_err().as_ref(),
        StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
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
        _mode: storage_types::AttemptMode,
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
    let backend = RocksDbBackend::initialize(
        path.clone(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap()
    .with_fault_probe(Arc::new(WriteAbort(Arc::clone(&armed))));
    let clock = Arc::new(AuditClock::default());
    let store = KvStore::initialize_format_with_clock(
        backend,
        storage_types::InitialStreamLineages::ONE,
        clock.clone(),
    )
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
    let driver =
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap();
    let (rows, _) = audit_stream_entries(
        driver,
        target,
        Arc::new(TableMetadataCache::new()),
        clock.as_ref(),
    )
    .await
    .unwrap();
    assert_eq!(rows, 4, "aborted attempts published history");
}

async fn load_fixture_entries(
    path: std::path::PathBuf,
    target: TableHistoryTarget,
) -> (
    Arc<storage_types::BoundTableInfo>,
    stream_provider::SystemStreamHistory,
    Vec<stream_provider::PositionedStreamEntry>,
) {
    use kv_backend::{KvBackend, ReadProtection, catalog::KvCatalogAttempt};
    use stream_provider::StreamAttempt;
    let driver =
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap();
    driver
        .with_attempt(
            storage_types::AttemptPlan::new(
                storage_types::AttemptMode::ReadOnly,
                storage_types::DeadlineClass::PointRead,
            )
            .start(driver.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Snapshot);
                    let (table, _) = storage_provider::resolve_table_in_attempt(
                        &mut catalog,
                        &TableMetadataCache::new(),
                        &target.table_name,
                    )
                    .await?;
                    let history = catalog.read_system_page_start_snapshot().await?;
                    let reads = history
                        .retirement_boundaries()
                        .unwrap()
                        .iter()
                        .map(|position| {
                            Ok(stream_provider::TableHistoryIndexRead {
                                lane: storage_types::TableHistoryLane::try_from((
                                    table.history_id,
                                    history.as_ref().source_scope(),
                                    position,
                                ))?,
                                exclusive_start: None,
                                through: storage_types::TableStreamSequence::try_from(
                                    position.offset(),
                                )?,
                                direction: stream_provider::ReadDirection::Forward,
                                limit: std::num::NonZeroU32::new(100).unwrap(),
                            })
                        })
                        .collect::<StorageResult<Vec<_>>>()?;
                    let entries = stream_provider::read_table_history_indexes(&mut catalog, &reads)
                        .await?
                        .into_iter()
                        .flat_map(|page| {
                            assert!(!page.more);
                            page.entries
                        })
                        .collect();
                    Ok((table, history, entries))
                })
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn given_missing_system_membership_when_audited_then_unretired_entry_is_rejected() {
    use kv_backend::{KvAttempt, KvBackend, KvMutation};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let target = create_history_fixture(path.clone()).await;
    let (_, _, entries) = load_fixture_entries(path.clone(), target.clone()).await;
    let key = kv_backend::stream_write::key_layout::record_key(&entries[0].position).unwrap();
    let driver =
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap();
    driver
        .with_attempt(
            storage_types::AttemptPlan::new(
                storage_types::AttemptMode::ReadWrite,
                storage_types::DeadlineClass::Write,
            )
            .start(driver.attempt_deadlines()),
            move |attempt| {
                Box::pin(
                    async move { attempt.native_stage(vec![KvMutation::DeleteUnchunked { key }]) },
                )
            },
        )
        .await
        .unwrap();
    let result = audit_stream_entries(
        driver,
        target,
        Arc::new(TableMetadataCache::new()),
        &AuditClock::default(),
    )
    .await;
    assert!(result.is_err(), "unretired feed membership disappeared");
}

#[tokio::test]
async fn given_successor_with_wrong_old_image_when_compared_then_chain_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let target = create_history_fixture(path.clone()).await;
    let (table, history, mut entries) = load_fixture_entries(path, target).await;
    let successor = entries
        .iter_mut()
        .find(|entry| entry.entry.old_image().is_some())
        .unwrap();
    let mut value = serde_json::to_value(&successor.entry).unwrap();
    value["old_image"]["item"]["extra"] = serde_json::json!({ "S": "wrong predecessor" });
    successor.entry = storage_types::storage_serde::from_bytes(
        &storage_types::storage_serde::to_bytes(&value).unwrap(),
    )
    .unwrap();
    assert!(
        super::table_stream_audit::verify_image_chains(&entries, history.as_ref(), &table).is_err()
    );
}

#[tokio::test]
async fn given_retained_first_successor_when_compared_then_retired_predecessor_is_not_required() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let target = create_history_fixture(path.clone()).await;
    let (table, history, mut entries) = load_fixture_entries(path, target).await;
    entries.remove(0);
    let comparisons =
        super::table_stream_audit::verify_image_chains(&entries, history.as_ref(), &table).unwrap();
    assert_eq!(comparisons, 1);
}

#[tokio::test]
async fn given_system_retirement_when_audited_then_retained_table_images_still_form_chains() {
    use kv_backend::{KvBackend, ReadProtection, catalog::KvCatalogAttempt};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let target = create_history_fixture(path.clone()).await;
    let (_, history, _) = load_fixture_entries(path.clone(), target.clone()).await;
    let request = stream_provider::SystemStreamTrimPageRequest::try_new(
        history.as_ref().source_scope().clone(),
        TimestampMillis::from_timestamp(i64::MAX),
        100,
        None,
    )
    .unwrap();
    let driver =
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap();
    let result = driver
        .with_attempt(
            storage_types::AttemptPlan::new(
                storage_types::AttemptMode::ReadWrite,
                storage_types::DeadlineClass::Write,
            )
            .start(driver.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Conflict);
                    stream_provider::execute_trim_page(&mut catalog, request).await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(result.reclaimed_records(), 4);
    let (entries, comparisons) = audit_stream_entries(
        driver,
        target,
        Arc::new(TableMetadataCache::new()),
        &AuditClock::default(),
    )
    .await
    .unwrap();
    assert_eq!((entries, comparisons), (4, 2));
}
