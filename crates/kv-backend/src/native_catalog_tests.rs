//! Real native ports and public catalog publishers. Catalog reads use checked
//! native versions.

use storage_provider::{CatalogAttempt as _, DynamoOperations as _, DynamoPort as _};
use storage_types::{AttemptPlan, CatalogVersion, DeadlineClass, StorageError, StorageResult};
use stream_provider::{
    StreamOperations as _, SystemStreamLineage, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use crate::{AttemptMode, KvAttempt, KvBackend, KvMutation, KvStore};

/// Record a catalog change the way every DDL path does: with a write to a
/// protected lookup key (the commit-time debug assertion requires one).
async fn record_protected_change<A: KvAttempt>(attempt: &mut A) -> StorageResult<()> {
    attempt.native_record_catalog_change().await?;
    attempt.stage(vec![KvMutation::PutUnchunked {
        key: vec![crate::constants::LOOKUP_FAMILY],
        value: Vec::new(),
    }])
}

#[derive(Default)]
struct CatalogCalls(
    std::sync::Mutex<(usize, usize)>,
    std::sync::Mutex<(usize, usize)>,
);

impl crate::KvFaultProbe for CatalogCalls {
    fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
        None
    }
    fn observe_access(&self, access: crate::KvAccess<'_>) {
        {
            let mut routes = self.1.lock().unwrap();
            if access.kind == crate::KvAccessKind::Read
                && access.key == crate::constants::SYSTEM_STREAM_SCOPE_KEY
            {
                routes.0 += 1;
            }
            if access.kind == crate::KvAccessKind::ReadRange
                && access.key == crate::constants::SYSTEM_STREAM_LINEAGE_PREFIX
            {
                routes.1 += 1;
            }
        }
        if access.key != crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY
            && access.key != crate::constants::ROCKSDB_CATALOG_VERSION_KEY
        {
            return;
        }
        let mut counts = self.0.lock().unwrap();
        match access.kind {
            crate::KvAccessKind::Read => counts.0 += 1,
            crate::KvAccessKind::Write => counts.1 += 1,
            _ => {}
        }
    }
}

impl CatalogCalls {
    fn take(&self, operation: &str) -> (usize, usize) {
        let counts = std::mem::take(&mut *self.0.lock().unwrap());
        println!(
            "native_catalog {operation}: reads={} writes={}",
            counts.0, counts.1
        );
        counts
    }
}

#[derive(Default)]
struct SuppressCommitReply(std::sync::atomic::AtomicBool);

impl crate::KvFaultProbe for SuppressCommitReply {
    fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
        None
    }
    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == crate::KvCommitBoundary::AfterDurableAcknowledgement
                && self.0.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StorageError::internal(
                    "deliberately suppressed durable reply",
                ));
            }
            Ok(())
        })
    }
}

async fn observe<B: KvBackend>(backend: &B) -> CatalogVersion {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let version = attempt
                        .native_catalog_version()
                        .await?
                        .expect("published catalog");
                    assert_eq!(attempt.native_catalog_version().await?, Some(version));
                    Ok(version)
                })
            },
        )
        .await
        .unwrap()
}

async fn check_publication<B: KvBackend + 'static>(backend: B, calls: &CatalogCalls) {
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let bootstrap = calls.take("bootstrap");
    assert_eq!(bootstrap.1, 1);
    let initial = observe(store.backend.as_ref()).await;
    calls.take("initial_observation");
    store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    crate::stream_history::initialize_system_stream(
                        attempt,
                        storage_types::InitialStreamLineages::ONE,
                    )
                    .await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(calls.take("reopen"), (0, 0));
    assert_eq!(
        observe(store.backend.as_ref()).await,
        initial,
        "reopen does not publish"
    );
    calls.take("reopen_observation");
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "catalog_publication", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    assert_eq!(
        calls.take("create").1,
        0,
        "CreateTable publishes no catalog change"
    );
    let created = observe(store.backend.as_ref()).await;
    assert_eq!(created, initial);
    calls.take("create_observation");
    store
        .put_item(
            storage_types::PutItemRequest::try_from(serde_json::json!({
                "TableName": "catalog_publication", "Item": {"pk": {"S": "kept"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(calls.take("ordinary_item").1, 0);
    let cache = store.metadata_cache();
    let name = storage_types::TableName::new("catalog_publication");
    storage_provider::DynamoPort::with_attempt(
        &store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
        ),
        move |attempt| {
            Box::pin(async move {
                let (table, _) = storage_provider::RequestTableBindings::default()
                    .resolve_table(attempt, &cache, &name)
                    .await?;
                assert!(
                    attempt.identity_for(&name).is_err(),
                    "warm authority must not load full identity"
                );
                assert!(
                    !storage_provider::DeleteTableAttempt::load_external_cleanup_requirement(
                        attempt, &table
                    )
                    .await?
                );
                assert!(
                    attempt.identity_for(&name).is_ok(),
                    "lifecycle owner loads its full protected identity"
                );
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    calls.take("warm_lifecycle_identity");
    assert_eq!(
        observe(store.backend.as_ref()).await,
        created,
        "ordinary item leaves catalog unchanged"
    );
    calls.take("item_observation");
    store
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": "catalog_publication", "AuxStreamDurationHours": 48
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(calls.take("replace_layout").1, 1);
    let updated = observe(store.backend.as_ref()).await;
    assert_ne!(updated, created, "layout replacement publishes");
    calls.take("updated_observation");
    store.update_table(serde_json::from_value(serde_json::json!({
        "TableName": "catalog_publication", "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    assert_eq!(calls.take("stream_settings").1, 1);
    let streaming = observe(store.backend.as_ref()).await;
    assert_ne!(streaming, updated);
    calls.take("stream_observation");
    for key in 0..4 {
        store
            .put_item(
                storage_types::PutItemRequest::try_from(serde_json::json!({
                    "TableName": "catalog_publication", "Item": {"pk": {"S": format!("gsi-{key}")}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(calls.take("gsi_seed_items").1, 0);
    store
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": "catalog_publication", "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "by_pk", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }}]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(calls.take("gsi_create").1, 1);
    let mut last = observe(store.backend.as_ref()).await;
    assert_ne!(last, streaming);
    calls.take("gsi_create_observation");
    store
        .put_item(stream_put("catalog_publication", "route_warm"))
        .await
        .unwrap();
    calls.take("route_warm");
    std::mem::take(&mut *calls.1.lock().unwrap());
    let mut progress_pages = 0;
    let mut phase_pages = 0;
    let mut completed = false;
    for _ in 0..48 {
        use storage_provider::MaintenanceOperations as _;
        let result = store
            .advance_gsi_build(storage_provider::maintenance::GsiBuildRequest {
                destination: b"aux-storage:catalog-proof:gsi:v1"
                    .to_vec()
                    .try_into()
                    .unwrap(),
                job: [7; 16].into(),
                fence: std::num::NonZeroU64::MIN,
                page_limit: storage_backfill::GsiPageLimit::try_from(1usize).unwrap(),
                captured_at: storage_types::TimestampMillis::from_timestamp(1_800_000_000_000),
            })
            .await
            .unwrap();
        let counts = calls.take("gsi_advance");
        let current = observe(store.backend.as_ref()).await;
        calls.take("gsi_advance_observation");
        if counts.1 == 0 {
            assert_eq!(current, last, "pure GSI progress retains version");
            if matches!(result, storage_types::maintenance::StepResult::Advanced(())) {
                progress_pages += 1;
            }
        } else {
            assert_eq!(counts.1, 1);
            assert_ne!(current, last);
            phase_pages += 1;
        }
        // Real progress keeps the version and route slot; a real phase
        // change forces protected scope/lineage reload on the next write.
        std::mem::take(&mut *calls.1.lock().unwrap());
        store
            .put_item(stream_put("catalog_publication", "route_after_page"))
            .await
            .unwrap();
        let routes = std::mem::take(&mut *calls.1.lock().unwrap());
        assert_eq!(routes, if counts.1 == 0 { (0, 0) } else { (1, 1) });
        calls.take("gsi_page_writer");
        store
            .put_item(stream_put("catalog_publication", "route_after_page_warm"))
            .await
            .unwrap();
        assert_eq!(std::mem::take(&mut *calls.1.lock().unwrap()), (0, 0));
        calls.take("gsi_page_writer_warm");
        println!(
            "native_routes gsi_page: catalog_writes={} scope={} lineage={}",
            counts.1, routes.0, routes.1
        );
        last = current;
        if matches!(result, storage_types::maintenance::StepResult::Complete) {
            completed = true;
            break;
        }
    }
    assert!(completed && progress_pages > 0 && phase_pages > 0);
    println!("native_catalog gsi_pages: progress={progress_pages} phase={phase_pages}");
    store.update_time_to_live(serde_json::from_value(serde_json::json!({
        "TableName": "catalog_publication", "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}
    })).unwrap()).await.unwrap();
    assert_eq!(calls.take("ttl_policy").1, 1);
    let ttl = observe(store.backend.as_ref()).await;
    assert_ne!(ttl, last, "TTL binding publishes");
    calls.take("ttl_observation");
    let mut previous = ttl;
    for _ in 0..2 {
        storage_provider::DynamoPort::with_attempt(
            &store,
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
                storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
            ),
            |attempt| {
                Box::pin(async move {
                    let name = storage_types::TableName::try_from("catalog_publication").unwrap();
                    assert!(attempt.attempt.native_catalog_version().await?.is_some());
                    let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                    let changed = attempt.store_external_cleanup_barrier(&name, stamp).await?;
                    let mut joined = crate::catalog::KvCatalogAttempt::new(
                        &mut *attempt.attempt,
                        crate::ReadProtection::Conflict,
                    );
                    assert_eq!(joined.attempt.native_catalog_version().await?, None);
                    assert!(joined.read_catalog_stamp(&name).await? == Some(changed));
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(calls.take("cleanup_barrier").1, 1);
        let current = observe(store.backend.as_ref()).await;
        calls.take("barrier_observation");
        assert_ne!(current, previous, "each cleanup barrier revision publishes");
        previous = current;
    }
    let before_split = observe(store.backend.as_ref()).await;
    let scope = store.system_stream_scope().await.unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    let children = ["left", "right"].map(|side| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                side,
                format!("catalog-{side}"),
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    let request = SystemStreamSplitRequest::try_new(
        scope,
        parent,
        children,
        SystemStreamSplitOperationId::try_new("catalog-split").unwrap(),
    )
    .unwrap();
    calls.take("before_split_observation");
    store.split_system_stream(request.clone()).await.unwrap();
    assert_eq!(calls.take("split").1, 1);
    let split = observe(store.backend.as_ref()).await;
    assert_ne!(split, before_split);
    calls.take("split_observation");
    store.split_system_stream(request).await.unwrap();
    assert_eq!(calls.take("split_replay"), (0, 0));
    assert_eq!(
        observe(store.backend.as_ref()).await,
        split,
        "split receipt replay leaves catalog unchanged"
    );
    calls.take("replay_observation");
    store
        .delete_table(
            serde_json::from_value(serde_json::json!({"TableName": "catalog_publication"}))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(calls.take("delete").1, 1);
    assert_ne!(observe(store.backend.as_ref()).await, split);
}

async fn check_attempt<B: KvBackend>(backend: B, probe: &SuppressCommitReply) {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { record_protected_change(attempt).await }),
        )
        .await
        .unwrap();
    let before = observe(&backend).await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_catalog_version().await?.is_some());
                    record_protected_change(attempt).await?;
                    assert!(attempt.catalog_changed());
                    assert_eq!(attempt.native_catalog_version().await?, None);
                    record_protected_change(attempt).await?;
                    assert_eq!(attempt.native_catalog_version().await?, None);
                    // A newly constructed joined view still uses root state.
                    let joined = crate::catalog::KvCatalogAttempt::new(
                        attempt,
                        crate::ReadProtection::Conflict,
                    );
                    assert_eq!(joined.attempt.native_catalog_version().await?, None);
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let after = observe(&backend).await;
    assert_ne!(after, before);
    if let (CatalogVersion::RocksDb(before), CatalogVersion::RocksDb(after)) = (before, after) {
        assert_eq!(after.get(), before.get() + 1, "two changes publish once");
    }
    let rejected: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    record_protected_change(attempt).await?;
                    Err(StorageError::internal("deliberately aborted publication"))
                })
            },
        )
        .await;
    assert!(rejected.is_err());
    assert_eq!(observe(&backend).await, after);
    let read_only = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(record_protected_change(attempt).await.is_err());
                    Ok(())
                })
            },
        )
        .await;
    assert!(read_only.is_err(), "caught mode failure poisons settlement");
    assert_eq!(observe(&backend).await, after);
    probe.0.store(true, std::sync::atomic::Ordering::SeqCst);
    let unknown = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { record_protected_change(attempt).await }),
        )
        .await;
    assert!(unknown.unwrap_err().is_commit_unknown());
    assert_ne!(
        observe(&backend).await,
        after,
        "unknown reply retains durable native publication"
    );
}

#[cfg(feature = "rocksdb")]
fn rocks(path: &std::path::Path) -> crate::rocksdb::RocksDbBackend {
    crate::rocksdb::RocksDbBackend::initialize(path.to_path_buf(), Default::default(), 256).unwrap()
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_catalog_when_public_publishers_run_then_only_catalog_changes_publish() {
    let root = tempfile::tempdir().unwrap();
    let calls = std::sync::Arc::new(CatalogCalls::default());
    check_publication(rocks(root.path()).with_fault_probe(calls.clone()), &calls).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_attempt_when_catalog_changes_then_root_observation_is_invalidated_once() {
    let root = tempfile::tempdir().unwrap();
    let probe = std::sync::Arc::new(SuppressCommitReply::default());
    check_attempt(rocks(root.path()).with_fault_probe(probe.clone()), &probe).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_corrupt_or_exhausted_counter_when_published_then_settlement_is_rejected() {
    for bytes in [
        vec![0; 8],
        vec![1; 7],
        vec![1; 9],
        u64::MAX.to_be_bytes().to_vec(),
    ] {
        let root = tempfile::tempdir().unwrap();
        let backend = rocks(root.path());
        backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt.native_stage(vec![KvMutation::PutUnchunked {
                            key: crate::constants::ROCKSDB_CATALOG_VERSION_KEY.to_vec(),
                            value: bytes,
                        }])
                    })
                },
            )
            .await
            .unwrap();
        let observed = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                |attempt| Box::pin(async move { attempt.native_catalog_version().await }),
            )
            .await;
        if observed.is_ok() {
            assert_eq!(
                observed.unwrap(),
                Some(CatalogVersion::RocksDb(
                    std::num::NonZeroU64::new(u64::MAX).unwrap()
                ))
            );
        }
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        assert!(record_protected_change(attempt).await.is_err());
                        Ok(())
                    })
                },
            )
            .await;
        assert!(result.is_err());
    }
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
async fn fdb() -> crate::foundationdb::FoundationDbBackend {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let prefix = storage_types::ambient::uuid_v4().to_string();
    crate::foundationdb::FoundationDbBackend::initialize(crate::foundationdb::FoundationDbOptions {
        cluster_file: Some(&cluster),
        subspace_prefix: Some(prefix.as_bytes()),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_catalog_when_public_publishers_run_then_only_catalog_changes_publish() {
    let calls = std::sync::Arc::new(CatalogCalls::default());
    check_publication(fdb().await.with_fault_probe(calls.clone()), &calls).await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_attempt_when_catalog_changes_then_root_observation_is_invalidated_once() {
    let probe = std::sync::Arc::new(SuppressCommitReply::default());
    check_attempt(fdb().await.with_fault_probe(probe.clone()), &probe).await;
}

#[test]
fn given_equal_commit_version_when_transaction_order_differs_then_catalog_tokens_differ() {
    let first = [0u8; 10];
    let mut second = first;
    second[9] = 1;
    assert_ne!(
        CatalogVersion::FoundationDb(first),
        CatalogVersion::FoundationDb(second)
    );
    assert_eq!(&first[..8], &second[..8]);
    #[cfg(feature = "foundationdb")]
    {
        assert_eq!(
            crate::native_catalog::decode_foundationdb_version(&first).unwrap(),
            CatalogVersion::FoundationDb(first)
        );
        assert_eq!(
            crate::native_catalog::decode_foundationdb_version(&second).unwrap(),
            CatalogVersion::FoundationDb(second)
        );
        for length in [0, 8, 9, 11] {
            assert!(crate::native_catalog::decode_foundationdb_version(&vec![0; length]).is_err());
        }
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_unobserved_counter_when_publication_is_cancelled_then_attempt_cannot_settle() {
    use futures::FutureExt as _;
    let root = tempfile::tempdir().unwrap();
    struct HoldFirstRead(std::sync::atomic::AtomicBool);
    impl crate::KvFaultProbe for HoldFirstRead {
        fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
            None
        }
        fn before_wave(
            &self,
            wave: crate::KvWave,
        ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
            Box::pin(async move {
                if wave.kind == crate::KvWaveKind::GetBatch
                    && self.0.swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    futures::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    }
    let backend = rocks(root.path()).with_fault_probe(std::sync::Arc::new(HoldFirstRead(
        std::sync::atomic::AtomicBool::new(true),
    )));
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    // Hold the native read before dispatch so cancellation is
                    // deterministic. Dropping it must reject the whole attempt
                    // rather than permit commit.
                    assert!(
                        attempt
                            .native_record_catalog_change()
                            .now_or_never()
                            .is_none()
                    );
                    assert!(attempt.settlement().require_open().is_err());
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert_eq!(attempt.native_catalog_version().await?, None);
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_protected_token_when_another_attempt_publishes_then_stale_writer_rejects_without_item()
 {
    let backend = std::sync::Arc::new(fdb().await);
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { record_protected_change(attempt).await }),
        )
        .await
        .unwrap();
    // The same namespace: the publisher changes the protected stamp.
    let publisher = std::sync::Arc::clone(&backend);
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_catalog_version().await?.is_some());
                    // The version is a snapshot hint; the trusted stamp is
                    // protected without a read.
                    attempt.native_protect_reads(&[b"protected-stamp".to_vec()], &[])?;
                    publisher
                        .with_attempt(
                            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                                .start(publisher.attempt_deadlines()),
                            |other| {
                                Box::pin(async move {
                                    record_protected_change(other).await?;
                                    other.stage(vec![KvMutation::Put {
                                        key: b"protected-stamp".to_vec(),
                                        value: b"next".to_vec(),
                                    }])
                                })
                            },
                        )
                        .await?;
                    attempt.stage(vec![KvMutation::Put {
                        key: b"stale-item".to_vec(),
                        value: b"must-not-commit".to_vec(),
                    }])
                })
            },
        )
        .await;
    assert_eq!(
        result.unwrap_err().attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert_eq!(
                        attempt
                            .get(b"stale-item", crate::ReadProtection::Snapshot)
                            .await?,
                        None
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_catalog_version().await?.is_some());
                    attempt.stage(vec![KvMutation::Put {
                        key: b"stale-item".to_vec(),
                        value: b"fresh-retry".to_vec(),
                    }])
                })
            },
        )
        .await
        .unwrap();
}

async fn check_root_routes_and_ordinals<B: KvBackend>(backend: B) {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    crate::stream_history::initialize_system_stream(
                        attempt,
                        storage_types::InitialStreamLineages::ONE,
                    )
                    .await
                })
            },
        )
        .await
        .unwrap();
    let rejected = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let routes = crate::stream_history::read_writer_routes(attempt).await?;
                    attempt.install_writer_routes(routes.clone())?;
                    assert_eq!(attempt.reserve_system_event_number()?, 0);
                    // Construct joined views and flushes without giving either
                    // a counter.
                    let _view = crate::catalog::KvCatalogAttempt::new(
                        attempt,
                        crate::ReadProtection::Conflict,
                    );
                    attempt.stage(vec![KvMutation::Put {
                        key: b"overflow-prefix".to_vec(),
                        value: vec![1],
                    }])?;
                    attempt
                        .get(b"missing", crate::ReadProtection::Conflict)
                        .await?;
                    attempt.install_writer_routes(routes.clone())?;
                    for ordinal in 1..=u16::MAX {
                        assert_eq!(attempt.reserve_system_event_number()?, ordinal);
                    }
                    assert!(attempt.reserve_system_event_number().is_err());
                    // Catching the error must not permit committing the
                    // already-staged prefix.
                    Ok(())
                })
            },
        )
        .await;
    assert!(rejected.is_err());
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert_eq!(
                        attempt
                            .get(b"overflow-prefix", crate::ReadProtection::Conflict)
                            .await?,
                        None
                    );
                    assert_eq!(attempt.reserve_system_event_number()?, 0);
                    let routes = crate::stream_history::read_writer_routes(attempt).await?;
                    attempt.install_writer_routes(routes)?;
                    record_protected_change(attempt).await?;
                    assert!(attempt.writer_routes().is_none());
                    assert_eq!(attempt.reserve_system_event_number()?, 1);
                    let routes = crate::stream_history::read_writer_routes(attempt).await?;
                    attempt.install_writer_routes(routes)?;
                    record_protected_change(attempt).await?;
                    assert!(attempt.writer_routes().is_none());
                    assert_eq!(attempt.reserve_system_event_number()?, 2);
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_root_routes_when_views_flush_or_overflow_then_ordinal_and_settlement_are_preserved()
 {
    let root = tempfile::tempdir().unwrap();
    check_root_routes_and_ordinals(rocks(root.path())).await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_root_routes_when_views_flush_or_overflow_then_ordinal_and_settlement_are_preserved()
 {
    check_root_routes_and_ordinals(fdb().await).await;
}

fn stream_put(table: &str, value: &str) -> storage_types::PutItemRequest {
    storage_types::PutItemRequest::try_from(serde_json::json!({
        "TableName": table, "Item": {"pk": {"S": "same-key"}, "value": {"S": value}}
    }))
    .unwrap()
}

async fn stage_stream_put<A: crate::KvAttempt>(
    attempt: &mut A,
    table: &str,
    value: &str,
) -> StorageResult<()> {
    use storage_provider::{DynamoStreamWriteAttempt as _, ItemWriteAttempt as _};
    let mut catalog =
        crate::catalog::KvCatalogAttempt::new(attempt, crate::ReadProtection::Conflict);
    let shape = std::sync::Arc::new(storage_provider::PutItemShape::try_from_request(
        stream_put(table, value),
    )?);
    let (table, _) = storage_provider::resolve_table_in_attempt(
        &mut catalog,
        &storage_provider::TableMetadataCache::default(),
        &shape.table_name,
    )
    .await?;
    storage_provider::prepare_writer_routes_in_attempt(&mut catalog, None).await?;
    let bound = shape.bind_table(table)?;
    let (old, lifecycle) = catalog.observe_for_write(&bound.table, &bound.key).await?;
    let (accepted, _) = bound.accept_old(old, lifecycle)?.into_result()?;
    storage_provider::prepare_accepted_upsert(
        &mut catalog,
        accepted,
        storage_types::TimestampMillis::now(),
    )
    .await?;
    catalog.flush_dynamo_write().await
}

async fn check_joined_stream_writes<B: KvBackend + 'static>(backend: B) -> KvStore<B> {
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    for table in ["joined_a", "joined_b"] {
        store.create_table(serde_json::from_value(serde_json::json!({
            "TableName": table, "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        })).unwrap()).await.unwrap();
        store.put_item(stream_put(table, "original")).await.unwrap();
    }
    let scope = store.system_stream_scope().await.unwrap();
    assert_eq!(
        store
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap()
            .len(),
        1,
        "every selected key uses the same actual lineage, exposing feed-key collisions"
    );
    store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    // One logical batch, with a rebuilt borrowed catalog and
                    // explicit route installation per member.
                    stage_stream_put(attempt, "joined_a", "first").await?;
                    attempt
                        .get(b"joined-flush-probe", crate::ReadProtection::Conflict)
                        .await?;
                    stage_stream_put(attempt, "joined_b", "second").await?;
                    attempt
                        .get(b"joined-flush-probe", crate::ReadProtection::Conflict)
                        .await?;
                    stage_stream_put(attempt, "joined_a", "third").await
                })
            },
        )
        .await
        .unwrap();
    let page = store
        .get_system_stream_page(
            stream_provider::SystemStreamPageRequest::try_new(scope, None, 20).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        page.records().len(),
        5,
        "two seed events plus all three accepted batch events survive in SYSTEM"
    );
    let positions = page
        .records()
        .iter()
        .map(|record| record.system_position.clone().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        positions
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        5
    );
    let batch = &positions[2..];
    assert!(
        batch
            .iter()
            .all(|position| position.lineage_id() == batch[0].lineage_id()
                && position.shard_id() == batch[0].shard_id())
    );
    if let Some(stamp) = batch[0].offset().versionstamp() {
        for (ordinal, position) in batch.iter().enumerate() {
            let actual = position.offset().versionstamp().unwrap();
            assert_eq!(&actual[..10], &stamp[..10]);
            assert_eq!(
                u16::from_be_bytes(actual[10..].try_into().unwrap()),
                ordinal as u16
            );
        }
    } else {
        let first = batch[0].offset().order().unwrap();
        for (ordinal, position) in batch.iter().enumerate() {
            assert_eq!(position.offset().order().unwrap(), first + ordinal as u64);
        }
    }
    let records = page.into_records();
    for (record, (table, value)) in records[2..].iter().zip([
        ("joined_a", "first"),
        ("joined_b", "second"),
        ("joined_a", "third"),
    ]) {
        assert_eq!(record.source_table_name.as_ref().unwrap().as_ref(), table);
        assert_eq!(
            serde_json::to_value(record.item.as_ref().unwrap()).unwrap(),
            serde_json::json!({"pk":{"S":"same-key"},"value":{"S":value}})
        );
    }
    store.backend.with_attempt(AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(store.backend.attempt_deadlines()), move |attempt| Box::pin(async move {
        let mut histories = std::collections::HashMap::new();
        for (index, record) in records.iter().enumerate() {
            let position = record.system_position.as_ref().unwrap();
            let feed = attempt.get(&crate::stream_write::key_layout::record_key(position)?, crate::ReadProtection::Snapshot).await?.unwrap();
            let reference = crate::stream_write::record::FeedReference::decode(&feed)?;
            let bytes = attempt.get(&crate::stream_write::key_layout::entry_key(reference.history_id, position)?, crate::ReadProtection::Snapshot).await?.unwrap();
            let entry = stream_provider::PositionedStreamEntry::decode_value(reference.history_id, position.clone(), &bytes)?;
            assert_eq!(entry.source_table_name, *record.source_table_name.as_ref().unwrap());
            assert_eq!(entry.entry.new_image().unwrap().item.to_attribute_map()?, *record.item.as_ref().unwrap());
            let old = entry.entry.old_image().map(|image| serde_json::to_value(image.item.to_attribute_map().unwrap()).unwrap());
            let expected_old = if index < 2 { None } else { Some(serde_json::json!({"pk":{"S":"same-key"},"value":{"S":if index == 4 {"first"} else {"original"}}})) };
            assert_eq!(old, expected_old, "canonical old image follows the actual per-key chain");
            *histories.entry(reference.history_id).or_insert(0usize) += 1;
        }
        let mut counts = Vec::new();
        for (history, expected) in histories {
            let begin = crate::stream_write::key_layout::table_entry_prefix(history)?;
            let end = crate::range::prefix_upper_bound(&begin).unwrap();
            let rows = attempt.scan(crate::KvRange { entry_bounds: None, begin, end, reverse: false,
                limit: std::num::NonZeroU32::new(20).unwrap(), max_bytes: std::num::NonZeroUsize::new(1_048_576).unwrap()
            }, crate::ReadProtection::Snapshot).await?;
            assert!(!rows.more);
            assert_eq!(rows.entries.len(), expected, "canonical table rows must also survive independently of SYSTEM references");
            counts.push(expected);
        }
        counts.sort(); assert_eq!(counts, [2, 3]);
        Ok(())
    })).await.unwrap();
    for (table, value) in [("joined_a", "third"), ("joined_b", "second")] {
        let current = store
            .get_item(
                serde_json::from_value(
                    serde_json::json!({"TableName":table,"Key":{"pk":{"S":"same-key"}}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(current.item.unwrap().to_attribute_map().unwrap()).unwrap(),
            serde_json::json!({"pk":{"S":"same-key"},"value":{"S":value}})
        );
    }
    store
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_stream_batch_when_joined_views_flush_then_all_feed_and_table_images_survive() {
    let root = tempfile::tempdir().unwrap();
    check_joined_stream_writes(rocks(root.path())).await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_stream_batch_when_joined_views_flush_then_all_feed_and_table_images_survive() {
    let store = check_joined_stream_writes(fdb().await).await;
    store.backend.delete_namespace_keys().await.unwrap();
}

#[derive(Default)]
struct WriterRouteCalls {
    catalog: std::sync::Mutex<(usize, usize, usize)>,
    reads: std::sync::Mutex<(usize, usize)>,
    suppress: SuppressCommitReply,
}

impl crate::KvFaultProbe for WriterRouteCalls {
    fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
        None
    }
    fn observe_access(&self, access: crate::KvAccess<'_>) {
        if access.kind == crate::KvAccessKind::Read {
            let mut catalog = self.catalog.lock().unwrap();
            if access.key == crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY
                || access.key == crate::constants::ROCKSDB_CATALOG_VERSION_KEY
            {
                catalog.0 += 1;
            }
            if access.key.first() == Some(&crate::constants::LOOKUP_FAMILY) {
                catalog.1 += 1;
            }
            if matches!(
                crate::table_key_layout::table_data_family(access.key),
                Some((_, crate::table_key_layout::TableSubFamily::Catalog))
            ) {
                catalog.2 += 1;
            }
        }
        let mut counts = self.reads.lock().unwrap();
        if access.kind == crate::KvAccessKind::Read
            && access.key == crate::constants::SYSTEM_STREAM_SCOPE_KEY
        {
            counts.0 += 1;
        }
        if access.kind == crate::KvAccessKind::ReadRange
            && access.key == crate::constants::SYSTEM_STREAM_LINEAGE_PREFIX
        {
            counts.1 += 1;
        }
    }
    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        crate::KvFaultProbe::at_commit_boundary(&self.suppress, boundary, mode)
    }
}

impl WriterRouteCalls {
    fn take(&self) -> (usize, usize) {
        std::mem::take(&mut *self.reads.lock().unwrap())
    }
}

async fn check_route_publication_after_unknown<B: KvBackend + 'static>(
    backend: B,
    calls: &WriterRouteCalls,
) -> KvStore<B> {
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "route_unknown", "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    // Warm metadata only. The first streamed mutation must still load the
    // route.
    store
        .get_item(
            serde_json::from_value(
                serde_json::json!({"TableName":"route_unknown","Key":{"pk":{"S":"same-key"}}}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    calls.take();
    calls
        .suppress
        .0
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        store
            .put_item(stream_put("route_unknown", "unknown"))
            .await
            .is_err()
    );
    assert_eq!(
        calls.take(),
        (1, 1),
        "first actual stream write loads a cold route"
    );
    assert!(!calls.suppress.0.load(std::sync::atomic::Ordering::SeqCst));
    store
        .put_item(stream_put("route_unknown", "acknowledged"))
        .await
        .unwrap();
    assert_eq!(
        calls.take(),
        (1, 1),
        "unknown acknowledgement cannot publish a reusable route"
    );
    store
        .put_item(stream_put("route_unknown", "warm"))
        .await
        .unwrap();
    assert_eq!(
        calls.take(),
        (0, 0),
        "acknowledged success publishes the route"
    );
    let scope = store.system_stream_scope().await.unwrap();
    let records = store
        .get_system_stream_page(
            stream_provider::SystemStreamPageRequest::try_new(scope, None, 20).unwrap(),
        )
        .await
        .unwrap()
        .into_records();
    assert_eq!(
        records.len(),
        3,
        "unknown durable success is neither replayed nor lost"
    );
    for (record, value) in records.iter().zip(["unknown", "acknowledged", "warm"]) {
        assert_eq!(
            serde_json::to_value(record.item.as_ref().unwrap()).unwrap(),
            serde_json::json!({"pk":{"S":"same-key"},"value":{"S":value}})
        );
    }
    store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(store.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    for (index, record) in records.iter().enumerate() {
                        let position = record.system_position.as_ref().unwrap();
                        let feed = attempt
                            .get(
                                &crate::stream_write::key_layout::record_key(position)?,
                                crate::ReadProtection::Snapshot,
                            )
                            .await?
                            .unwrap();
                        let history =
                            crate::stream_write::record::FeedReference::decode(&feed)?.history_id;
                        let bytes = attempt
                            .get(
                                &crate::stream_write::key_layout::entry_key(history, position)?,
                                crate::ReadProtection::Snapshot,
                            )
                            .await?
                            .unwrap();
                        let entry = stream_provider::PositionedStreamEntry::decode_value(
                            history,
                            position.clone(),
                            &bytes,
                        )?;
                        assert_eq!(
                            entry
                                .entry
                                .old_image()
                                .map(|image| image.item.to_attribute_map().unwrap()),
                            index
                                .checked_sub(1)
                                .map(|previous| records[previous].item.clone().unwrap())
                        );
                    }
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    store
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName":"route_unknown", "AuxStreamDurationHours":48
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    calls.take();
    store
        .put_item(stream_put("route_unknown", "metadata_changed"))
        .await
        .unwrap();
    assert_eq!(
        calls.take(),
        (1, 1),
        "public metadata change invalidates the route slot"
    );
    store
        .put_item(stream_put("route_unknown", "metadata_warm"))
        .await
        .unwrap();
    assert_eq!(calls.take(), (0, 0));
    let scope = store.system_stream_scope().await.unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    let children = ["route-left", "route-right"].map(|side| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                side,
                format!("{side}-lineage"),
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    store
        .split_system_stream(
            SystemStreamSplitRequest::try_new(
                scope,
                parent,
                children,
                SystemStreamSplitOperationId::try_new("route-reload-split").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    calls.take();
    store
        .put_item(stream_put("route_unknown", "split_changed"))
        .await
        .unwrap();
    assert_eq!(
        calls.take(),
        (1, 1),
        "public split invalidates the route slot"
    );
    store
        .put_item(stream_put("route_unknown", "split_warm"))
        .await
        .unwrap();
    assert_eq!(calls.take(), (0, 0));
    store
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_cold_route_when_commit_reply_is_unknown_then_only_acknowledged_success_warms_route()
 {
    let root = tempfile::tempdir().unwrap();
    let calls = std::sync::Arc::new(WriterRouteCalls::default());
    check_route_publication_after_unknown(
        rocks(root.path()).with_fault_probe(calls.clone()),
        &calls,
    )
    .await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_cold_route_when_commit_reply_is_unknown_then_only_acknowledged_success_warms_route()
 {
    let calls = std::sync::Arc::new(WriterRouteCalls::default());
    let store =
        check_route_publication_after_unknown(fdb().await.with_fault_probe(calls.clone()), &calls)
            .await;
    store.backend.delete_namespace_keys().await.unwrap();
}

async fn check_independent_route_owners<B: KvBackend + 'static>(
    backend: B,
    calls: &WriterRouteCalls,
) -> KvStore<B> {
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "route_owners", "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "route_owners_other", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    // These are separate cache owners sharing one backend and native SDK
    // process. They are deliberately not described as nodes or updaters.
    let owners = (0..12)
        .map(|_| {
            store
                .clone()
                .with_metadata_cache_ttl(std::time::Duration::from_secs(60))
        })
        .collect::<Vec<_>>();
    for owner in &owners {
        owner
            .put_item(stream_put("route_owners", "warm"))
            .await
            .unwrap();
    }
    // CreateTable publishes nothing; DeleteTable is the unrelated catalog
    // change.
    store
        .delete_table(
            serde_json::from_value(serde_json::json!({"TableName": "route_owners_other"})).unwrap(),
        )
        .await
        .unwrap();
    let mut total = (0, 0);
    for (index, owner) in owners.iter().enumerate() {
        calls.take();
        std::mem::take(&mut *calls.catalog.lock().unwrap());
        owner
            .put_item(stream_put("route_owners", "after_publication"))
            .await
            .unwrap();
        let routes = calls.take();
        let catalog = std::mem::take(&mut *calls.catalog.lock().unwrap());
        assert_eq!(routes, (1, 1), "cache owner {index} reloads independently");
        assert_eq!(
            catalog,
            (1, 1, 0),
            "same table stamp retains metadata payload"
        );
        total.0 += routes.0;
        total.1 += routes.1;
        owner
            .put_item(stream_put("route_owners", "warm_again"))
            .await
            .unwrap();
        assert_eq!(calls.take(), (0, 0));
        println!(
            "native_routes cache_owner={index} scope={} lineage={} token={} name={} metadata={}",
            routes.0, routes.1, catalog.0, catalog.1, catalog.2
        );
        let response = owner
            .get_item(
                serde_json::from_value(
                    serde_json::json!({"TableName":"route_owners","Key":{"pk":{"S":"same-key"}}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(response.item.unwrap().to_attribute_map().unwrap()).unwrap(),
            serde_json::json!({"pk":{"S":"same-key"},"value":{"S":"warm_again"}})
        );
    }
    assert_eq!(total, (12, 12));
    store
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_twelve_cache_owners_when_unrelated_catalog_changes_then_each_reloads_routes() {
    let root = tempfile::tempdir().unwrap();
    let calls = std::sync::Arc::new(WriterRouteCalls::default());
    check_independent_route_owners(rocks(root.path()).with_fault_probe(calls.clone()), &calls)
        .await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_twelve_cache_owners_when_unrelated_catalog_changes_then_each_reloads_routes() {
    let calls = std::sync::Arc::new(WriterRouteCalls::default());
    let store =
        check_independent_route_owners(fdb().await.with_fault_probe(calls.clone()), &calls).await;
    store.backend.delete_namespace_keys().await.unwrap();
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_warm_public_operations_when_native_tracing_is_enabled_then_results_are_preserved() {
    let calls = std::sync::Arc::new(CatalogCalls::default());
    let store = KvStore::initialize_format(
        fdb().await.with_fault_probe(calls.clone()),
        storage_types::InitialStreamLineages::ONE,
    )
    .await
    .unwrap();
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "network_qualification", "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    store
        .put_item(stream_put("network_qualification", "seed"))
        .await
        .unwrap();
    let get = || {
        storage_types::GetItemRequest::try_from(serde_json::json!({
            "TableName": "network_qualification", "Key": {"pk": {"S": "same-key"}}
        }))
        .unwrap()
    };
    store.get_item(get()).await.unwrap();
    calls.take("network_preparation");
    store.backend.trace_next_attempt("n7-warm-get");
    let item = store.get_item(get()).await.unwrap().item.unwrap();
    assert_eq!(
        serde_json::to_value(item.to_attribute_map().unwrap()).unwrap()["value"],
        serde_json::json!({"S": "seed"})
    );
    assert_eq!(calls.take("network_warm_get"), (1, 0));
    store.backend.trace_next_attempt("n7-warm-stream-put");
    store
        .put_item(stream_put("network_qualification", "changed"))
        .await
        .unwrap();
    assert_eq!(calls.take("network_warm_stream_put"), (1, 0));
    let item = store.get_item(get()).await.unwrap().item.unwrap();
    assert_eq!(
        serde_json::to_value(item.to_attribute_map().unwrap()).unwrap()["value"],
        serde_json::json!({"S": "changed"})
    );
    let scope = store.system_stream_scope().await.unwrap();
    let page = store
        .get_system_stream_page(
            stream_provider::SystemStreamPageRequest::try_new(scope, None, 20).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.records().len(), 2);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    println!("native_network public: warm_get=seed streamed_put=changed stream_records=2");
    drop(store);
    crate::foundationdb::shutdown_foundationdb_network().unwrap();
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[derive(Default)]
struct WriteCostCalls(std::sync::Mutex<Vec<String>>);

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
impl crate::KvFaultProbe for WriteCostCalls {
    fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
        None
    }
    fn observe(&self, event: crate::KvNativeEvent) {
        self.0.lock().unwrap().push(format!("{event:?}"));
    }
    fn observe_access(&self, access: crate::KvAccess<'_>) {
        self.0.lock().unwrap().push(format!(
            "access {:?} family={} bytes={} conflict={}",
            access.kind,
            crate::key_family(access.key),
            access.native_bytes,
            access.conflict
        ));
    }
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_paired_write_policies_when_complete_operations_are_traced_then_results_match() {
    use futures::FutureExt as _;
    let outcome = std::panic::AssertUnwindSafe(async {
    use crate::foundationdb::{FoundationDbBackend, FoundationDbOptions};
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let namespace = storage_types::ambient::uuid_v4().to_string();
    let mut stores = Vec::new();
    // Construct positive first: both explicit policies share the same positive network.
    for (policy, age) in [("cached", 100), ("fresh", 0)] {
        let calls = std::sync::Arc::new(WriteCostCalls::default());
        let prefix = format!("{namespace}-{policy:0<6}");
        let backend = FoundationDbBackend::initialize(FoundationDbOptions {
            cluster_file: Some(&cluster), subspace_prefix: Some(prefix.as_bytes()),
            cache_read_version_ms: age, ..Default::default()
        }).await.unwrap().with_fault_probe(calls.clone());
        let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE).await.unwrap();
        for table in ["plain", "streamed"] {
            let mut request = serde_json::json!({"TableName": table,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]});
            if table == "streamed" { request["StreamSpecification"] = serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}); }
            store.create_table(serde_json::from_value(request).unwrap()).await.unwrap();
        }
        stores.push((policy, store, calls));
    }
    let cases = ["put", "update", "delete", "absent", "reject", "equal"];
    // Finish every publisher and seed before warming either metadata/route owner.
    for (_, store, _) in &stores {
        for table in ["plain", "streamed"] {
            for case in cases {
                if case == "absent" || case == "reject" { continue; }
                store.put_item(storage_types::PutItemRequest::try_from(serde_json::json!({
                    "TableName": table, "Item": {"pk":{"S":case}, "value":{"S":"seed"}}
                })).unwrap()).await.unwrap();
            }
        }
    }
    let mut results = Vec::new();
    for (policy, store, calls) in &stores {
        observe(store.backend.as_ref()).await;
        // A real fresh observation follows seeding; all measured keys are independent.
        for table in ["plain", "streamed"] {
            store.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
                "TableName":table,"Key":{"pk":{"S":"put"}},"ConsistentRead":true
            })).unwrap()).await.unwrap();
        }
        let mut responses = Vec::new();
        for table in ["plain", "streamed"] {
            for case in cases {
                let label = format!("n7-cost-{policy}-{table}-{case}-00");
                let input = serde_json::json!({"TableName":table,"Item":{"pk":{"S":case},"value":{"S":if case == "equal" {"seed"} else {"changed"}}},"ReturnValues":"ALL_OLD"});
                calls.0.lock().unwrap().clear();
                let capture = store.backend.trace_operation(&label);
                let response = match case {
                    "delete" | "absent" => serde_json::to_value(store.delete_item(storage_types::DeleteItemRequest::try_from(serde_json::json!({
                        "TableName":table,"Key":{"pk":{"S":case}},"ReturnValues":"ALL_OLD"
                    })).unwrap()).await.unwrap()).unwrap(),
                    "update" => serde_json::to_value(store.update_item(storage_types::UpdateItemRequest::try_from(serde_json::json!({
                        "TableName":table,"Key":{"pk":{"S":case}},"UpdateExpression":"SET #v = :v",
                        "ExpressionAttributeNames":{"#v":"value"},"ExpressionAttributeValues":{":v":{"S":"changed"}},"ReturnValues":"ALL_NEW"
                    })).unwrap()).await.unwrap()).unwrap(),
                    "reject" => {
                        let mut input = input;
                        input["ConditionExpression"] = serde_json::json!("attribute_exists(pk)");
                        let error = store.put_item(storage_types::PutItemRequest::try_from(input).unwrap()).await.unwrap_err();
                        assert!(matches!(error.as_ref(), storage_types::StorageEnum::ConditionalCheckFailed), "{error:?}");
                        serde_json::json!({"error_debug":format!("{error:?}"),"error_display":error.to_string()})
                    }
                    _ => serde_json::to_value(store.put_item(storage_types::PutItemRequest::try_from(input).unwrap()).await.unwrap()).unwrap(),
                };
                drop(capture);
                let events = std::mem::take(&mut *calls.0.lock().unwrap());
                println!("cached_write_record {}", serde_json::json!({"label":label,"response":response,"events":events}));
                responses.push(response);
                let actual = store.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
                    "TableName":table,"Key":{"pk":{"S":case}},"ConsistentRead":true
                })).unwrap()).await.unwrap().item.map(|item| serde_json::to_value(item.to_attribute_map().unwrap()).unwrap());
                let expected = match case { "delete" | "absent" | "reject" => None,
                    _ => Some(serde_json::json!({"pk":{"S":case},"value":{"S":if case == "equal" {"seed"} else {"changed"}}})) };
                assert_eq!(actual, expected);
                println!("cached_write_item {}", serde_json::json!({"label":label,"item":actual}));
            }
        }
        let scope = store.system_stream_scope().await.unwrap();
        let page = store.get_system_stream_page(stream_provider::SystemStreamPageRequest::try_new(scope, None, 100).unwrap()).await.unwrap();
        // Equal-value replacement remains an acknowledged MODIFY on streamed tables.
        println!("cached_write_stream policy={policy} records={:?}", page.records());
        assert_eq!(page.records().len(), 8, "four seeds and four acknowledged writes; rejection and absent deletion add none");
        let positions = page.records().iter().map(|record| record.system_position.as_ref().unwrap()).collect::<Vec<_>>();
        assert_eq!(positions.iter().collect::<std::collections::HashSet<_>>().len(), positions.len());
        for pair in positions.windows(2) { assert!(pair[0] < pair[1]); }
        let records = page.into_records();
        let logical = store.backend.with_attempt(AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(store.backend.attempt_deadlines()), move |attempt| Box::pin(async move {
            let mut logical = Vec::new();
            for record in records {
                let position = record.system_position.as_ref().unwrap();
                assert!(position.offset().versionstamp().is_some());
                let feed = attempt.get(&crate::stream_write::key_layout::record_key(position)?, crate::ReadProtection::Snapshot).await?.unwrap();
                let reference = crate::stream_write::record::FeedReference::decode(&feed)?;
                let bytes = attempt.get(&crate::stream_write::key_layout::entry_key(reference.history_id, position)?, crate::ReadProtection::Snapshot).await?.unwrap();
                let entry = stream_provider::PositionedStreamEntry::decode_value(reference.history_id, position.clone(), &bytes)?;
                logical.push(serde_json::json!({
                    "table":entry.source_table_name.as_ref(), "change":format!("{:?}", entry.entry.change()),
                    "keys":entry.entry.keys(),
                    "old":entry.entry.old_image().map(|image| image.item.to_attribute_map().unwrap()),
                    "new":entry.entry.new_image().map(|image| image.item.to_attribute_map().unwrap())
                }));
            }
            Ok(logical)
        })).await.unwrap();
        let expected_events = [
            ("Insert", "put", None, Some("seed")),
            ("Insert", "update", None, Some("seed")),
            ("Insert", "delete", None, Some("seed")),
            ("Insert", "equal", None, Some("seed")),
            ("Modify", "put", Some("seed"), Some("changed")),
            ("Modify", "update", Some("seed"), Some("changed")),
            ("Remove", "delete", Some("seed"), None),
            ("Modify", "equal", Some("seed"), Some("seed")),
        ];
        let expected: Vec<_> = expected_events.into_iter().map(|(change, key, old, new)| {
            let image = |value| serde_json::json!({"pk":{"S":key},"value":{"S":value}});
            serde_json::json!({"table":"streamed", "change":change, "keys":{"pk":{"S":key}},
                "old":old.map(image), "new":new.map(image)})
        }).collect();
        assert_eq!(logical, expected, "{policy} stream matches independent image oracle");
        println!("cached_write_logical_stream {}", serde_json::json!({"policy":policy,"records":logical}));
        results.push((responses, logical));
    }
    assert_eq!(results[0], results[1], "entire responses and logical stream item sequence match");
    for (_, store, _) in stores { store.backend.delete_namespace_keys().await.unwrap(); }
    }).catch_unwind().await;
    // Unwinding drops the body and all native owners before stopping the
    // network.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    crate::foundationdb::shutdown_foundationdb_network().unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Each public call below is awaited sequentially; no other owner uses this
/// probe.
#[derive(Default)]
struct WriteAuthorityReads(std::sync::Mutex<Vec<usize>>);

impl crate::KvFaultProbe for WriteAuthorityReads {
    fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
        None
    }
    fn observe(&self, event: crate::KvNativeEvent) {
        if let crate::KvNativeEvent::Begin = event {
            self.0.lock().unwrap().push(0);
        }
    }
    fn observe_access(&self, access: crate::KvAccess<'_>) {
        if !matches!(
            access.kind,
            crate::KvAccessKind::Read | crate::KvAccessKind::ReadRange
        ) {
            return;
        }
        // Only native version and table-name/metadata authority may precede
        // the whole-vector decision. Everything else depends on candidates.
        if matches!(access.key.first(), Some(b'M' | b'm'))
            || access.key == crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY
            || access.key == crate::constants::ROCKSDB_CATALOG_VERSION_KEY
        {
            return;
        }
        if let Some(reads) = self.0.lock().unwrap().last_mut() {
            *reads += 1;
        }
    }
}

async fn check_corrupt_candidate_authority<B: KvBackend + 'static>(
    backend: B,
    probe: &WriteAuthorityReads,
    // Undecoded primary reads of the stale try: FoundationDB pipelines the
    // primary row with the version read; RocksDB reads the version first.
    stale_raw_reads: usize,
) -> (KvStore<B>, std::thread::Result<()>) {
    use futures::FutureExt as _;
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap()
        .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
    let outcome = std::panic::AssertUnwindSafe(async {
    let publisher = store.clone().with_metadata_cache_ttl(std::time::Duration::from_secs(60));
    let create = |name: &str| serde_json::from_value(serde_json::json!({
        "TableName":name, "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
        "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
    })).unwrap();
    for name in ["authority_changed", "authority_matching"] {
        store.create_table(create(name)).await.unwrap();
        store.put_item(stream_put(name, "seed")).await.unwrap();
    }
    let mut physical = Vec::new();
    for name in ["authority_changed", "authority_matching"] {
        let cache = store.metadata_cache();
        let name = storage_types::TableName::new(name);
        physical.push(storage_provider::DynamoPort::with_attempt(&store, AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(storage_types::AttemptDeadlineSource::attempt_deadlines(&store)), move |attempt| Box::pin(async move {
            let (table, _) = storage_provider::RequestTableBindings::default().resolve_table(attempt, &cache, &name).await?;
            let request = storage_types::GetItemRequest::try_from(serde_json::json!({"TableName":name,"Key":{"pk":{"S":"same-key"}}})).unwrap();
            crate::item_read::primary_item_key(crate::catalog::table_id_from_history(table.history_id)?, &table, &request.key)
        })).await.unwrap());
    }
    publisher.delete_table(serde_json::from_value(serde_json::json!({"TableName":"authority_changed"})).unwrap()).await.unwrap();
    publisher.create_table(create("authority_changed")).await.unwrap();
    // Retired and matching rows both contain actual invalid physical bytes.
    let corrupt = physical[0].clone();
    store.backend.with_attempt(AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(store.backend.attempt_deadlines()), move |attempt| Box::pin(async move {
        attempt.stage(vec![KvMutation::Put { key: corrupt, value: vec![255] }])
    })).await.unwrap();
    probe.0.lock().unwrap().clear();
    store.put_item(stream_put("authority_changed", "new-incarnation")).await.unwrap();
    let attempts = std::mem::take(&mut *probe.0.lock().unwrap());
    assert_eq!(attempts[0], stale_raw_reads,
        "stale candidate reads at most its undecoded pipelined primary row; never history, routes or chunks: {attempts:?}");
    let result = store.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({"TableName":"authority_changed","Key":{"pk":{"S":"same-key"}}})).unwrap()).await.unwrap();
    assert_eq!(serde_json::to_value(result.item.unwrap().to_attribute_map().unwrap()).unwrap()["value"], serde_json::json!({"S":"new-incarnation"}));
    for has_mismatch in [false, true] {
        storage_provider::DynamoPort::with_attempt(&store, AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write).start(storage_types::AttemptDeadlineSource::attempt_deadlines(&store)), move |attempt| Box::pin(async move {
            let names = ["authority_matching", "authority_changed", "authority_absent"].map(storage_types::TableName::new);
            let stamps = attempt.read_catalog_stamps(&names).await?;
            assert_eq!(stamps.len(), 3);
            assert!(stamps[0].is_some() && stamps[1].is_some() && stamps[2].is_none());
            let first = attempt.read_table_at_stamp(&names[0], stamps[0].unwrap()).await?;
            let second = attempt.read_table_at_stamp(&names[1], stamps[1].unwrap()).await?;
            let tables = [
                (&names[0], Some(storage_provider::TableReadCandidate { table: &first, stamp: stamps[0].unwrap(), catalog_version: None })),
                (&names[1], (!has_mismatch).then_some(storage_provider::TableReadCandidate { table: &second, stamp: stamps[1].unwrap(), catalog_version: None })),
                (&names[2], None),
            ];
            let result = storage_provider::read_write_observations_default(attempt, storage_provider::WriteObservationBatch {
                tables: &tables, reads: &[], receipt_key: None,
                prerequisites: storage_provider::ItemWritePrerequisites { needs_history: false }, cache: None,
            }).await?;
            assert!(result.stamps.unwrap() == stamps, "every requested name retains its result slot");
            let observations = result.observations.ok().unwrap();
            assert_eq!(observations.is_none(), has_mismatch);
            if let Some(observations) = observations { assert!(observations.is_empty(), "valid empty reads retain exact cardinality"); }
            assert!(result.publications.is_empty());
            Ok(())
        })).await.unwrap();
    }
    // Warm both candidates, then change only the second name. A matching
    // first table cannot authorize reads before every requested name matches.
    publisher.delete_table(serde_json::from_value(serde_json::json!({"TableName":"authority_changed"})).unwrap()).await.unwrap();
    publisher.create_table(create("authority_changed")).await.unwrap();
    let corrupt = physical[1].clone();
    store.backend.with_attempt(AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(store.backend.attempt_deadlines()), move |attempt| Box::pin(async move {
        attempt.stage(vec![KvMutation::Put { key: corrupt, value: vec![255] }])
    })).await.unwrap();
    probe.0.lock().unwrap().clear();
    let result = store.transact_write_items(storage_types::TransactWriteItemsRequest::try_from(serde_json::json!({"TransactItems":[
        {"Put":{"TableName":"authority_matching","Item":{"pk":{"S":"same-key"},"value":{"S":"forbidden"}}}},
        {"Put":{"TableName":"authority_changed","Item":{"pk":{"S":"same-key"},"value":{"S":"forbidden"}}}}
    ]})).unwrap()).await;
    let error = result.unwrap_err();
    assert!(!error.class().is_retryable(), "matching corruption must remain permanent: {error:?}");
    assert!(matches!(error.as_ref(), storage_types::StorageEnum::InternalServerError { message } if message == "KV primary item header is truncated"), "{:?}", error.as_ref());
    let attempts = std::mem::take(&mut *probe.0.lock().unwrap());
    assert_eq!(attempts[0], 0,
        "one mismatch must skip the matching table too: {attempts:?}");
    assert!(attempts.len() >= 2,
        "fresh redecision must reach matching corruption: {attempts:?}");
    let current = publisher.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
        "TableName":"authority_changed", "Key":{"pk":{"S":"same-key"}}
    })).unwrap()).await.unwrap();
    assert!(current.item.is_none(), "failed transaction writes no current-incarnation member");
    let corrupt = physical[1].clone();
    store.backend.with_attempt(AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(store.backend.attempt_deadlines()), move |attempt| Box::pin(async move {
        assert_eq!(attempt.get(&corrupt, crate::ReadProtection::Snapshot).await?, Some(vec![255]),
            "failed transaction retains matching-table corruption");
        Ok(())
    })).await.unwrap();

    }).catch_unwind().await;
    (store, outcome)
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_corrupt_retired_rows_when_public_stale_candidates_write_then_whole_vector_precedes_reads()
 {
    let root = tempfile::tempdir().unwrap();
    let probe = std::sync::Arc::new(WriteAuthorityReads::default());
    let (_, outcome) = check_corrupt_candidate_authority(
        rocks(root.path()).with_fault_probe(probe.clone()),
        &probe,
        0,
    )
    .await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_corrupt_retired_rows_when_public_stale_candidates_write_then_whole_vector_precedes_reads()
 {
    let probe = std::sync::Arc::new(WriteAuthorityReads::default());
    let namespace = storage_types::ambient::uuid_v4().to_string();
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let backend = crate::foundationdb::FoundationDbBackend::initialize(
        crate::foundationdb::FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(namespace.as_bytes()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_fault_probe(probe.clone());
    println!("OWNED_FIXTURE created foundationdb namespace={namespace}");
    let (store, outcome) = check_corrupt_candidate_authority(backend, &probe, 1).await;
    store.backend.delete_namespace_keys().await.unwrap();
    assert!(!store.backend.namespace_has_keys().await.unwrap());
    println!("OWNED_FIXTURE deleted foundationdb namespace={namespace}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Records (kind, keys) of each native wave; the flag fails the next point
/// batch before its wave.
#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[derive(Default)]
struct WaveLog(
    std::sync::Mutex<Vec<(crate::KvWaveKind, usize)>>,
    std::sync::atomic::AtomicBool,
);

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
impl crate::KvFaultProbe for WaveLog {
    fn fail(&self, call: crate::KvFaultCall) -> Option<StorageError> {
        (call.operation == crate::KvFaultOperation::GetBatch
            && self.1.swap(false, std::sync::atomic::Ordering::SeqCst))
        .then(|| StorageError::internal("injected point failure before the wave"))
    }
    fn observe(&self, event: crate::KvNativeEvent) {
        if let crate::KvNativeEvent::Wave { kind, keys, .. } = event {
            self.0.lock().unwrap().push((kind, keys));
        }
    }
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
impl WaveLog {
    fn take(&self) -> Vec<(crate::KvWaveKind, usize)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
fn pipeline_keys() -> Vec<Vec<u8>> {
    vec![b"pipeline-a".to_vec(), b"pipeline-b".to_vec()]
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
fn pipeline_range() -> crate::KvRange {
    crate::KvRange {
        entry_bounds: None,
        begin: b"pipeline-".to_vec(),
        end: b"pipeline.".to_vec(),
        reverse: false,
        limit: std::num::NonZeroU32::new(10).unwrap(),
        max_bytes: crate::constants::PAGE_READ_BYTES,
    }
}

/// A published catalog version, an observed `version` and an empty wave log.
#[cfg(all(feature = "foundationdb", feature = "test-support"))]
async fn pipeline_fixture() -> (
    crate::foundationdb::FoundationDbBackend,
    std::sync::Arc<WaveLog>,
    CatalogVersion,
) {
    let log = std::sync::Arc::new(WaveLog::default());
    let backend = fdb().await.with_fault_probe(log.clone());
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { record_protected_change(attempt).await }),
        )
        .await
        .unwrap();
    let version = observe(&backend).await;
    log.take();
    (backend, log, version)
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_pipelined_version_when_point_batch_reads_then_one_wave_carries_both() {
    use crate::KvWaveKind::{GetBatch, ReadVersion};
    let (backend, log, version) = pipeline_fixture().await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_pipeline_catalog_version(version)?);
                    attempt
                        .get_batch(pipeline_keys(), crate::ReadProtection::Snapshot)
                        .await?;
                    assert_eq!(
                        attempt.native_pipelined_catalog_version(),
                        Some(Some(version))
                    );
                    assert_eq!(attempt.native_catalog_version().await?, Some(version));
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(log.take(), vec![(ReadVersion, 0), (GetBatch, 3)]);
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_pipelined_version_when_range_scans_then_first_scan_wave_carries_it() {
    use crate::KvWaveKind::{ReadVersion, Scan};
    let (backend, log, version) = pipeline_fixture().await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_pipeline_catalog_version(version)?);
                    attempt
                        .scan(pipeline_range(), crate::ReadProtection::Snapshot)
                        .await?;
                    attempt
                        .scan(pipeline_range(), crate::ReadProtection::Snapshot)
                        .await?;
                    assert_eq!(
                        attempt.native_pipelined_catalog_version(),
                        Some(Some(version))
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(log.take(), vec![(ReadVersion, 0), (Scan, 1), (Scan, 1)]);
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_pending_pipeline_when_version_is_read_first_then_own_wave_and_request_is_cancelled()
 {
    use crate::KvWaveKind::{GetBatch, ReadVersion};
    let (backend, log, version) = pipeline_fixture().await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_pipeline_catalog_version(version)?);
                    assert_eq!(attempt.native_catalog_version().await?, Some(version));
                    attempt
                        .get_batch(pipeline_keys(), crate::ReadProtection::Snapshot)
                        .await?;
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(
        log.take(),
        vec![(ReadVersion, 0), (GetBatch, 1), (GetBatch, 2)]
    );
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_catalog_change_when_pipelined_then_no_observation_is_kept() {
    use crate::KvWaveKind::GetBatch;
    let (backend, log, version) = pipeline_fixture().await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_pipeline_catalog_version(version)?);
                    record_protected_change(attempt).await?;
                    attempt
                        .get_batch(pipeline_keys(), crate::ReadProtection::Snapshot)
                        .await?;
                    assert_eq!(attempt.native_pipelined_catalog_version(), None);
                    assert_eq!(attempt.native_catalog_version().await?, None);
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let waves = log.take();
    assert!(waves.contains(&(GetBatch, 2)), "{waves:?}");
    assert!(!waves.contains(&(GetBatch, 3)), "{waves:?}");
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_observed_version_when_pipeline_is_asked_then_it_declines() {
    let (backend, _, version) = pipeline_fixture().await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    attempt.native_catalog_version().await?;
                    assert!(!attempt.native_pipeline_catalog_version(version)?);
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_point_failure_before_wave_when_pipelined_then_nothing_is_recorded() {
    let (backend, log, version) = pipeline_fixture().await;
    log.1.store(true, std::sync::atomic::Ordering::SeqCst);
    let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
    let sink = std::sync::Arc::clone(&captured);
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_pipeline_catalog_version(version)?);
                    let error = attempt
                        .get_batch(pipeline_keys(), crate::ReadProtection::Snapshot)
                        .await
                        .unwrap_err();
                    assert!(
                        error.to_string().contains("injected point failure"),
                        "{error}"
                    );
                    *sink.lock().unwrap() = Some(attempt.native_pipelined_catalog_version());
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err(), "the failed read rejects the settlement");
    assert_eq!(*captured.lock().unwrap(), Some(None));
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_pipelined_conflict_read_when_another_attempt_publishes_then_stale_writer_rejects_without_item()
 {
    let backend = std::sync::Arc::new(fdb().await);
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { record_protected_change(attempt).await }),
        )
        .await
        .unwrap();
    let version = observe(backend.as_ref()).await;
    // The same namespace: the publisher changes the protected stamp.
    let publisher = std::sync::Arc::clone(&backend);
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(attempt.native_pipeline_catalog_version(version)?);
                    attempt
                        .get_batch(
                            vec![b"pipeline-a".to_vec()],
                            crate::ReadProtection::Conflict,
                        )
                        .await?;
                    assert!(
                        attempt
                            .native_pipelined_catalog_version()
                            .flatten()
                            .is_some()
                    );
                    attempt.native_protect_reads(&[b"protected-stamp".to_vec()], &[])?;
                    publisher
                        .with_attempt(
                            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                                .start(publisher.attempt_deadlines()),
                            |other| {
                                Box::pin(async move {
                                    record_protected_change(other).await?;
                                    other.stage(vec![KvMutation::Put {
                                        key: b"protected-stamp".to_vec(),
                                        value: b"next".to_vec(),
                                    }])
                                })
                            },
                        )
                        .await?;
                    attempt.stage(vec![KvMutation::Put {
                        key: b"stale-item".to_vec(),
                        value: b"must-not-commit".to_vec(),
                    }])
                })
            },
        )
        .await;
    assert_eq!(
        result.unwrap_err().attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert_eq!(
                        attempt
                            .get(b"stale-item", crate::ReadProtection::Snapshot)
                            .await?,
                        None
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[derive(Clone, Copy, Debug)]
enum GateRead {
    Point,
    Write,
    Range,
}

/// One read of the undecodable item through the lowerer under test.
#[cfg(all(feature = "foundationdb", feature = "test-support"))]
async fn read_gate_target<A: crate::KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    read: GateRead,
    table: &storage_types::BoundTableInfo,
    key: &storage_types::KeyAttributes,
) -> StorageResult<()> {
    use storage_provider::{ItemReadAttempt as _, ItemWriteAttempt as _};
    match read {
        GateRead::Point => catalog
            .read_primary_pairs(&[(table, key)])
            .await
            .map(|_| ()),
        GateRead::Write => catalog
            .observe_for_writes(&[storage_provider::ItemWriteObservationRead::Write(
                table, key,
            )])
            .await
            .map(|_| ()),
        GateRead::Range => {
            let plan =
                storage_provider::RangeReadPlan::try_from_scan(storage_types::ScanTableRequest {
                    table_name: table.info.table_name.clone(),
                    index_name: None,
                    limit: None,
                    exclusive_start_key: None,
                    consistent_read: true,
                })
                .unwrap();
            let bound = plan
                .bind(
                    std::sync::Arc::clone(&table.info),
                    storage_provider::RangeReadiness::BaseTable,
                )
                .unwrap();
            let page = bound.page(25).unwrap();
            catalog
                .read_range_page(table, &bound, &page)
                .await
                .map(|_| ())
        }
    }
}

/// An undecodable item value commits with a newer catalog version. A read
/// expecting the older version is dropped undecoded and answers a conflict on
/// an open settlement; a read expecting the current version decodes and fails
/// with a non-conflict error.
#[cfg(all(feature = "foundationdb", feature = "test-support"))]
async fn check_moved_gate(read: GateRead) {
    let backend = fdb().await;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { record_protected_change(attempt).await }),
        )
        .await
        .unwrap();
    let stale = observe(&backend).await;
    let info: storage_types::StoredTableInfo = serde_json::from_value(serde_json::json!({
        "table_name": "pipelined_gate", "table_status": "ACTIVE", "created_at": 0,
        "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}], "max_indexers": 0,
        "global_secondary_indexes": null, "table_size_bytes": 0, "item_count": 0,
        "stream_specification": null
    }))
    .unwrap();
    let table = std::sync::Arc::new(storage_types::BoundTableInfo {
        binding: Default::default(),
        info: std::sync::Arc::new(info),
        history_id: 7_u32.into(),
    });
    let key: storage_types::KeyAttributes =
        serde_json::from_value(serde_json::json!({"pk": {"S": "a"}})).unwrap();
    let table_id = crate::catalog::table_id_from_history(table.history_id).unwrap();
    let physical = crate::item_read::primary_item_key(table_id, &table, &key).unwrap();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    record_protected_change(attempt).await?;
                    attempt.stage(vec![KvMutation::Put {
                        key: physical,
                        value: b"not an item".to_vec(),
                    }])
                })
            },
        )
        .await
        .unwrap();
    let current = observe(&backend).await;
    assert_ne!(current, stale);
    let (mode, class) = match read {
        GateRead::Write => (AttemptMode::ReadWrite, DeadlineClass::Write),
        _ => (AttemptMode::ReadOnly, DeadlineClass::PointRead),
    };
    for (expected, moved) in [(stale, true), (current, false)] {
        let (table, key) = (std::sync::Arc::clone(&table), key.clone());
        let result = backend
            .with_attempt(
                AttemptPlan::new(mode, class).start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let mut catalog = crate::catalog::KvCatalogAttempt::new(
                            attempt,
                            crate::ReadProtection::Snapshot,
                        );
                        assert!(catalog.pipeline_catalog_version(expected)?);
                        let first = read_gate_target(&mut catalog, read, &table, &key).await;
                        if !moved {
                            return first;
                        }
                        // The gate dropped the raw value undecoded and answers
                        // a conflict.
                        let error = first.expect_err("a moved version is not decoded");
                        assert_eq!(
                            error.attempt_kind(),
                            Some(storage_types::AttemptKind::Conflict),
                            "{read:?}: {error:?}"
                        );
                        assert_eq!(catalog.pipelined_catalog_version(), Some(Some(current)));
                        // The settlement stayed open and the gate is off: the
                        // next read decodes and fails with a non-conflict
                        // error.
                        let second = read_gate_target(&mut catalog, read, &table, &key).await;
                        if let Err(error) = &second {
                            assert_ne!(
                                error.attempt_kind(),
                                Some(storage_types::AttemptKind::Conflict),
                                "{read:?} second read: {error:?}"
                            );
                        }
                        second
                    })
                },
            )
            .await;
        if let Err(error) = result {
            // Confirmed version: the decode runs, fails, and the failure is not
            // a conflict.
            assert_ne!(
                error.attempt_kind(),
                Some(storage_types::AttemptKind::Conflict),
                "{read:?} moved={moved}: {error:?}"
            );
        } else {
            assert!(moved, "{read:?}: an undecodable item must fail the decode");
        }
    }
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_moved_pipelined_version_when_item_decode_would_fail_then_attempt_answers_conflict()
 {
    check_moved_gate(GateRead::Point).await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_moved_pipelined_version_when_write_observation_would_fail_then_attempt_answers_conflict()
 {
    check_moved_gate(GateRead::Write).await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_moved_pipelined_version_when_range_decode_would_fail_then_attempt_answers_conflict()
 {
    check_moved_gate(GateRead::Range).await;
}
