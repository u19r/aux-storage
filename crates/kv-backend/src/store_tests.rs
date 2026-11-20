use storage_types::{
    AttemptPlan, DeadlineClass, InfrastructureContext, InfrastructureResultKind, StorageEnum,
};

use super::{
    AttemptMode, KvAttempt, KvBackend, KvMutation, KvStore, ReadProtection,
    constants::{
        INFRASTRUCTURE_FORMAT_KEY, MAINTENANCE_IDENTITY_KEY, SYSTEM_STREAM_SCOPE_KEY,
        TTL_INDEX_FORMAT_KEY,
    },
    rocksdb::RocksDbBackend,
};

#[tokio::test]
async fn given_unsupported_final_format_when_store_initialization_fails_then_no_earlier_format_is_committed()
 {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let backend = RocksDbBackend::initialize(
        path.clone(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![KvMutation::Put {
                        key: INFRASTRUCTURE_FORMAT_KEY.to_vec(),
                        value: vec![u8::MAX],
                    }])
                })
            },
        )
        .await
        .unwrap();

    let error = match KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
    {
        Ok(_) => panic!("unsupported infrastructure format was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InfrastructureResult(InfrastructureResultKind::UnsupportedVersion {
            context: InfrastructureContext::PhysicalLayout,
        })
    ));

    let reopened =
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap();
    let observed = reopened
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(reopened.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .get_batch(
                            vec![
                                INFRASTRUCTURE_FORMAT_KEY.to_vec(),
                                TTL_INDEX_FORMAT_KEY.to_vec(),
                                SYSTEM_STREAM_SCOPE_KEY.to_vec(),
                                MAINTENANCE_IDENTITY_KEY.to_vec(),
                            ],
                            ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(observed, vec![Some(vec![u8::MAX]), None, None, None]);
}

#[tokio::test]
async fn given_four_initial_roots_when_rocksdb_reopens_with_one_then_scope_and_roots_are_preserved()
{
    use stream_provider::{
        StreamOperations as _, SystemStreamLineage, SystemStreamSplitChild,
        SystemStreamSplitOperationId, SystemStreamSplitRequest,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lineages");
    let open = || {
        RocksDbBackend::initialize(
            path.clone(),
            storage_types::AttemptDeadlines::default(),
            256,
        )
        .unwrap()
    };
    let store = KvStore::initialize_format(open(), storage_types::InitialStreamLineages::FOUR)
        .await
        .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    let roots = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap();
    assert_eq!(roots.len(), 4);
    let parent = roots.into_iter().next().unwrap();
    let children = ["left", "right"].map(|side| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                format!("split-{side}"),
                format!("split-{side}-lineage"),
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
                SystemStreamSplitOperationId::try_new("initial-config-reopen").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let before = store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            |attempt| Box::pin(crate::stream_history::capture_high(attempt)),
        )
        .await
        .unwrap();
    let before_catalog = store
        .system_stream_lineage_catalog(before.scope().clone())
        .await
        .unwrap();
    assert_eq!(before_catalog.len(), 6);
    assert_eq!(
        before_catalog
            .iter()
            .filter(|lineage| lineage.sealed())
            .count(),
        1
    );
    assert_eq!(before.frontier().len(), 5);
    drop(store);
    let store = KvStore::initialize_format(open(), storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let after = store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            |attempt| Box::pin(crate::stream_history::capture_high(attempt)),
        )
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        store
            .system_stream_lineage_catalog(after.scope().clone())
            .await
            .unwrap(),
        before_catalog
    );
}

#[tokio::test]
async fn given_native_child_start_beyond_snapshot_when_split_then_store_is_unchanged_and_reopens() {
    use storage_provider::DynamoOperations as _;
    use stream_provider::{
        StreamOperations as _, SystemStreamLineage, SystemStreamSplitChild,
        SystemStreamSplitOperationId, SystemStreamSplitRequest,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("rejected-split");
    let open = || {
        RocksDbBackend::initialize(
            path.clone(),
            storage_types::AttemptDeadlines::default(),
            256,
        )
        .unwrap()
    };
    let store = KvStore::initialize_format(open(), storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "rejected_split", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    store
        .put_item(
            storage_types::PutItemRequest::try_from(serde_json::json!({
                "TableName": "rejected_split", "Item": {"pk": {"S": "preserved"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    let catalog = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap();
    let parent = catalog[0].clone();
    let before = store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(store.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .native_scan(
                            crate::KvRange {
                                begin: vec![],
                                end: vec![255],
                                reverse: false,
                                limit: std::num::NonZeroU32::new(1000).unwrap(),
                                max_bytes: std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
                                entry_bounds: None,
                            },
                            ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert!(!before.more);
    let children = [("left", u64::MAX - 1), ("right", 1)].map(|(side, initial)| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                side,
                format!("{side}-lineage"),
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            initial,
        )
        .unwrap()
    });
    let request = SystemStreamSplitRequest::try_new(
        scope.clone(),
        parent,
        children,
        SystemStreamSplitOperationId::try_new("rejected-native-start").unwrap(),
    )
    .unwrap();
    assert!(
        store.split_system_stream(request).await.is_err(),
        "future native child start must be rejected"
    );
    let after = store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(store.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .native_scan(
                            crate::KvRange {
                                begin: vec![],
                                end: vec![255],
                                reverse: false,
                                limit: std::num::NonZeroU32::new(1000).unwrap(),
                                max_bytes: std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
                                entry_bounds: None,
                            },
                            ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert!(!after.more);
    assert_eq!(
        before
            .entries
            .iter()
            .map(|entry| (&entry.key, &entry.value))
            .collect::<Vec<_>>(),
        after
            .entries
            .iter()
            .map(|entry| (&entry.key, &entry.value))
            .collect::<Vec<_>>(),
        "catalog, items, history, and receipts must not change"
    );
    assert_eq!(
        store
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap(),
        catalog
    );
    drop(store);
    let reopened = KvStore::initialize_format(open(), storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    assert_eq!(
        reopened.system_stream_lineage_catalog(scope).await.unwrap(),
        catalog
    );
}
