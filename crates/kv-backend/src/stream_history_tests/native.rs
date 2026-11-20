use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use storage_types::{AttemptPlan, DeadlineClass, StorageError};
use stream_provider::{
    StreamOperations as _, SystemStreamLineage, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use crate::{
    AttemptMode, KvAttempt as _, KvBackend as _, KvMutation, KvStore, ReadProtection,
    constants::SYSTEM_STREAM_LINEAGE_PREFIX as LINEAGE_PREFIX,
    foundationdb::{FoundationDbBackend, FoundationDbOptions},
    stream_history::*,
};

#[tokio::test]
#[ignore = "requires disposable FoundationDB: just test-fdb-owned with --run-ignored all"]
async fn given_native_future_child_start_when_split_then_catalog_and_receipt_remain_unchanged() {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run with just test-fdb-owned");
    let prefix = uuid::Uuid::new_v4().to_string();
    let open = || async {
        FoundationDbBackend::initialize(FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            ..Default::default()
        })
        .await
        .unwrap()
    };
    let store = KvStore::initialize_format(open().await, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    let catalog = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap();
    let parent = catalog[0].clone();
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
    let operation = SystemStreamSplitOperationId::try_new("future-native-start").unwrap();
    let request =
        SystemStreamSplitRequest::try_new(scope.clone(), parent, children, operation.clone())
            .unwrap();
    assert!(store.split_system_stream(request).await.is_err());
    assert_eq!(
        store
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap(),
        catalog
    );
    store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(
                        crate::stream_split::read_split_state(attempt, &operation)
                            .await?
                            .1
                            .is_none()
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    drop(store);
    let reopened =
        KvStore::initialize_format(open().await, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    assert_eq!(
        reopened.system_stream_lineage_catalog(scope).await.unwrap(),
        catalog
    );
}

#[derive(Default)]
struct SuppressSplitReply(AtomicBool);

impl crate::KvFaultProbe for SuppressSplitReply {
    fn fail(&self, _: crate::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if boundary == crate::KvCommitBoundary::AfterDurableAcknowledgement
                && self.0.swap(false, Ordering::SeqCst)
            {
                return Err(StorageError::internal(
                    "suppress durable split acknowledgement",
                ));
            }
            Ok(())
        })
    }
}

#[tokio::test]
#[ignore = "requires disposable FoundationDB: just test-fdb-owned with --run-ignored all"]
async fn given_native_split_when_acknowledgement_is_lost_then_reopen_replays_stamped_parent_without_counters()
 {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run with just test-fdb-owned");
    let prefix = uuid::Uuid::new_v4().to_string();
    let probe = Arc::new(SuppressSplitReply::default());
    let open = || async {
        FoundationDbBackend::initialize(FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            ..Default::default()
        })
        .await
        .unwrap()
        .with_fault_probe(probe.clone())
    };
    let backend = open().await;
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let backend = &store.backend;
    let scope = store.system_stream_scope().await.unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    use storage_provider::DynamoOperations as _;
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "native_split", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    store
        .put_item(
            storage_types::PutItemRequest::try_from(serde_json::json!({
                "TableName": "native_split", "Item": {"pk": {"S": "before-split"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let version = attempt.native_read_version().await?.unwrap();
                    let high = crate::stream_history::capture_high(attempt).await?;
                    assert_eq!(
                        high.frontier()[0].offset().as_str(),
                        format!("{version:020}-vsffffffff")
                    );
                    assert!(
                        attempt
                            .scan(
                                crate::stream_history::prefix_range(b"\x7fs/q/")?,
                                ReadProtection::Snapshot
                            )
                            .await?
                            .entries
                            .is_empty()
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let children = ["left", "right"].map(|side| {
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
    let request = SystemStreamSplitRequest::try_new(
        scope.clone(),
        parent.clone(),
        children,
        SystemStreamSplitOperationId::try_new("unknown-native-split").unwrap(),
    )
    .unwrap();
    probe.0.store(true, Ordering::SeqCst);
    let error = store
        .split_system_stream(request.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error.as_ref(), stream_provider::StreamEnum::StorageError(error) if error.is_commit_unknown())
    );
    let parent_key = segmented_key(LINEAGE_PREFIX, parent.lineage_id().as_str()).unwrap();
    let before = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let bytes = attempt
                        .get(&parent_key, ReadProtection::Snapshot)
                        .await?
                        .unwrap();
                    assert_eq!(bytes[0], NATIVE_SEALED_LINEAGE);
                    let version = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
                    assert_eq!(
                        decode_lineage_row(&bytes)?.sealed_after_offset,
                        Some(version + 1)
                    );
                    assert!(
                        attempt
                            .scan(
                                crate::stream_history::prefix_range(b"\x7fs/q/")?,
                                ReadProtection::Snapshot
                            )
                            .await?
                            .entries
                            .is_empty()
                    );
                    Ok(bytes)
                })
            },
        )
        .await
        .unwrap();
    drop(store);
    let backend = open().await;
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let backend = &store.backend;
    let replay = store.split_system_stream(request.clone()).await.unwrap();
    assert_eq!(
        replay.disposition(),
        stream_provider::SystemStreamSplitDisposition::Replayed
    );
    assert_eq!(replay, store.split_system_stream(request).await.unwrap());
    let child_parent = replay.children()[0].lineage().clone();
    let grandchildren = ["left", "right"].map(|side| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                format!("grandchild-{side}"),
                format!("grandchild-{side}-lineage"),
                Some(child_parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    let child_request = SystemStreamSplitRequest::try_new(
        scope.clone(),
        child_parent,
        grandchildren,
        SystemStreamSplitOperationId::try_new("stale-writer-child-split").unwrap(),
    )
    .unwrap();
    let splitting_store = store.clone();
    let error = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    // Hold the real writer's routing snapshot while the split
                    // commits.
                    crate::stream_history::read_writer_routes(attempt).await?;
                    splitting_store
                        .split_system_stream(child_request)
                        .await
                        .unwrap();
                    attempt.stage(vec![KvMutation::Put {
                        key: b"stale-writer".to_vec(),
                        value: vec![1],
                    }])
                })
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert_eq!(
        store
            .system_stream_lineage_catalog(scope)
            .await
            .unwrap()
            .len(),
        5
    );
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert_eq!(
                        attempt
                            .get(
                                &segmented_key(LINEAGE_PREFIX, parent.lineage_id().as_str())?,
                                ReadProtection::Snapshot
                            )
                            .await?
                            .unwrap(),
                        before
                    );
                    assert!(
                        attempt
                            .scan(
                                crate::stream_history::prefix_range(b"\x7fs/q/")?,
                                ReadProtection::Snapshot
                            )
                            .await?
                            .entries
                            .is_empty()
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "requires disposable FoundationDB: just test-fdb-owned with --run-ignored all"]
async fn given_empty_native_parent_when_split_and_reopened_then_empty_reads_and_replay_complete() {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run with just test-fdb-owned");
    let prefix = uuid::Uuid::new_v4().to_string();
    let open = || async {
        FoundationDbBackend::initialize(FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            ..Default::default()
        })
        .await
        .unwrap()
    };
    let store = KvStore::initialize_format(open().await, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    let children = ["empty-left", "empty-right"].map(|side| {
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
    let request = SystemStreamSplitRequest::try_new(
        scope.clone(),
        parent,
        children,
        SystemStreamSplitOperationId::try_new("empty-native-split").unwrap(),
    )
    .unwrap();
    let applied = store.split_system_stream(request.clone()).await.unwrap();
    assert_eq!(
        applied.disposition(),
        stream_provider::SystemStreamSplitDisposition::Applied
    );
    drop(store);
    let store = KvStore::initialize_format(open().await, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let replay = store.split_system_stream(request).await.unwrap();
    assert_eq!(
        replay.disposition(),
        stream_provider::SystemStreamSplitDisposition::Replayed
    );
    assert_eq!(applied.sealed_parent(), replay.sealed_parent());
    assert_eq!(applied.children(), replay.children());
    let mut token = None;
    let mut terminal = false;
    for _ in 0..4 {
        let page = store
            .get_system_stream_page(
                stream_provider::SystemStreamPageRequest::try_new(scope.clone(), token, 1).unwrap(),
            )
            .await
            .unwrap();
        assert!(page.records().is_empty());
        if page.terminal() {
            terminal = true;
            break;
        }
        token = Some(page.issued_checkpoint().token().clone());
    }
    assert!(
        terminal,
        "empty sealed parent and children must reach the captured high"
    );
    store.backend.delete_namespace_keys().await.unwrap();
    assert!(!store.backend.namespace_has_keys().await.unwrap());
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "requires disposable FoundationDB: just test-fdb-owned with --run-ignored all"]
async fn given_native_commit_with_extreme_ordinals_when_parent_splits_then_pages_finish_parent_before_children()
 {
    use storage_provider::DynamoOperations as _;
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run with just test-fdb-owned");
    let prefix = uuid::Uuid::new_v4().to_string();
    let backend = FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: Some(&cluster),
        subspace_prefix: Some(prefix.as_bytes()),
        ..Default::default()
    })
    .await
    .unwrap();
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": "ordinal_pages", "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}, "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    store
        .put_item(
            storage_types::PutItemRequest::try_from(serde_json::json!({
                "TableName": "ordinal_pages", "Item": {"pk": {"S": "seed"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let seed_page = store
        .get_system_stream_page(
            stream_provider::SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(seed_page.records().len(), 1);
    let seed_position = seed_page.records()[0].system_position.clone().unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    let ((), committed_stamp) = store
        .backend
        .with_versionstamped_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    // Use a real accepted entry and its table incarnation; only
                    // the event ordinals differ.
                    crate::stream_history::read_writer_routes(attempt).await?;
                    let reference = attempt
                        .get(
                            &crate::stream_write::key_layout::record_key(&seed_position)?,
                            ReadProtection::Snapshot,
                        )
                        .await?
                        .unwrap();
                    let history =
                        crate::stream_write::record::FeedReference::decode(&reference)?.history_id;
                    let bytes = attempt
                        .get(
                            &crate::stream_write::key_layout::entry_key(history, &seed_position)?,
                            ReadProtection::Snapshot,
                        )
                        .await?
                        .unwrap();
                    let template = stream_provider::PositionedStreamEntry::decode_value(
                        history,
                        seed_position,
                        &bytes,
                    )?;
                    for ordinal in [0u16, u16::MAX] {
                        let mut stamp = [255; 12];
                        stamp[10..].copy_from_slice(&ordinal.to_be_bytes());
                        let mut entry = template.clone();
                        entry.position = entry.position.with_versionstamp(stamp).unwrap();
                        crate::stream_write::stage_commit_ordered_dynamo(
                            attempt,
                            (entry.history_id, &entry.position),
                            entry.entry.created_at(),
                            entry.encode_value()?,
                        )?;
                    }
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let committed_stamp = committed_stamp.unwrap();
    let children = ["ordinal-left", "ordinal-right"].map(|side| {
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
                scope.clone(),
                parent.clone(),
                children,
                SystemStreamSplitOperationId::try_new("ordinal-native-split").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .put_item(
            storage_types::PutItemRequest::try_from(serde_json::json!({
                "TableName": "ordinal_pages", "Item": {"pk": {"S": "child"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut token = Some(seed_page.issued_checkpoint().token().clone());
    let mut positions = Vec::new();
    let mut terminal = false;
    for _ in 0..8 {
        let page = store
            .get_system_stream_page(
                stream_provider::SystemStreamPageRequest::try_new(scope.clone(), token, 1).unwrap(),
            )
            .await
            .unwrap();
        assert!(page.records().len() <= 1);
        positions.extend(
            page.records()
                .iter()
                .map(|record| record.system_position.clone().unwrap()),
        );
        if page.terminal() {
            terminal = true;
            break;
        }
        token = Some(page.issued_checkpoint().token().clone());
    }
    assert!(terminal);
    assert_eq!(positions.len(), 3);
    for (position, ordinal) in positions[..2].iter().zip([0u16, u16::MAX]) {
        assert_eq!(position.lineage_id(), parent.lineage_id());
        let stamp = position.offset().versionstamp().unwrap();
        assert_eq!(&stamp[..10], &committed_stamp);
        assert_eq!(&stamp[10..], &ordinal.to_be_bytes());
    }
    assert_ne!(positions[2].lineage_id(), parent.lineage_id());
    store.backend.delete_namespace_keys().await.unwrap();
    assert!(!store.backend.namespace_has_keys().await.unwrap());
}

#[tokio::test]
#[ignore = "requires disposable FoundationDB: just test-fdb-owned with --run-ignored all"]
async fn given_native_stamp_slots_when_inline_and_chunked_values_commit_then_hydration_preserves_surrounding_bytes()
 {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run with just test-fdb-owned");
    let prefix = uuid::Uuid::new_v4().to_string();
    let backend = FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: Some(&cluster),
        subspace_prefix: Some(prefix.as_bytes()),
        ..Default::default()
    })
    .await
    .unwrap();
    let chunk = crate::constants::KV_CHUNK_BYTES;
    let values = [
        (b"inline".to_vec(), vec![23; 48], 1u32),
        (b"chunked".to_vec(), vec![37; chunk * 3], (chunk + 1) as u32),
    ];
    for (key, mut value, offset) in values {
        let start = offset as usize;
        value[start..start + 10].copy_from_slice(&crate::constants::KV_VERSIONSTAMP_PLACEHOLDER);
        let expected = value.clone();
        let write_key = key.clone();
        backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(
                        async move { attempt.stage_versionstamped_value(write_key, value, offset) },
                    )
                },
            )
            .await
            .unwrap();
        backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let actual = attempt
                            .get_admitted(
                                &key,
                                ReadProtection::Snapshot,
                                crate::KvValueAllowance::new(expected.len(), expected.len()),
                            )
                            .await?
                            .unwrap();
                        assert_eq!(actual.len(), expected.len());
                        assert_eq!(&actual[..start], &expected[..start]);
                        assert_eq!(&actual[start + 10..], &expected[start + 10..]);
                        assert_ne!(
                            &actual[start..start + 10],
                            &crate::constants::KV_VERSIONSTAMP_PLACEHOLDER
                        );
                        let version =
                            u64::from_be_bytes(actual[start..start + 8].try_into().unwrap());
                        assert!(
                            version > 0 && version <= attempt.native_read_version().await?.unwrap()
                        );
                        Ok(())
                    })
                },
            )
            .await
            .unwrap();
    }
}
