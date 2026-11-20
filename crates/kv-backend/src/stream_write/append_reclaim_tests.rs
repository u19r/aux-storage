use storage_provider::{
    DynamoOperations, ReclaimAttempt, TableReclaimPhase, TableReclaimStep, TableReclaimTarget,
};
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, TableHistoryTarget};
use stream_provider::{
    StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt, StreamAttempt,
    TableAppendRequest,
};

use crate::KvAttempt as _;

#[tokio::test]
async fn given_deleted_table_when_id_history_is_reclaimed_then_only_deleted_incarnation_entries_and_feed_references_are_removed()
 {
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    let history =
        storage_provider::table_reclaim_test_support::qualify_table_history_reclaim(&store).await;
    storage_provider::DynamoPort::with_attempt(
        &store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
        ),
        move |attempt| {
            Box::pin(async move {
                let id = crate::catalog::table_id_from_history(history)?;
                assert!(
                    attempt
                        .attempt
                        .get(
                            &crate::ttl_policy::ttl_config_key(id),
                            storage_types::ReadProtection::Conflict
                        )
                        .await?
                        .is_none()
                );
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn given_scoped_append_receipts_when_one_table_is_reclaimed_then_the_other_receipt_survives()
{
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    let mut targets = Vec::new();
    for name in ["deleted", "peer"] {
        let request = serde_json::from_value(serde_json::json!({
            "TableName": name, "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }))
        .unwrap();
        let created = store.create_table(request).await.unwrap();
        targets.push(TableHistoryTarget::try_from(&created.table_description).unwrap());
    }
    let operation = StreamAppendOperationId::fresh();
    storage_provider::DynamoPort::with_attempt(
        &store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
        ),
        move |attempt| {
            Box::pin(async move {
                let mut requests = targets
                    .iter()
                    .map(|target| TableAppendRequest::new(operation, target.clone(), b"payload"))
                    .collect::<Vec<_>>();
                let peer = requests.pop().unwrap();
                let deleted = requests.pop().unwrap();
                for request in [&deleted, &peer] {
                    let receipt = StreamAppendReceipt::new(
                        request,
                        stream_provider::StreamAppendProof::try_new(vec![1]).unwrap(),
                        StreamAppendAllocation::AttemptAllocated(
                            storage_types::StreamItemId::from([1; 12]),
                        ),
                    );
                    attempt.store_append_receipt(&receipt).await?;
                    assert_eq!(
                        attempt.load_append_receipt(request.into()).await?,
                        Some(receipt)
                    );
                }
                let target = TableReclaimTarget {
                    history_id: targets[0].history_id,
                    table_name: targets[0].table_name.clone(),
                    phase: TableReclaimPhase::ExplicitAppends,
                    gsi_targets: vec![],
                };
                assert_eq!(
                    attempt.delete_reclaim_page(&target).await?,
                    TableReclaimStep::Complete
                );
                assert!(
                    attempt
                        .load_append_receipt((&deleted).into())
                        .await?
                        .is_none()
                );
                assert!(attempt.load_append_receipt((&peer).into()).await?.is_some());
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn given_external_cleanup_requirement_when_name_is_reused_then_native_finalization_waits() {
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    storage_provider::table_reclaim_test_support::qualify_external_cleanup_fence(&store).await;
}

#[tokio::test]
async fn given_unmatched_stamp_when_storing_barrier_then_conflict_on_every_cause() {
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    storage_provider::table_reclaim_test_support::qualify_external_cleanup_barrier_misses(&store)
        .await;
}

#[tokio::test]
async fn given_reclaimed_incarnation_when_system_feed_is_read_then_survivor_and_later_writes_remain_visible()
 {
    use stream_provider::{StreamOperations, SystemStreamPageRequest};
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    for name in ["gap-deleted", "gap-survivor"] {
        store
            .create_table(
                serde_json::from_value(serde_json::json!({
                    "TableName": name, "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    for (name, key) in [
        ("gap-deleted", "first"),
        ("gap-survivor", "live"),
        ("gap-deleted", "tail"),
    ] {
        store
            .put_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": name, "Item": {"pk": {"S": key}, "value": {"S": key}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let scope = store.system_stream_scope().await.unwrap();
    store
        .delete_table(storage_types::DeleteTableRequest {
            table_name: storage_types::TableName::new("gap-deleted"),
        })
        .await
        .unwrap();
    let mut complete = false;
    for _ in 0..32 {
        if storage_provider::MaintenanceOperations::reclaim_table_page(&store)
            .await
            .unwrap()
            == TableReclaimStep::Complete
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    let page = store
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap())
        .await
        .unwrap();
    assert_eq!(page.records().len(), 1);
    assert_eq!(
        page.records()[0]
            .source_table_name
            .as_ref()
            .unwrap()
            .as_ref(),
        "gap-survivor"
    );
    let token = page.issued_checkpoint().token().clone();
    let tail = store
        .get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(token), 1).unwrap(),
        )
        .await
        .unwrap();
    assert!(tail.records().is_empty());
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": "gap-survivor", "Item": {"pk": {"S": "later"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let later = store
        .get_system_stream_page(
            SystemStreamPageRequest::try_new(
                scope,
                Some(tail.issued_checkpoint().token().clone()),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(later.records().len(), 1);
}

#[tokio::test]
async fn given_wide_keys_only_entries_when_system_page_is_read_then_complete_images_resume_exclusively()
 {
    use stream_provider::{StreamOperations, SystemStreamPageRequest};
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    store
        .create_table(
            serde_json::from_value(serde_json::json!({
                "TableName": "wide-feed", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut random = 0x193ab7cdu64;
    let mut expected = Vec::new();
    for index in 0..3 {
        let payload: String = (0..390_000)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                char::from(b'!' + (random % 90) as u8)
            })
            .collect();
        let item =
            serde_json::json!({"pk": {"S": format!("key-{index}")}, "payload": {"S": payload}});
        store
            .put_item(
                serde_json::from_value(serde_json::json!({"TableName": "wide-feed", "Item": item}))
                    .unwrap(),
            )
            .await
            .unwrap();
        expected.push(serde_json::from_value::<storage_types::AttributeMap>(item).unwrap());
    }
    let scope = store.system_stream_scope().await.unwrap();
    let mut token = None;
    let mut seen = 0;
    let mut terminal = false;
    let mut page_sizes = Vec::new();
    for _ in 0..8 {
        let page = store
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), token, 10).unwrap(),
            )
            .await
            .unwrap();
        page_sizes.push(page.records().len());
        for record in page.records() {
            assert!(seen < expected.len(), "exclusive cursor repeated a record");
            assert_eq!(record.item.as_ref(), Some(&expected[seen]));
            assert_eq!(record.keys.len(), 1);
            assert_eq!(record.keys.get("pk"), expected[seen].get("pk"));
            seen += 1;
        }
        token = Some(page.issued_checkpoint().token().clone());
        if page.terminal() {
            terminal = true;
            break;
        }
    }
    assert_eq!(seen, 3);
    assert!(terminal, "bounded feed must reach its committed frontier");
    assert!(
        page_sizes.len() > 1,
        "wide images require byte-bounded pagination"
    );
    let empty = store
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope, token, 10).unwrap())
        .await
        .unwrap();
    assert!(empty.records().is_empty());
    assert!(empty.terminal());
}

#[tokio::test]
async fn given_forever_table_when_system_trim_runs_then_table_entries_survive_and_feed_resumes() {
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    storage_provider::table_reclaim_test_support::qualify_system_trim(&store).await;
}
