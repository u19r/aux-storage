use storage_provider::{
    DynamoOperations, ReclaimAttempt, TableReclaimPhase, TableReclaimStep, TableReclaimTarget,
};
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, TableHistoryTarget};
use stream_provider::{
    StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt, StreamAttempt,
    TableAppendRequest,
};

#[tokio::test]
async fn given_deleted_table_when_id_history_is_reclaimed_then_only_deleted_incarnation_entries_and_feed_references_are_removed()
 {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap(),
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    storage_provider::table_reclaim_test_support::qualify_table_history_reclaim(&store).await;
}

#[tokio::test]
async fn given_scoped_append_receipts_when_one_table_is_reclaimed_then_the_other_receipt_survives()
{
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap(),
        storage_types::InitialStreamLineages::ONE,
        true,
    )
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
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap(),
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    storage_provider::table_reclaim_test_support::qualify_external_cleanup_fence(&store).await;
}

#[tokio::test]
async fn given_unmatched_stamp_when_storing_barrier_then_conflict_on_every_cause() {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap(),
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    storage_provider::table_reclaim_test_support::qualify_external_cleanup_barrier_misses(&store)
        .await;
}

#[tokio::test]
async fn given_forever_table_when_system_trim_runs_then_table_entries_survive_and_feed_resumes() {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap(),
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    storage_provider::table_reclaim_test_support::qualify_system_trim(&store).await;
}
