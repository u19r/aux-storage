use storage_provider::{
    DynamoOperations, DynamoPort, TableReclaimAttempt, TableReclaimPhase, TableReclaimStep,
    TableReclaimTarget,
};
use storage_types::{AttemptMode, TableHistoryTarget};
use stream_provider::{
    StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt, StreamAttempt,
    TableAppendRequest,
};

#[tokio::test]
async fn given_deleted_table_when_id_history_is_reclaimed_then_all_bodies_are_removed_and_source_positions_remain()
 {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    storage_provider::table_reclaim_test_support::qualify_table_history_reclaim(&store).await;
}

#[tokio::test]
async fn given_scoped_append_receipts_when_one_table_is_reclaimed_then_the_other_receipt_survives()
{
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap(),
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
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
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
                        vec![1],
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
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn given_external_cleanup_requirement_when_name_is_reused_then_native_finalization_waits() {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    storage_provider::table_reclaim_test_support::qualify_external_cleanup_fence(&store).await;
}
