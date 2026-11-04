use std::sync::Arc;

use replication_contract_tests::native::commit_fault::{NativeCommitBoundary, NativeCommitFault};
use replication_types::event::content::CanonicalEventCodec;
use storage_provider::{
    CatalogAttempt, DynamoOperations, DynamoPort, PutItemShape, TableReclaimAttempt,
    TableReclaimRepository, TableReclaimStep,
};
use storage_types::{AttemptMode, DeleteTableRequest, PutItemRequest, TableName};

use super::{JournalStore, table_reclaim_test_support::reclaim_deleted_table};
use crate::{
    origin::{
        OperationReceiptState, OriginExecution, OriginReceiptObservation, reconcile_item_write,
    },
    test_support as fixture,
};

#[tokio::test]
async fn given_an_old_image_response_loses_ack_when_cleanup_reopens_midway_then_native_and_archived_data_disappear_without_retargeting_the_replacement()
 {
    let directory = tempfile::tempdir().unwrap();
    let fault = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let path = directory.path().join("source.sqlite");
    let root = directory.path().join("archive");
    std::fs::create_dir(&root).unwrap();
    let source = fixture::open_with_probe(&path, Some(fault.clone())).await;
    let peer_path = directory.path().join("peer.sqlite");
    let peer = fixture::open(&peer_path).await;
    let cluster = fixture::NativeCluster::start_with_archive(&[&source, &peer], &root).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("old-response-body"),
        ))
        .await
        .unwrap();
    let physical = fixture::installed_state(&source, 1, fixture::NAME)
        .await
        .physical;
    let journal = Arc::clone(&cluster.journals[0]);
    let archive = Arc::clone(journal.archive.get().unwrap());
    let body = b"separately-owned-archived-response-body";
    let digest = replication_types::protocol::CanonicalDigest::of(body);
    archive
        .publish_object(physical, digest, body)
        .await
        .unwrap();
    let mut request = PutItemRequest::new(
        TableName::new(fixture::NAME),
        fixture::item("new-response-body"),
    );
    request.return_values = Some(storage_types::AllOld::AllOld);
    fault.arm();
    let failure = crate::origin::execute_item_write(
        &source,
        OriginExecution {
            journal: Arc::clone(&journal),
            codec: fixture::codec(),
        },
        PutItemShape::try_from_request(request).unwrap(),
    )
    .await
    .err()
    .unwrap();
    assert!(failure.error.is_commit_unknown());
    let complete = recover_receipt(&source, &journal, failure.recovery).await;
    assert_eq!(complete.owners.len(), 1);
    assert_eq!(complete.owners[0].physical, physical);
    assert!(complete.response.get().contains("old-response-body"));
    let reclaimer = Arc::clone(&cluster.operations[0]);
    cluster.shutdown().await;
    source
        .delete_table(DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    fixture::create_table(&source, &TableName::new(fixture::NAME)).await;
    source
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("replacement"),
        ))
        .await
        .unwrap();
    let replacement = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(attempt
                    .read_catalog_stamp(&TableName::new(fixture::NAME))
                    .await?
                    .unwrap()
                    .history_id)
            })
        })
        .await
        .unwrap();
    assert_ne!(replacement, physical);
    archive
        .publish_object(replacement, digest, body)
        .await
        .unwrap();
    for _ in 0..4 {
        assert_eq!(
            reclaimer.reclaim_table_page().await.unwrap(),
            TableReclaimStep::Advanced
        );
    }
    source
        .with_dynamo_schema_attempt(move |attempt| {
            Box::pin(async move {
                let target = attempt.load_reclaim_target().await?.unwrap();
                assert_eq!(target.history_id, physical);
                assert_eq!(
                    target.phase,
                    storage_provider::TableReclaimPhase::ExternalData
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(reclaimer);
    drop(source);
    drop(peer);
    let source = fixture::open(&path).await;
    let peer = fixture::open(&peer_path).await;
    let restarted = fixture::NativeCluster::start_with_archive(&[&source, &peer], &root).await;
    let reclaimer = Arc::clone(&restarted.operations[0]);
    restarted.shutdown().await;
    reclaim_deleted_table(&source, reclaimer.as_ref(), physical).await;
    assert!(
        archive
            .fetch_object(physical, digest, body.len())
            .await
            .is_err()
    );
    assert!(
        archive
            .publish_object(physical, digest, body)
            .await
            .is_err()
    );
    assert_eq!(
        archive
            .fetch_object(replacement, digest, body.len())
            .await
            .unwrap()
            .as_ref(),
        body
    );
    assert!(matches!(
        reconcile_item_write(&source, journal, failure.recovery)
            .await
            .unwrap(),
        OriginReceiptObservation::Unresolved
    ));
    assert!(failure.error.is_commit_unknown());
    fixture::assert_item(&source, Some("replacement")).await;
}

#[tokio::test]
async fn given_a_mixed_unprocessed_response_loses_ack_when_one_owner_is_deleted_then_only_its_data_is_removed_and_partial_recovery_is_rejected()
 {
    let directory = tempfile::tempdir().unwrap();
    let fault = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let source =
        fixture::open_with_probe(&directory.path().join("source.sqlite"), Some(fault.clone()))
            .await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&source, &peer]).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.install(&source, "other-response", &[2]).await;
    let first = fixture::installed_state(&source, 1, fixture::NAME).await;
    let second = fixture::installed_state(&source, 1, "other-response").await;
    let partition = fixture::partitions().select(
        first.configuration.lineage,
        &fixture::codec().encode_key(&fixture::key()).unwrap(),
    );
    let items = [&first, &second].into_iter().map(|state| {
        let keys = matching_keys(state.configuration.lineage, partition);
        let value = if state.physical == first.physical { "erase-a-unprocessed" } else { "keep-b-unprocessed" };
        (state.name.clone(), keys.into_iter().map(|key| serde_json::json!({"PutRequest": {"Item": {"pk": {"S": key}, "value": {"S": value}}}})).collect::<Vec<_>>())
    }).collect::<storage_types::VecMap<_, _>>();
    let shape = storage_provider::DynamoBatchWriteShape::try_from_request(
        serde_json::from_value(serde_json::json!({"RequestItems": items})).unwrap(),
    )
    .unwrap();
    let journal = fixture::journal_with_maximum_events(1, fixture::nz(1));
    fault.arm();
    let failure = crate::origin::execute_batch_write(
        &source,
        OriginExecution {
            journal: Arc::clone(&journal),
            codec: fixture::codec(),
        },
        shape,
    )
    .await
    .err()
    .unwrap();
    assert!(failure.error.is_commit_unknown());
    let before = recover_receipt(&source, &journal, failure.recovery).await;
    assert_eq!(before.owners.len(), 2);
    let before: storage_types::BatchWriteItemResponse =
        serde_json::from_str(before.response.get()).unwrap();
    let retained =
        serde_json::to_value(before.unprocessed_items.unwrap().get(&second.name).unwrap()).unwrap();
    let reclaimer = Arc::clone(&cluster.operations[0]);
    cluster.shutdown().await;
    source
        .delete_table(DeleteTableRequest {
            table_name: first.name.clone(),
        })
        .await
        .unwrap();
    reclaim_deleted_table(&source, reclaimer.as_ref(), first.physical).await;
    let second_physical = second.physical;
    let second_name = second.name.clone();
    let owner = Arc::clone(&journal);
    let identity = failure.recovery.identity();
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let key = owner.identity_key(crate::constants::OPERATION_SUFFIX, &identity)?;
                let rows = owner
                    .records
                    .load(attempt, &[key], storage_types::ReadProtection::Snapshot)
                    .await?;
                let receipt: crate::origin::OriginOperationReceipt =
                    owner.records.decode(rows[0].as_ref().unwrap())?;
                assert!(receipt.state == OperationReceiptState::Incomplete);
                assert_eq!(receipt.owners.len(), 1);
                assert_eq!(receipt.owners[0].physical, second.physical);
                assert!(!receipt.response.get().contains("erase-a-unprocessed"));
                let response: storage_types::BatchWriteItemResponse =
                    serde_json::from_str(receipt.response.get()).unwrap();
                assert_eq!(
                    serde_json::to_value(
                        response
                            .unprocessed_items
                            .unwrap()
                            .get(&second.name)
                            .unwrap()
                    )
                    .unwrap(),
                    retained
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let error = match reconcile_item_write(&source, Arc::clone(&journal), failure.recovery).await {
        Err(error) => error,
        Ok(_) => panic!("partial response became a complete recovery"),
    };
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::HistoryUnavailable
    ));
    assert!(failure.error.is_commit_unknown());
    source
        .delete_table(DeleteTableRequest {
            table_name: second_name,
        })
        .await
        .unwrap();
    reclaim_deleted_table(&source, reclaimer.as_ref(), second_physical).await;
    assert!(matches!(
        reconcile_item_write(&source, journal, failure.recovery)
            .await
            .unwrap(),
        OriginReceiptObservation::Unresolved
    ));
}

async fn recover_receipt(
    source: &fixture::Store,
    journal: &Arc<JournalStore>,
    recovery: crate::origin::RecoveryOperation,
) -> crate::origin::OriginOperationReceipt {
    match reconcile_item_write(source, Arc::clone(journal), recovery)
        .await
        .unwrap()
    {
        OriginReceiptObservation::Durable(receipt) => receipt,
        OriginReceiptObservation::Unresolved => panic!("native committed response is missing"),
    }
}

fn matching_keys(
    lineage: replication_types::identity::resource::TableLineage,
    partition: replication_types::identity::journal_partition::JournalPartition,
) -> Vec<String> {
    (0..4096)
        .map(|index| format!("customer-{index}"))
        .filter(|value| {
            let key = storage_types::KeyAttributes::from_iter([(
                "pk".to_owned(),
                storage_types::AttributeValue::S(value.clone()),
            )]);
            fixture::partitions().select(lineage, &fixture::codec().encode_key(&key).unwrap())
                == partition
        })
        .take(2)
        .collect()
}
