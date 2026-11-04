use std::sync::Arc;

use storage_provider::{DynamoOperations, DynamoPort, PutItemShape};
use storage_types::{AttemptMode, DeleteTableRequest, PutItemRequest, StorageEnum, TableName};

use super::{
    OriginExecution,
    executor::{ItemOperation, OriginAttemptContext, OriginOperation},
};
use crate::test_support as fixture;

#[tokio::test]
async fn given_a_regional_write_aborts_when_its_name_is_recreated_then_retry_cannot_write_the_new_table()
 {
    let operation = ItemOperation(
        PutItemShape::try_from_request(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("must-stay-with-first-id"),
        ))
        .unwrap(),
    );
    assert_retry_is_bound(operation, &[]).await;
}

#[tokio::test]
async fn given_a_regional_batch_aborts_when_one_name_is_recreated_then_retry_changes_neither_table()
{
    let operation = super::mixed::BatchOperation(storage_provider::DynamoBatchWriteShape::try_from_request(serde_json::from_value(serde_json::json!({"RequestItems": {
        (fixture::NAME): [{"PutRequest": {"Item": {"pk": {"S": "customer"}, "value": {"S": "first"}}}}],
        "stable": [{"PutRequest": {"Item": {"pk": {"S": "customer"}, "value": {"S": "second"}}}}]
    }})).unwrap()).unwrap());
    assert_retry_is_bound(operation, &["stable"]).await;
}

#[tokio::test]
async fn given_a_regional_transaction_aborts_when_one_name_is_recreated_then_retry_changes_neither_table()
 {
    let operation = super::mixed::TransactOperation(storage_provider::DynamoTransactWriteShape::try_from_request(serde_json::from_value(serde_json::json!({"TransactItems": [
        {"Put": {"TableName": fixture::NAME, "Item": {"pk": {"S": "customer"}, "value": {"S": "first"}}}},
        {"Put": {"TableName": "stable", "Item": {"pk": {"S": "customer"}, "value": {"S": "second"}}}}
    ]})).unwrap()).unwrap());
    assert_retry_is_bound(operation, &["stable"]).await;
}

async fn assert_retry_is_bound<O: OriginOperation>(operation: O, retained: &[&str]) {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&source, &peer]).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    for name in retained {
        fixture::create_table(&source, &TableName::new(*name)).await;
    }
    let context = Arc::new(
        OriginAttemptContext::try_new(
            &source,
            OriginExecution {
                journal: Arc::clone(&cluster.journals[0]),
                codec: fixture::codec(),
            },
            replication_types::identity::resource::OperationIdentity::fresh(),
        )
        .unwrap(),
    );
    let first_context = Arc::clone(&context);
    let first_operation = operation.clone();
    let error = source
        .with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
            Box::pin(async move {
                first_operation.execute(attempt, &first_context).await?;
                Err::<(), _>(storage_types::StorageError::from(
                    StorageEnum::TransactionConflict {
                        message: "test aborts the first native attempt".into(),
                    },
                ))
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
    cluster.shutdown().await;
    source
        .delete_table(DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    fixture::create_table(&source, &TableName::new(fixture::NAME)).await;
    let retry = source
        .with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
            Box::pin(async move { operation.execute(attempt, &context).await.map(|_| ()) })
        })
        .await;
    assert!(matches!(
        retry.unwrap_err().as_ref(),
        StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
    ));
    fixture::assert_item(&source, None).await;
    for name in retained {
        assert!(
            source
                .get_item(storage_types::GetItemRequest::new(
                    TableName::new(*name),
                    fixture::key()
                ))
                .await
                .unwrap()
                .item
                .is_none()
        );
    }
}
