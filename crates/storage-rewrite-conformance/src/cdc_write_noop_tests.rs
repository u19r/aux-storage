//! Missing-item deletes leave the CDC frontier unchanged; accepted writes
//! remain durable.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    BatchWriteItemRequest, CreateTableRequest, DescribeTableRequest, GetItemRequest,
    PutItemRequest, TransactWriteItemsRequest,
};
use stream_provider::SystemStreamPageRequest;

use super::test_support::{TestStore, unique_table_name};

// Business and boundary rules from the legacy SQL no-op delete test and the
// authoritative TransactWriteItems missing-delete response fixture.
#[tokio::test]
async fn given_missing_item_when_batch_and_transaction_delete_it_then_cdc_waits_for_a_real_write() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let private_key = "private_item_key_marker_7b1c";
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let initial = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap(),
        ))
        .await;
        initial.assert_no_warning_or_error();
        let initial = initial.result.unwrap();
        assert!(initial.records().is_empty());
        let checkpoint = initial.issued_checkpoint().token().clone();

        let batch = BatchWriteItemRequest::try_from(json!({
            "RequestItems": {
                table.as_ref(): [{"DeleteRequest": {"Key": {"pk": {"S": private_key}}}}]
            }
        }))
        .unwrap();
        let batch_result = observe(store.operations.batch_write_item(batch)).await;
        batch_result.assert_no_warning_or_error();
        let batch_span =
            batch_result.one_span_with("dynamo_operation", &[("operation", "BatchWriteItem")]);
        assert_eq!(batch_span.fields.len(), 1);
        assert!(
            !format!("{:?}{:?}", batch_result.events, batch_result.spans).contains(private_key)
        );
        assert_eq!(
            serde_json::to_value(batch_result.result.unwrap()).unwrap(),
            json!({"UnprocessedItems": {}}),
        );
        let after_batch = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 1).unwrap(),
        ))
        .await;
        after_batch.assert_no_warning_or_error();
        let after_batch = after_batch.result.unwrap();
        assert!(after_batch.records().is_empty());
        assert_eq!(after_batch.issued_checkpoint().token(), &checkpoint);

        let transaction = TransactWriteItemsRequest::try_from(json!({
            "TransactItems": [{"Delete": {
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": private_key}}
            }}]
        }))
        .unwrap();
        let transaction_result = observe(store.operations.transact_write_items(transaction)).await;
        transaction_result.assert_no_warning_or_error();
        let transaction_span = transaction_result
            .one_span_with("dynamo_operation", &[("operation", "TransactWriteItems")]);
        assert_eq!(transaction_span.fields.len(), 1);
        assert!(
            !format!(
                "{:?}{:?}",
                transaction_result.events, transaction_result.spans
            )
            .contains(private_key)
        );
        assert_eq!(
            serde_json::to_value(transaction_result.result.unwrap()).unwrap(),
            json!({})
        );
        let after_transaction = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 1).unwrap(),
        ))
        .await;
        after_transaction.assert_no_warning_or_error();
        let after_transaction = after_transaction.result.unwrap();
        assert!(after_transaction.records().is_empty());
        assert_eq!(after_transaction.issued_checkpoint().token(), &checkpoint);

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": private_key}}
        }))
        .unwrap();
        assert!(store.operations.get_item(get).await.unwrap().item.is_none());

        let image = json!({"pk": {"S": private_key}, "status": {"S": "created"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": image,
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.result.unwrap();
        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint), 1).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert_eq!(resumed.records().len(), 1);
        assert_eq!(
            resumed.records()[0].source_table_name.as_ref(),
            Some(&table)
        );
        assert_eq!(
            serde_json::to_value(&resumed.records()[0].new_image).unwrap(),
            image
        );
        assert!(resumed.records()[0].old_image.is_none());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule: a table without DynamoDB Streams stays undiscoverable after
// a write, while the replacement store still records that write internally.
#[tokio::test]
async fn given_stream_disabled_table_when_item_is_put_then_public_stream_stays_disabled_and_internal_cdc_records_it()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let disabled = store.create_standard_table().await;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let item = json!({"pk": {"S": "disabled-item"}, "value": {"S": "stored"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": disabled.as_ref(),
            "Item": item,
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
        assert_eq!(
            serde_json::to_value(written.result.unwrap()).unwrap(),
            json!({})
        );

        let get = GetItemRequest::try_from(json!({
            "TableName": disabled.as_ref(),
            "Key": {"pk": {"S": "disabled-item"}},
        }))
        .unwrap();
        let persisted = observe(store.operations.get_item(get)).await;
        persisted.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(persisted.result.unwrap()).unwrap(),
            json!({"Item": item}),
        );
        let description = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: disabled.clone(),
        }))
        .await;
        description.assert_no_warning_or_error();
        let description = description.result.unwrap().table;
        assert!(description.latest_stream_arn.is_none());
        assert!(description.latest_stream_label.is_none());
        assert!(description.stream_specification.is_none());

        let internal = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap(),
        ))
        .await;
        internal.assert_no_warning_or_error();
        let internal = internal.result.unwrap();
        assert_eq!(internal.records().len(), 1);
        assert_eq!(
            internal.records()[0].source_table_name.as_ref(),
            Some(&disabled)
        );
        assert_eq!(
            serde_json::to_value(&internal.records()[0].new_image).unwrap(),
            item
        );
        assert!(internal.records()[0].old_image.is_none());
        assert!(internal.terminal());
        let checkpoint = internal.issued_checkpoint().token().clone();

        let enabled = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": enabled.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let enabled_item = json!({"pk": {"S": "enabled-item"}, "value": {"S": "visible"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": enabled.as_ref(),
            "Item": enabled_item,
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.result.unwrap();
        let description = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: enabled.clone(),
        }))
        .await;
        description.assert_no_warning_or_error();
        let description = description.result.unwrap().table;
        assert!(description.latest_stream_arn.is_some());
        assert!(description.latest_stream_label.is_some());

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint), 2).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert_eq!(resumed.records().len(), 1);
        assert!(resumed.terminal());
        assert_eq!(
            resumed.records()[0].source_table_name.as_ref(),
            Some(&enabled)
        );
        assert_eq!(
            serde_json::to_value(&resumed.records()[0].new_image).unwrap(),
            enabled_item,
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
