//! Deleted table history remains readable until bounded SystemStream
//! reclamation.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::SystemStreamTrimExecutor;
use storage_types::{CreateTableRequest, DeleteTableRequest, PutItemRequest, StreamName};
use stream_provider::{StreamRowKind, SystemStreamPageRequest};
use tracing::Level;

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

// Business and boundary rules from the legacy SQLite deleted-history trim test:
// table deletion keeps committed CDC readable, then a bounded trim reclaims old
// records without invalidating a checkpoint already issued to a reader.
#[tokio::test]
async fn given_deleted_table_history_when_cdc_is_read_and_trimmed_then_records_remain_until_reclaimed()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
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
        let created = observe(store.operations.create_table(create)).await;
        created.assert_no_warning_or_error();
        created.result.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        for key in ["first", "second"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}, "value": {"S": key}}
            }))
            .unwrap();
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.result.unwrap();
        }

        let issued = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ))
        .await;
        issued.assert_no_warning_or_error();
        let issued = issued.result.unwrap();
        assert_eq!(issued.records().len(), 2);
        let checkpoint = issued.issued_checkpoint().token().clone();

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        deleted.one_span_with("dynamo_operation", &[("operation", "DeleteTable")]);
        deleted.result.unwrap();

        let history = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ))
        .await;
        history.assert_no_warning_or_error();
        let history = history.result.unwrap();
        assert_eq!(history.records().len(), 2);
        for (record, key) in history.records().iter().zip(["first", "second"]) {
            assert_eq!(record.source_table_name.as_ref(), Some(&table));
            assert_eq!(
                serde_json::to_value(&record.new_image).unwrap(),
                json!({"pk": {"S": key}, "value": {"S": key}}),
            );
        }
        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 2).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        assert!(resumed.result.unwrap().records().is_empty());

        let worker = SystemStreamTrimExecutor::new(store.system_stream_trim_repository.as_ref(), 1);
        let cutoff = store.now() + 60_000;
        let trimmed = observe(worker.run_page(scope.clone(), cutoff, None)).await;
        trimmed.assert_no_warning_or_error();
        let trimmed = trimmed.result.unwrap();
        assert_eq!(trimmed.reclaimed_records(), 1);

        let system = StreamRowKind::System(StreamName::system_table_stream());
        let retained = observe(store.stream_operations.read_forward(system, None, 10)).await;
        retained.assert_no_warning_or_error();
        assert_eq!(retained.result.unwrap().items.len(), 1);

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint.clone()), 2).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert!(resumed.records().is_empty());
        assert_eq!(resumed.issued_checkpoint().token(), &checkpoint);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary rule from the legacy SQLite same-name trim test: reclaiming an old
// incarnation must not remove a later incarnation's history or its checkpoint.
#[tokio::test]
async fn given_recreated_table_when_old_cdc_is_trimmed_then_new_history_and_checkpoint_survive() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = || {
            CreateTableRequest::try_from(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }))
            .unwrap()
        };
        let created = observe(store.operations.create_table(create())).await;
        created.assert_no_warning_or_error();
        created.result.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        let put = |key, status| {
            PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}, "status": {"S": status}}
            }))
            .unwrap()
        };
        for key in ["same-key", "protected-old-key"] {
            let old_write = observe(store.operations.put_item(put(key, "old"))).await;
            old_write.assert_no_warning_or_error();
            old_write.result.unwrap();
        }
        let old_page = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ))
        .await;
        old_page.assert_no_warning_or_error();
        let old_page = old_page.result.unwrap();
        assert_eq!(old_page.records().len(), 2);
        let checkpoint = old_page.issued_checkpoint().token().clone();
        let old_cutoff = store.now() + 1;

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        deleted.result.unwrap();
        store.advance_time(Duration::from_secs(31));
        let recreated = observe(store.operations.create_table(create())).await;
        recreated.assert_no_warning_or_error();
        recreated.result.unwrap();
        let new_write = observe(store.operations.put_item(put("same-key", "new"))).await;
        new_write.assert_no_warning_or_error();
        new_write.result.unwrap();

        let before_trim = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 3).unwrap(),
        ))
        .await;
        before_trim.assert_no_warning_or_error();
        let before_trim = before_trim.result.unwrap();
        assert_eq!(before_trim.records().len(), 3);
        let new_sequence = before_trim.records()[2].sequence_number.clone();

        let worker = SystemStreamTrimExecutor::new(
            store.system_stream_trim_repository.as_ref(),
            10,
        );
        let trimmed = observe(worker.run_page(scope.clone(), old_cutoff, None)).await;
        trimmed.assert_no_warning_or_error();
        assert_eq!(trimmed.result.unwrap().reclaimed_records(), 1);

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint), 2).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert_eq!(resumed.records().len(), 1);
        assert_eq!(resumed.records()[0].source_table_name.as_ref(), Some(&table));
        assert_eq!(resumed.records()[0].sequence_number, new_sequence);
        assert_eq!(
            serde_json::to_value(&resumed.records()[0].new_image).unwrap(),
            json!({"pk": {"S": "same-key"}, "status": {"S": "new"}}),
        );

        let unavailable = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, None, 2).unwrap(),
        ))
        .await;
        let error = unavailable.only_warning_or_error_in_span(
            "system_stream_operation",
            &[("operation", "GetSystemStreamPage"), ("stream_name", "system")],
        );
        assert_eq!(error.level, Level::ERROR);
        assert_eq!(
            error.fields.get("error_code").map(String::as_str),
            Some("HistoryUnavailable"),
        );
        assert_eq!(
            error.fields.get("diagnostic").map(String::as_str),
            Some("history page is not contiguous or its pointer evidence differs"),
        );
        assert_eq!(
            error.fields.get("failure_site").map(String::as_str),
            Some("system_stream.read.finish_bounded"),
        );
        assert_eq!(
            error.fields.get("message").map(String::as_str),
            Some("required logical history could not be verified"),
        );
        assert_eq!(error.fields.len(), 4, "no item key or image is logged");
        assert!(!format!("{:?}{:?}", unavailable.events, unavailable.spans).contains("same-key"));
        assert_dynamo_error_response_exact(
            unavailable.result.unwrap_err().into_storage_error(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
