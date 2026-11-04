//! Customer-visible DeleteTable behavior across replacement providers.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::TableReclaimStep;
use storage_types::{
    DeleteTableRequest, DescribeTableRequest, GetItemRequest, ListTablesRequest, PutItemRequest,
    TableStatus, UpdateTableRequest,
};

use super::test_support::{TestStore, assert_dynamo_error_response_exact};

#[tokio::test]
async fn given_explicit_table_appends_when_deleted_then_old_id_access_is_revoked_and_peer_is_preserved()
 {
    use storage_types::TableHistoryTarget;
    use stream_provider::{CursorName, CursorPosition, StreamAppendOperationId, StreamKind};

    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let mut targets = Vec::new();
        for _ in 0..2 {
            let name = super::test_support::unique_table_name();
            let created = store
                .operations
                .create_table(super::test_support::hash_table_request(name))
                .await
                .unwrap();
            targets.push(TableHistoryTarget::try_from(&created.table_description).unwrap());
        }
        let operation = StreamAppendOperationId::fresh();
        for target in &targets {
            store
                .stream_operations
                .append_table_item(operation, target.clone(), b"payload")
                .await
                .unwrap();
            let kind = StreamKind::Table(target.history_id.append_stream_name());
            store
                .stream_operations
                .create_cursor(
                    kind.clone(),
                    CursorName::new("reader"),
                    CursorPosition::Head,
                )
                .await
                .unwrap();
            let page = store
                .stream_operations
                .read_forward(kind, None, 10)
                .await
                .unwrap();
            assert_eq!(page.items.len(), 1);
            assert_eq!(page.items[0].data, b"payload");
        }
        let old = &targets[0];
        let wrong = TableHistoryTarget {
            table_name: old.table_name.clone(),
            history_id: targets[1].history_id,
        };
        assert!(
            store
                .stream_operations
                .append_table_item(StreamAppendOperationId::fresh(), wrong, b"rejected")
                .await
                .is_err()
        );
        store
            .operations
            .delete_table(DeleteTableRequest {
                table_name: old.table_name.clone(),
            })
            .await
            .unwrap();
        let kind = StreamKind::Table(old.history_id.append_stream_name());
        assert!(
            store
                .stream_operations
                .read_forward(kind.clone(), None, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .stream_operations
                .get_cursor(kind.clone(), CursorName::new("reader"))
                .await
                .is_err()
        );
        assert!(
            store
                .stream_operations
                .create_cursor(kind, CursorName::new("late"), CursorPosition::Head)
                .await
                .is_err()
        );
        assert!(
            store
                .stream_operations
                .append_table_item(operation, old.clone(), b"payload")
                .await
                .is_err()
        );
        for _ in 0..100 {
            if store
                .table_reclaim_repository
                .reclaim_table_page()
                .await
                .unwrap()
                == TableReclaimStep::Complete
            {
                break;
            }
        }
        let peer = StreamKind::Table(targets[1].history_id.append_stream_name());
        let page = store
            .stream_operations
            .read_forward(peer.clone(), None, 10)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].data, b"payload");
        assert!(
            store
                .stream_operations
                .get_cursor(peer, CursorName::new("reader"))
                .await
                .unwrap()
                .is_some()
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_a_table_with_an_item_when_deleted_and_recreated_then_old_data_is_absent() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}, "value": {"S": "before-delete"}}
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();
        super::table_stream_eligibility_tests::set_stream(&store, &table, true)
            .await
            .unwrap();
        super::table_stream_eligibility_tests::finish_initialization(&store, &table)
            .await
            .unwrap();
        let create_index = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "gpk", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .unwrap();
        store.operations.update_table(create_index).await.unwrap();

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        let span = deleted.one_span_with("dynamo_operation", &[("operation", "DeleteTable")]);
        assert_eq!(span.fields.len(), 1);
        let description = deleted.result.unwrap().table_description;
        assert_eq!(description.table_name, table);
        assert_eq!(description.table_status, TableStatus::Deleting);
        let missing = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: table.clone(),
        }))
        .await;
        missing.assert_no_warning_or_error();
        let missing_error = json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
            "message": format!("Requested resource not found: Table: {} not found", table.as_ref()),
        })
        .to_string();
        assert_dynamo_error_response_exact(
            missing.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &missing_error,
        );
        let listed = store
            .operations
            .list_tables(ListTablesRequest {
                exclusive_start_table_name: None,
                limit: None,
            })
            .await
            .unwrap();
        assert!(
            !listed.table_names.contains(&table),
            "deleted table remains listed"
        );

        let repeated = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        repeated.assert_no_warning_or_error();
        let expected_error = json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
            "message": format!("Requested resource not found: Table: {} not found", table.as_ref()),
        })
        .to_string();
        assert_dynamo_error_response_exact(
            repeated.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &expected_error,
        );

        let recreated = observe(
            store
                .operations
                .create_table(super::test_support::hash_table_request(table.clone())),
        )
        .await;
        recreated.assert_no_warning_or_error();
        recreated.result.unwrap();
        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert_eq!(described.table.table_name, table);
        assert_ne!(described.table.table_id, description.table_id);
        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        }))
        .unwrap();
        let fetched = store.operations.get_item(get).await.unwrap();
        assert!(
            fetched.item.is_none(),
            "item from deleted table became visible again"
        );

        let replacement = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}, "value": {"S": "after-recreate"}}
        }))
        .unwrap();
        store.operations.put_item(replacement).await.unwrap();
        let mut advanced = 0;
        for _ in 0..1000 {
            match store
                .table_reclaim_repository
                .reclaim_table_page()
                .await
                .unwrap()
            {
                TableReclaimStep::Advanced => advanced += 1,
                TableReclaimStep::Complete => break,
            }
        }
        assert!(advanced > 0, "deleted table should have reclaim work");
        assert_eq!(
            store
                .table_reclaim_repository
                .reclaim_table_page()
                .await
                .unwrap(),
            TableReclaimStep::Complete,
            "deleted table reclaim must finish within a bounded number of pages"
        );
        let fetched = store
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(fetched).unwrap()["Item"]["value"],
            json!({"S": "after-recreate"}),
            "reclaiming the deleted incarnation must preserve replacement items"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
