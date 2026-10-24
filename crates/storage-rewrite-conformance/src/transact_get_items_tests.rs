//! Public TransactGetItems rules retained from legacy provider tests.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{DeleteTableRequest, PutItemRequest, TransactGetItemsRequest};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

/// Negative rule from the legacy Postgres completion test: a failed read does
/// not prevent a fresh request from reading a table created afterward.
#[tokio::test]
async fn given_a_missing_table_when_transact_get_fails_then_a_new_request_reads_the_created_table()
{
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let request = json!({
            "TransactItems": [{"Get": {
                "TableName": table,
                "Key": {"pk": {"S": "row"}}
            }}]
        });

        let missing = TransactGetItemsRequest::try_from(request.clone())
            .unwrap();
        let rejected = observe(store.operations.transact_get_items(missing)).await;
        rejected.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            rejected.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        );

        store.create_hash_table(table.clone()).await;
        let put = PutItemRequest::try_from(json!({
            "TableName": table,
            "Item": {"pk": {"S": "row"}, "value": {"S": "new"}}
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();

        let retry = TransactGetItemsRequest::try_from(request).unwrap();
        let observed = observe(store.operations.transact_get_items(retry)).await;
        observed.assert_no_warning_or_error();
        let response = observed.result.unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({"Responses": [{"Item": {
                "pk": {"S": "row"},
                "value": {"S": "new"}
            }}]})
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
async fn given_a_recreated_table_when_transact_get_runs_then_only_the_new_incarnation_is_visible() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let read = json!({
            "TransactItems": [{"Get": {
                "TableName": table,
                "Key": {"pk": {"S": "row"}}
            }}]
        });
        let old = PutItemRequest::try_from(json!({
            "TableName": table,
            "Item": {"pk": {"S": "row"}, "value": {"S": "old"}}
        }))
        .unwrap();
        store.operations.put_item(old).await.unwrap();

        let request = TransactGetItemsRequest::try_from(read.clone()).unwrap();
        let observed = observe(store.operations.transact_get_items(request)).await;
        observed.assert_no_warning_or_error();
        let old = observed.result.unwrap();
        assert_eq!(
            serde_json::to_value(old).unwrap()["Responses"][0]["Item"]["value"],
            json!({"S": "old"})
        );

        store
            .operations
            .delete_table(DeleteTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        let rejected = observe(
            store
                .operations
                .create_table(super::test_support::hash_table_request(table.clone())),
        )
        .await;
        rejected.assert_no_warning_or_error();
        assert!(
            rejected.result.is_err(),
            "deleted table name remains reserved"
        );
        store.advance_time(std::time::Duration::from_secs(31));
        store.create_hash_table(table.clone()).await;
        let request = TransactGetItemsRequest::try_from(read.clone()).unwrap();
        let observed = observe(store.operations.transact_get_items(request)).await;
        observed.assert_no_warning_or_error();
        let missing = observed.result.unwrap();
        assert_eq!(
            serde_json::to_value(missing).unwrap(),
            json!({"Responses": [{}]})
        );

        let new = PutItemRequest::try_from(json!({
            "TableName": table,
            "Item": {"pk": {"S": "row"}, "value": {"S": "new"}}
        }))
        .unwrap();
        store.operations.put_item(new).await.unwrap();
        let request = TransactGetItemsRequest::try_from(read).unwrap();
        let observed = observe(store.operations.transact_get_items(request)).await;
        observed.assert_no_warning_or_error();
        let new = observed.result.unwrap();
        assert_eq!(
            serde_json::to_value(new).unwrap(),
            json!({"Responses": [{"Item": {
                "pk": {"S": "row"},
                "value": {"S": "new"}
            }}]})
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
async fn given_arn_and_plain_table_names_when_transact_get_runs_then_capacity_modes_preserve_requested_spelling()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let arn = format!("arn:aws:dynamodb:eu-central-1:123456789012:table/{table}");
        let item = json!({"pk": {"S": "present"}, "value": {"S": "kept"}});
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table, "Item": item
                }))
                .expect("valid seed"),
            )
            .await
            .expect("seed item");
        for mode in [None, Some("NONE"), Some("TOTAL"), Some("INDEXES")] {
            let mut request = json!({"TransactItems": [
                {"Get": {"TableName": arn, "Key": {"pk": {"S": "present"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"S": "missing"}}}}
            ]});
            if let Some(mode) = mode {
                request["ReturnConsumedCapacity"] = json!(mode);
            }
            let response = store
                .operations
                .transact_get_items(
                    TransactGetItemsRequest::try_from(request).expect("valid capacity request"),
                )
                .await
                .expect("read mixed table spellings");
            let mut expected = json!({"Responses": [{"Item": item}, {}]});
            if matches!(mode, Some("TOTAL" | "INDEXES")) {
                let mut capacity =
                    json!({"TableName": arn, "CapacityUnits": 4.0, "ReadCapacityUnits": 4.0});
                if mode == Some("INDEXES") {
                    capacity["Table"] = json!({"CapacityUnits": 4.0, "ReadCapacityUnits": 4.0});
                }
                expected["ConsumedCapacity"] = json!([capacity]);
            }
            assert_eq!(
                serde_json::to_value(response).expect("serialize response"),
                expected
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_nested_item_when_transact_get_projects_then_aliases_lists_and_missing_paths_are_exact()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let item = json!({"pk": {"S": "item"}, "status": {"S": "active"},
            "nested": {"M": {"child": {"S": "value"}}},
            "events": {"L": [{"S": "created"}, {"S": "updated"}]}});
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table, "Item": item
                }))
                .expect("valid document"),
            )
            .await
            .expect("seed document");
        for (projection, aliases, expected) in [
            (
                "#s",
                json!({"#s": "status"}),
                json!({"Item": {"status": {"S": "active"}}}),
            ),
            (
                "nested.child, events[1]",
                json!(null),
                json!({"Item": {
                    "nested": {"M": {"child": {"S": "value"}}}, "events": {"L": [{"S": "updated"}]}
                }}),
            ),
            ("absent", json!(null), json!({})),
            ("nested.absent", json!(null), json!({})),
        ] {
            let mut get = json!({"TableName": table, "Key": {"pk": {"S": "item"}},
                "ProjectionExpression": projection});
            if !aliases.is_null() {
                get["ExpressionAttributeNames"] = aliases;
            }
            let request =
                TransactGetItemsRequest::try_from(json!({"TransactItems": [{"Get": get}]}))
                    .expect("valid projection");
            let response = store
                .operations
                .transact_get_items(request)
                .await
                .expect("project item");
            assert_eq!(
                serde_json::to_value(response).expect("serialize projection"),
                json!({"Responses": [expected]})
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_two_present_items_when_arn_and_plain_transact_get_runs_then_items_and_grouped_capacity_are_exact()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let arn = format!("arn:aws:dynamodb:eu-central-1:123456789012:table/{table}");
        let items = [json!({"pk": {"S": "first"}, "status": {"S": "active"}}),
            json!({"pk": {"S": "second"}, "status": {"S": "standby"}})];
        for item in &items {
            store.operations.put_item(PutItemRequest::try_from(json!({"TableName": table, "Item": item}))
                .expect("valid item")).await.expect("seed item");
        }
        let response = store.operations.transact_get_items(TransactGetItemsRequest::try_from(json!({
            "TransactItems": [
                {"Get": {"TableName": arn, "Key": {"pk": {"S": "first"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"S": "second"}}}}
            ], "ReturnConsumedCapacity": "TOTAL"
        })).expect("valid ARN request")).await.expect("read both items");
        assert_eq!(serde_json::to_value(response).expect("serialize items"), json!({
            "Responses": [{"Item": items[0]}, {"Item": items[1]}],
            "ConsumedCapacity": [{"TableName": arn, "CapacityUnits": 4.0, "ReadCapacityUnits": 4.0}]
        }));
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
