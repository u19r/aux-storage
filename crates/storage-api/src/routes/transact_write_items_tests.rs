use serde_json::json;
use storage::DatabaseManager;
use storage_types::{AttributeValue, TableName, TransactWriteItemsRequest};

use crate::{
    routes::routes_support_tests::{
        create_test_db, handle_create_table, handle_transact_write_items,
    },
    types::Response,
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn transact_write_items_empty_request() {
    let _db = create_test_db_manager().await;
    let request = json!({
        "TransactItems": []
    });

    let result: Result<TransactWriteItemsRequest, String> = request.try_into();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("TransactItems cannot be empty")
    );
}

#[tokio::test]
async fn transact_write_items_invalid_operation_count() {
    let _db = create_test_db_manager().await;
    let request = json!({
        "TransactItems": [
            {
                "Put": {"TableName": "test", "Item": {"id": {"S": "test"}}},
                "Delete": {"TableName": "test", "Key": {"id": {"S": "test"}}}
            }
        ]
    });

    let result: Result<TransactWriteItemsRequest, String> = request.try_into();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("can only contain one of"), "Error is: {err:?}");
}

#[tokio::test]
async fn transact_write_items_valid_request() {
    let db = create_test_db_manager().await;
    let create_table_payload = json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let create_request = create_table_payload
        .try_into()
        .expect("Failed to parse create table request");
    let _create_response = handle_create_table(db.clone(), create_request)
        .await
        .expect("Failed to create table");
    let request = json!({
        "TransactItems": [
            {
                "Put": {
                    "TableName": "TestTable",
                    "Item": {
                        "id": {"S": "test1"},
                        "data": {"S": "value1"}
                    }
                }
            }
        ]
    });

    let transact_request = request
        .try_into()
        .expect("Failed to parse transact write items request");

    let response = handle_transact_write_items(db, transact_request)
        .await
        .expect("Failed to handle transact write items");

    match response {
        Response::TransactWriteItems(resp) => {
            // Basic validation that we get a response
            assert!(resp.consumed_capacity.is_none());
            assert!(resp.item_collection_metrics.is_none());
        }
        _ => panic!("Expected TransactWriteItems response"),
    }
}

#[tokio::test]
async fn transact_write_items_put_operations() {
    let db = create_test_db_manager().await;
    let create_table_request = json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {
                "AttributeName": "id",
                "AttributeType": "S"
            }
        ],
        "KeySchema": [
            {
                "AttributeName": "id",
                "KeyType": "HASH"
            }
        ]
    });

    let create_result =
        handle_create_table(db.clone(), create_table_request.try_into().unwrap()).await;
    assert!(create_result.is_ok());

    // Now test transaction with PUT operations
    let request = json!({
        "TransactItems": [
            {
                "Put": {
                    "TableName": "TestTable",
                    "Item": {
                        "id": {"S": "item1"},
                        "name": {"S": "Test Item 1"}
                    }
                }
            },
            {
                "Put": {
                    "TableName": "TestTable",
                    "Item": {
                        "id": {"S": "item2"},
                        "name": {"S": "Test Item 2"}
                    }
                }
            }
        ]
    });

    let transact_request = request
        .try_into()
        .expect("Failed to parse transact write items request");

    let response = handle_transact_write_items(db.clone(), transact_request)
        .await
        .expect("Failed to handle transact write items");

    match response {
        Response::TransactWriteItems(resp) => {
            assert!(resp.consumed_capacity.is_none()); // Default behavior when not requested
            assert!(resp.item_collection_metrics.is_none());
        }
        _ => panic!("Expected TransactWriteItems response"),
    }
    let get_result1 = db
        .get_item_map(
            TableName::new("TestTable"),
            std::collections::HashMap::from([(
                "id".to_string(),
                storage_types::AttributeValue::S("item1".to_string()),
            )]),
        )
        .await;
    assert!(get_result1.is_ok());
    assert!(get_result1.unwrap().is_some());

    let get_result2 = db
        .get_item_map(
            TableName::new("TestTable"),
            std::collections::HashMap::from([(
                "id".to_string(),
                storage_types::AttributeValue::S("item2".to_string()),
            )]),
        )
        .await;
    assert!(get_result2.is_ok());
    assert!(get_result2.unwrap().is_some());
}

#[tokio::test]
async fn transact_write_items_accepts_aux_item_stream_ttl_without_storing_attribute() {
    let db = create_test_db_manager().await;
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "TxnRouteDuration",
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create table");

    handle_transact_write_items(
        db.clone(),
        json!({
            "TransactItems": [
                {
                    "Put": {
                        "TableName": "TxnRouteDuration",
                        "Item": {
                            "id": {"S": "put-item"},
                            "data": {"S": "created"}
                        },
                        "AuxItemStreamTtlHours": 120
                    }
                },
                {
                    "Update": {
                        "TableName": "TxnRouteDuration",
                        "Key": {"id": {"S": "update-item"}},
                        "UpdateExpression": "SET #data = :data",
                        "ExpressionAttributeNames": {"#data": "data"},
                        "ExpressionAttributeValues": {
                            ":data": {"S": "updated"}
                        },
                        "AuxItemStreamTtlHours": -1
                    }
                }
            ]
        })
        .try_into()
        .expect("transact write items request"),
    )
    .await
    .expect("transaction with custom item stream ttl");

    let put_item = get_txn_route_item(db.clone(), "put-item").await;
    assert_eq!(
        put_item.get("data"),
        Some(&AttributeValue::S("created".to_string()))
    );
    assert!(
        !put_item.contains_key("AuxItemStreamTtlHours"),
        "transact put aux ttl is request metadata, not item data"
    );

    let update_item = get_txn_route_item(db, "update-item").await;
    assert_eq!(
        update_item.get("data"),
        Some(&AttributeValue::S("updated".to_string()))
    );
    assert!(
        !update_item.contains_key("AuxItemStreamTtlHours"),
        "transact update aux ttl is request metadata, not item data"
    );
}

#[tokio::test]
async fn transact_write_condition_failure_returns_all_old_item_when_requested() {
    let db = create_test_db_manager().await;
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "TxnConditionAllOld",
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("create table");
    db.put_item(storage::PutItemInput {
        table_name: TableName::new("TxnConditionAllOld"),
        item: std::collections::HashMap::from([
            ("id".to_string(), AttributeValue::S("item1".to_string())),
            ("status".to_string(), AttributeValue::S("open".to_string())),
        ])
        .into(),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        return_old_on_condition_failure: false,
        aux_item_stream_ttl_hours: None,
    })
    .await
    .expect("seed item");

    let err = handle_transact_write_items(
        db,
        json!({
            "TransactItems": [{
                "Update": {
                    "TableName": "TxnConditionAllOld",
                    "Key": {"id": {"S": "item1"}},
                    "UpdateExpression": "SET #status = :next",
                    "ConditionExpression": "#status = :expected",
                    "ExpressionAttributeNames": {"#status": "status"},
                    "ExpressionAttributeValues": {
                        ":next": {"S": "closed"},
                        ":expected": {"S": "missing"}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
            }]
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect_err("transaction should fail");

    let reasons = err.cancellation_reasons.expect("cancellation reasons");
    let item = reasons[0].item.as_ref().expect("conditional failure item");
    assert_eq!(
        item.get("status"),
        Some(&AttributeValue::S("open".to_string()))
    );
}

#[tokio::test]
async fn competing_transaction_writes_return_the_atomic_winner_as_all_old() {
    let db = create_test_db_manager().await;
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "TxnConditionalAtomicPreimage",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create table");
    db.put_item(storage::PutItemInput {
        table_name: TableName::new("TxnConditionalAtomicPreimage"),
        item: std::collections::HashMap::from([
            ("pk".to_string(), AttributeValue::S("p".to_string())),
            ("status".to_string(), AttributeValue::S("open".to_string())),
        ])
        .into(),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        return_old_on_condition_failure: false,
        aux_item_stream_ttl_hours: None,
    })
    .await
    .expect("seed item");

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = tokio::task::JoinSet::new();
    for next in ["closed-a", "closed-b"] {
        let db = db.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            let request = json!({
                "TransactItems": [{
                    "Update": {
                        "TableName": "TxnConditionalAtomicPreimage",
                        "Key": {"pk": {"S": "p"}},
                        "UpdateExpression": "SET #status = :next",
                        "ConditionExpression": "#status = :expected",
                        "ExpressionAttributeNames": {"#status": "status"},
                        "ExpressionAttributeValues": {
                            ":next": {"S": next},
                            ":expected": {"S": "open"}
                        },
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }]
            })
            .try_into()
            .expect("transaction request");
            barrier.wait().await;
            (next, handle_transact_write_items(db, request).await)
        });
    }

    let mut winner = None;
    let mut loser_item = None;
    while let Some(result) = tasks.join_next().await {
        let (next, result) = result.expect("transaction task");
        match result {
            Ok(_) => winner = Some(next),
            Err(error) => {
                loser_item = error
                    .cancellation_reasons
                    .and_then(|mut reasons| reasons.pop())
                    .and_then(|reason| reason.item)
            }
        }
    }
    let winner = winner.expect("one transaction wins");
    let loser_item = loser_item.expect("one transaction loses with an atomic preimage");
    assert_eq!(
        loser_item.get("status"),
        Some(&AttributeValue::S(winner.to_string()))
    );
}

async fn get_txn_route_item(
    db: std::sync::Arc<DatabaseManager>,
    id: &str,
) -> std::collections::HashMap<String, AttributeValue> {
    db.get_item_map(
        TableName::new("TxnRouteDuration"),
        std::collections::HashMap::from([("id".to_string(), AttributeValue::S(id.to_string()))]),
    )
    .await
    .expect("get transaction item")
    .unwrap_or_else(|| panic!("expected transaction item {id}"))
}
