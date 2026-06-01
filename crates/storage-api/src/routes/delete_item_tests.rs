use serde_json::json;
use storage::DatabaseManager;
use storage_types::AttributeValue;

use crate::{
    routes::routes_support_tests::{
        create_test_db, handle_create_table, handle_delete_item, handle_put_item,
    },
    types::Response,
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn delete_item_hash_only_key() {
    let db = create_test_db_manager().await;
    let create_table_payload = json!({
        "TableName": "Users",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let create_result =
        handle_create_table(db.clone(), create_table_payload.try_into().unwrap()).await;
    assert!(
        create_result.is_ok(),
        "CreateTable should succeed: {create_result:?}"
    );

    // First put an item
    let put_payload = json!({
        "TableName": "Users",
        "Item": {
            "id": {"S": "user123"},
            "name": {"S": "John Doe"}
        }
    });

    let put_result = handle_put_item(db.clone(), put_payload.try_into().unwrap()).await;
    assert!(put_result.is_ok(), "PutItem should succeed: {put_result:?}");
    let delete_payload = json!({
        "TableName": "Users",
        "Key": {
            "id": {"S": "user123"}
        }
    });

    let delete_result = handle_delete_item(db.clone(), delete_payload.try_into().unwrap()).await;
    assert!(
        delete_result.is_ok(),
        "DeleteItem should succeed in hash-only table: {delete_result:?}"
    );

    match delete_result.unwrap() {
        Response::DeleteItem(response) => {
            assert!(
                response.attributes.is_none(),
                "DeleteItem should not return deleted item attributes by default"
            );
        }
        other => panic!("Expected DeleteItem response, got: {other:?}"),
    }
}

#[tokio::test]
async fn delete_item_return_values_all_old_returns_deleted_item() {
    let db = create_test_db_manager().await;
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "DeleteReturnAllOld",
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("create table");
    handle_put_item(
        db.clone(),
        json!({
            "TableName": "DeleteReturnAllOld",
            "Item": {
                "id": {"S": "item1"},
                "status": {"S": "open"}
            }
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("seed item");

    let response = handle_delete_item(
        db,
        json!({
            "TableName": "DeleteReturnAllOld",
            "Key": {"id": {"S": "item1"}},
            "ReturnValues": "ALL_OLD"
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("delete should succeed");

    match response {
        Response::DeleteItem(response) => {
            let attributes = response.attributes.expect("deleted item attributes");
            assert_eq!(
                attributes.get("status"),
                Some(&AttributeValue::S("open".to_string()))
            );
        }
        other => panic!("Expected DeleteItem response, got: {other:?}"),
    }
}

#[tokio::test]
async fn delete_item_condition_failure_returns_dynamodb_error_code() {
    let db = create_test_db_manager().await;

    let create_table_payload = json!({
        "TableName": "DeleteConditional",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });
    handle_create_table(db.clone(), create_table_payload.try_into().unwrap())
        .await
        .expect("create table");

    let put_payload = json!({
        "TableName": "DeleteConditional",
        "Item": {
            "id": {"S": "item1"},
            "status": {"S": "open"}
        }
    });
    handle_put_item(db.clone(), put_payload.try_into().unwrap())
        .await
        .expect("seed item");

    let conditional_delete_payload = json!({
        "TableName": "DeleteConditional",
        "Key": {
            "id": {"S": "item1"}
        },
        "ConditionExpression": "#status = :expected",
        "ExpressionAttributeNames": {
            "#status": "status"
        },
        "ExpressionAttributeValues": {
            ":expected": {"S": "closed"}
        }
    });

    let err = handle_delete_item(db, conditional_delete_payload.try_into().unwrap())
        .await
        .expect_err("conditional delete should fail");

    assert_eq!(err.status_code, 400);
    assert_eq!(
        err.error_type,
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
    assert_eq!(err.message, "The conditional request failed");
}

#[tokio::test]
async fn delete_item_condition_failure_returns_all_old_item_when_requested() {
    let db = create_test_db_manager().await;

    handle_create_table(
        db.clone(),
        json!({
            "TableName": "DeleteConditionalAllOld",
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("create table");
    handle_put_item(
        db.clone(),
        json!({
            "TableName": "DeleteConditionalAllOld",
            "Item": {
                "id": {"S": "item1"},
                "status": {"S": "open"}
            }
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("seed item");

    let err = handle_delete_item(
        db,
        json!({
            "TableName": "DeleteConditionalAllOld",
            "Key": {"id": {"S": "item1"}},
            "ConditionExpression": "#status = :expected",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":expected": {"S": "closed"}},
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect_err("conditional delete should fail");

    let item = err.item.expect("conditional failure item");
    assert_eq!(
        item.get("status"),
        Some(&AttributeValue::S("open".to_string()))
    );
}
