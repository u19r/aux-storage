use serde_json::json;
use storage::DatabaseManager;
use storage_types::AttributeValue;

use crate::{
    routes::routes_support_tests::{
        create_test_db, handle_create_table, handle_get_item, handle_put_item,
    },
    types::Response,
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn put_item_with_special_characters() {
    let db = create_test_db_manager().await;
    let create_table_payload = json!({
        "TableName": "SpecialChars",
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
    let put_payload = json!({
        "TableName": "SpecialChars",
        "Item": {
            "id": {"S": "special1"},
            "text": {"S": "Special chars: !@#$%^&*()[]{}|;':\",./<>?"},
            "unicode": {"S": "Unicode: 你好世界 🌍"}
        }
    });

    let put_result = handle_put_item(db.clone(), put_payload.try_into().unwrap()).await;
    assert!(
        put_result.is_ok(),
        "PutItem with special characters should succeed: {put_result:?}"
    );
    let get_payload = json!({
        "TableName": "SpecialChars",
        "Key": {
            "id": {"S": "special1"}
        }
    });

    let get_result = handle_get_item(db.clone(), get_payload.try_into().unwrap()).await;
    assert!(get_result.is_ok(), "GetItem should succeed: {get_result:?}");

    let response = expect_get_item_response(get_result.unwrap());
    assert!(
        response.item.is_some(),
        "GetItem should return the item with special characters"
    );

    let item = response.item.unwrap();

    assert!(matches!(item.get("id").unwrap(), AttributeValue::S(_)));

    assert_eq!(
        item.get("id"),
        Some(&AttributeValue::S("special1".to_string()))
    );
    assert_eq!(
        item.get("text"),
        Some(&AttributeValue::S(
            "Special chars: !@#$%^&*()[]{}|;':\",./<>?".to_string()
        ))
    );
    assert_eq!(
        item.get("unicode"),
        Some(&AttributeValue::S("Unicode: 你好世界 🌍".to_string()))
    );
}

#[tokio::test]
async fn put_item_with_pipe_character() {
    let db = create_test_db_manager().await;
    let create_table_payload = json!({
        "TableName": "PipeTest",
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
    let put_payload = json!({
        "TableName": "PipeTest",
        "Item": {
            "id": {"S": "pipe1"},
            "text": {"S": "Contains | pipe character"},
            "more_text": {"S": "Multiple | pipes | here"}
        }
    });

    let put_result = handle_put_item(db.clone(), put_payload.try_into().unwrap()).await;
    assert!(
        put_result.is_ok(),
        "PutItem with pipe character should succeed: {put_result:?}"
    );
    let get_payload = json!({
        "TableName": "PipeTest",
        "Key": {
            "id": {"S": "pipe1"}
        }
    });

    let get_result = handle_get_item(db.clone(), get_payload.try_into().unwrap()).await;
    assert!(get_result.is_ok(), "GetItem should succeed: {get_result:?}");

    let response = expect_get_item_response(get_result.unwrap());
    assert!(
        response.item.is_some(),
        "GetItem should return the item with pipe characters"
    );

    let item = response.item.unwrap();
    assert_eq!(
        item.get("text"),
        Some(&AttributeValue::S("Contains | pipe character".to_string()))
    );
    assert_eq!(
        item.get("more_text"),
        Some(&AttributeValue::S("Multiple | pipes | here".to_string()))
    );
}

#[tokio::test]
async fn put_item_condition_failure_returns_dynamodb_error_code() {
    let db = create_test_db_manager().await;

    let create_table_payload = json!({
        "TableName": "ConditionalPut",
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

    let first_put_payload = json!({
        "TableName": "ConditionalPut",
        "Item": {
            "id": {"S": "item1"},
            "value": {"S": "v1"}
        }
    });
    handle_put_item(db.clone(), first_put_payload.try_into().unwrap())
        .await
        .expect("seed item");

    let conditional_put_payload = json!({
        "TableName": "ConditionalPut",
        "Item": {
            "id": {"S": "item1"},
            "value": {"S": "v2"}
        },
        "ConditionExpression": "attribute_not_exists(id)"
    });
    let err = handle_put_item(db, conditional_put_payload.try_into().unwrap())
        .await
        .expect_err("conditional put should fail");

    assert_eq!(err.status_code, 400);
    assert_eq!(
        err.error_type,
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
    assert_eq!(err.message, "The conditional request failed");
}

#[tokio::test]
async fn put_item_condition_failure_returns_all_old_item_when_requested() {
    let db = create_test_db_manager().await;

    handle_create_table(
        db.clone(),
        json!({
            "TableName": "ConditionalPutAllOld",
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
            "TableName": "ConditionalPutAllOld",
            "Item": {
                "id": {"S": "item1"},
                "value": {"S": "old"}
            }
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect("seed item");

    let err = handle_put_item(
        db,
        json!({
            "TableName": "ConditionalPutAllOld",
            "Item": {
                "id": {"S": "item1"},
                "value": {"S": "new"}
            },
            "ConditionExpression": "attribute_not_exists(id)",
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
        })
        .try_into()
        .unwrap(),
    )
    .await
    .expect_err("conditional put should fail");

    let item = err.item.expect("conditional failure item");
    assert_eq!(
        item.get("value"),
        Some(&AttributeValue::S("old".to_string()))
    );
}

fn expect_get_item_response(response: Response) -> storage_types::GetItemResponse {
    match response {
        Response::GetItem(response) => response,
        Response::GetWire(response) => response.into_get_item_response().unwrap(),
        other => panic!("Expected GetItem response, got: {other:?}"),
    }
}
