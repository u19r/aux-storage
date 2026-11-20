use serde_json::json;
use storage_types::AttributeValue;

use crate::{
    routes::routes_tests::{
        create_sqlite_test_db, for_each_conformance_backend, handle_create_table, handle_get_item,
        handle_put_item,
    },
    types::Response,
};

#[tokio::test]
async fn given_missing_key_when_put_runs_then_schema_validation_identifies_the_key() {
    let db = create_sqlite_test_db().await;
    handle_create_table(
        db.clone(),
        json!({"TableName": "PutErrorOrder", "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let error = handle_put_item(
        db,
        json!({"TableName": "PutErrorOrder", "Item": {"other": {"S": "x"}}})
            .try_into()
            .unwrap(),
    )
    .await
    .expect_err("missing key must fail");
    assert_eq!(
        error.message,
        "One or more parameter values were invalid: Missing the key id in the item"
    );
    assert_eq!(
        error.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
}

#[tokio::test]
async fn given_existing_item_when_put_returns_all_old_then_old_image_is_returned() {
    let db = create_sqlite_test_db().await;
    handle_create_table(
        db.clone(),
        json!({"TableName": "PutOldImage", "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    handle_put_item(
        db.clone(),
        json!({"TableName": "PutOldImage", "Item": {"id": {"S": "key"}, "value": {"S": "before"}}})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let response = handle_put_item(
        db,
        json!({"TableName": "PutOldImage", "Item": {"id": {"S": "key"}, "value": {"S": "after"}}, "ReturnValues": "ALL_OLD"})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let Response::PutItem(response) = response else {
        panic!("expected PutItem response");
    };
    assert_eq!(
        response
            .attributes
            .and_then(|item| item.get("value").cloned()),
        Some(AttributeValue::S("before".to_string()))
    );
}

#[tokio::test]
async fn given_customer_timestamp_named_attributes_when_put_item_runs_then_values_are_preserved() {
    for_each_conformance_backend(|_backend, db| async move {
        handle_create_table(
            db.clone(),
            json!({
                "TableName": "CustomerAttributes",
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
            })
            .try_into()
            .unwrap(),
        )
        .await
        .unwrap();
        let expected = json!({
            "id": {"S": "item"},
            "u_at": {"S": "customer-owned"},
            "updated_at": {"M": {"origin": {"S": "customer"}}},
            "c_at": {"N": "100"},
            "created_at": {"N": "200"}
        });
        handle_put_item(
            db.clone(),
            json!({"TableName": "CustomerAttributes", "Item": expected.clone()})
                .try_into()
                .unwrap(),
        )
        .await
        .unwrap();
        let response = handle_get_item(
            db,
            json!({"TableName": "CustomerAttributes", "Key": {"id": {"S": "item"}}})
                .try_into()
                .unwrap(),
        )
        .await
        .unwrap();
        let stored = expect_get_item_response(response).item.unwrap();
        assert_eq!(
            serde_json::to_value(stored.into_attribute_map().unwrap()).unwrap(),
            expected
        );
    })
    .await;
}

fn expect_get_item_response(response: Response) -> storage_types::GetItemResponse {
    match response {
        Response::GetItem(response) => response,
        other => panic!("Expected GetItem response, got: {other:?}"),
    }
}
