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
async fn given_missing_table_and_invalid_item_when_put_runs_then_table_error_wins() {
    let db = create_sqlite_test_db().await;
    let error = handle_put_item(
        db,
        json!({"TableName": "MissingPutTable", "Item": {"other": {"S": "x"}}})
            .try_into()
            .unwrap(),
    )
    .await
    .expect_err("missing table must fail before item validation");
    assert_eq!(error.status_code, 400);
    assert!(error.message.contains("MissingPutTable"), "{error:?}");
}

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

#[tokio::test]
async fn put_item_with_special_characters() {
    for_each_conformance_backend(|_backend, db| async move {
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

        let item = storage_types::AttributeMap::try_from(response.item.unwrap()).unwrap();

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
    })
    .await;
}

#[tokio::test]
async fn put_item_with_pipe_character() {
    for_each_conformance_backend(|_backend, db| async move {
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

        let item = storage_types::AttributeMap::try_from(response.item.unwrap()).unwrap();
        assert_eq!(
            item.get("text"),
            Some(&AttributeValue::S("Contains | pipe character".to_string()))
        );
        assert_eq!(
            item.get("more_text"),
            Some(&AttributeValue::S("Multiple | pipes | here".to_string()))
        );
    })
    .await;
}

#[tokio::test]
async fn put_item_accepts_aux_item_stream_ttl_without_storing_attribute() {
    for_each_conformance_backend(|_backend, db| async move {
        handle_create_table(
            db.clone(),
            json!({
                "TableName": "RoutePutItemDuration",
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ]
            })
            .try_into()
            .expect("valid create table request"),
        )
        .await
        .expect("create table");

        handle_put_item(
            db.clone(),
            json!({
                "TableName": "RoutePutItemDuration",
                "Item": {
                    "id": {"S": "item1"},
                    "value": {"S": "stored"}
                },
                "AuxItemStreamTtlHours": 120
            })
            .try_into()
            .expect("valid put item request"),
        )
        .await
        .expect("put item with custom item stream ttl");

        let item = expect_get_item_response(
            handle_get_item(
                db,
                json!({
                    "TableName": "RoutePutItemDuration",
                    "Key": {"id": {"S": "item1"}},
                    "ConsistentRead": true
                })
                .try_into()
                .expect("valid get item request"),
            )
            .await
            .expect("get item"),
        )
        .item
        .expect("item");
        let item = storage_types::AttributeMap::try_from(item).expect("decode item");
        assert_eq!(
            item.get("value"),
            Some(&AttributeValue::S("stored".to_string()))
        );
        assert!(
            !item.contains_key("AuxItemStreamTtlHours"),
            "aux item stream ttl is request metadata, not item data"
        );
    })
    .await;
}

#[tokio::test]
async fn put_item_condition_failure_returns_dynamodb_error_code() {
    for_each_conformance_backend(|_backend, db| async move {
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
    })
    .await;
}

#[tokio::test]
async fn put_item_condition_failure_returns_all_old_item_when_requested() {
    for_each_conformance_backend(|_backend, db| async move {
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
    })
    .await;
}

fn expect_get_item_response(response: Response) -> storage_types::GetItemResponse {
    match response {
        Response::GetItem(response) => response,
        other => panic!("Expected GetItem response, got: {other:?}"),
    }
}
