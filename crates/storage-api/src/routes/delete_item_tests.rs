use serde_json::json;

use crate::{
    routes::routes_tests::{create_sqlite_test_db, handle_create_table, handle_delete_item},
    types::Response,
};

#[tokio::test]
async fn given_missing_table_and_key_when_delete_runs_then_table_error_wins() {
    let error = handle_delete_item(
        create_sqlite_test_db().await,
        json!({"TableName": "MissingDeleteTable", "Key": {"other": {"S": "x"}}})
            .try_into()
            .unwrap(),
    )
    .await
    .expect_err("missing table must fail first");
    assert_eq!(error.status_code, 400);
    assert_eq!(error.message, "Requested resource not found");
}

#[tokio::test]
async fn given_missing_key_when_delete_runs_then_generic_validation_error_is_preserved() {
    let db = create_sqlite_test_db().await;
    handle_create_table(
        db.clone(),
        json!({"TableName": "DeleteErrorOrder", "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let error = handle_delete_item(
        db,
        json!({"TableName": "DeleteErrorOrder", "Key": {"other": {"S": "x"}}})
            .try_into()
            .unwrap(),
    )
    .await
    .expect_err("missing key must fail");
    assert_eq!(error.message, "One or more parameter values were invalid");
    assert_eq!(
        error.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
}

#[tokio::test]
async fn given_absent_item_when_delete_requests_all_old_then_response_is_empty() {
    let db = create_sqlite_test_db().await;
    handle_create_table(
        db.clone(),
        json!({"TableName": "DeleteAbsentOld", "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let response = handle_delete_item(
        db,
        json!({"TableName": "DeleteAbsentOld", "Key": {"id": {"S": "missing"}}, "ReturnValues": "ALL_OLD"})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let Response::DeleteItem(response) = response else {
        panic!("expected DeleteItem response");
    };
    assert!(response.attributes.is_none());
}
