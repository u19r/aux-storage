#[cfg(test)]
use serde_json::json;
use storage::DatabaseManager;
use storage_types::{CreateTableRequest, StreamViewType, TableName, TableStatus};

use crate::{
    routes::routes_support_tests::{create_test_db, handle_create_table},
    types::Response,
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn handle_create_table_success() {
    let db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "ValidTable",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort_key", "AttributeType": "N"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort_key", "KeyType": "RANGE"}
        ]
    });

    let result = handle_create_table(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok(), "CreateTable should succeed: {result:?}");

    match result.unwrap() {
        Response::CreateTable(response) => {
            assert_eq!(
                response.table_description.table_name,
                TableName::new("ValidTable")
            );
            assert_eq!(response.table_description.table_status, TableStatus::Active);
            assert_eq!(
                response.table_description.table_arn,
                "arn:aws:dynamodb:us-east-1:123456789012:table/ValidTable"
            );
            assert_eq!(response.table_description.attribute_definitions.len(), 2);
            assert_eq!(response.table_description.key_schema.len(), 2);
        }
        other => panic!("Expected CreateTable response, got: {other:?}"),
    }
}

#[tokio::test]
async fn handle_create_table_duplicate() {
    let db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "DuplicateTable",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let result1 = handle_create_table(db.clone(), payload.clone().try_into().unwrap()).await;
    assert!(
        result1.is_ok(),
        "First CreateTable should succeed: {result1:?}"
    );

    let result2 = handle_create_table(db.clone(), payload.try_into().unwrap()).await;
    assert!(result2.is_err(), "Second CreateTable should fail");

    match result2 {
        Err(handler_error) => {
            assert_eq!(handler_error.status_code, 400);
            assert_eq!(
                handler_error.error_type,
                "com.amazonaws.dynamodb.v20120810#ResourceInUseException"
            );
            assert_eq!(
                handler_error.message,
                "Table already exists: DuplicateTable"
            );
        }
        Ok(_) => panic!("Expected validation error"),
    }
}

#[tokio::test]
async fn handle_create_table_empty_table_name() {
    let _db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let request_result: Result<CreateTableRequest, String> = payload.try_into();
    assert!(
        request_result.is_err(),
        "CreateTable with empty name should fail during validation"
    );

    match request_result {
        Err(error) => {
            assert!(error.contains("TableName cannot be empty"));
        }
        Ok(_) => panic!("Expected validation error"),
    }
}

#[tokio::test]
async fn handle_create_table_short_table_name() {
    let _db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "ab",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let request_result: Result<CreateTableRequest, String> = payload.try_into();
    assert!(
        request_result.is_err(),
        "CreateTable with short name should fail"
    );

    match request_result {
        Err(error) => {
            assert!(error.contains("TableName must be between 3 and 255 characters"));
        }
        Ok(_) => panic!("Expected validation error"),
    }
}

#[tokio::test]
async fn handle_create_table_invalid_table_name_characters() {
    let _db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "Invalid@Table",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let request_result: Result<CreateTableRequest, String> = payload.try_into();
    assert!(
        request_result.is_err(),
        "CreateTable with invalid characters should fail"
    );

    match request_result {
        Err(error) => {
            assert!(error.contains("TableName contains invalid characters"));
        }
        Ok(_) => panic!("Expected validation error"),
    }
}

#[tokio::test]
async fn handle_create_table_invalid_json() {
    let _db = create_test_db_manager().await;

    let invalid_payload = json!({
        "TableName": "ValidTable",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ]
    });

    let request_result: Result<CreateTableRequest, String> = invalid_payload.try_into();
    assert!(
        request_result.is_err(),
        "CreateTable with missing KeySchema should fail"
    );

    match request_result {
        Err(error) => {
            assert!(error.contains("Invalid request format"));
        }
        Ok(_) => panic!("Expected validation error"),
    }
}

#[tokio::test]
async fn handle_create_table_creation_datetime_unix_timestamp() {
    let db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "TimeTestTable",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ]
    });

    let result = handle_create_table(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok(), "CreateTable should succeed: {result:?}");

    match result.unwrap() {
        Response::CreateTable(response) => {
            // Serialize the response to JSON to check the format
            let json_response = serde_json::to_value(&response).expect("Should serialize to JSON");
            let table_desc = json_response
                .get("TableDescription")
                .expect("Should have TableDescription");
            let creation_time = table_desc
                .get("CreationDateTime")
                .expect("Should have CreationDateTime");
            assert!(
                creation_time.is_number(),
                "CreationDateTime should be a number (Unix timestamp), got: {creation_time:?}"
            );
            if let Some(timestamp) = creation_time.as_f64() {
                #[expect(clippy::cast_precision_loss)]
                let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
                assert!(
                    timestamp > now - 60.0,
                    "Timestamp should be recent (within last minute)"
                );
                assert!(
                    timestamp <= now + 1.0,
                    "Timestamp should not be in the future"
                );
            } else {
                panic!("CreationDateTime should be a valid number");
            }
        }
        other => panic!("Expected CreateTable response, got: {other:?}"),
    }
}

#[tokio::test]
async fn handle_create_table_with_stream_specification() {
    let db = create_test_db_manager().await;

    let payload = json!({
        "TableName": "StreamTable",
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ],
        "StreamSpecification": {
            "StreamEnabled": true,
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        }
    });

    let result = handle_create_table(db.clone(), payload.try_into().unwrap()).await;
    assert!(
        result.is_ok(),
        "CreateTable with stream should succeed: {result:?}"
    );

    match result.unwrap() {
        Response::CreateTable(response) => {
            assert_eq!(
                response.table_description.table_name,
                TableName::new("StreamTable")
            );
            assert_eq!(response.table_description.table_status, TableStatus::Active);

            let stream_spec = response
                .table_description
                .stream_specification
                .expect("Should have stream specification");
            assert!(stream_spec.stream_enabled);
            assert_eq!(
                stream_spec.stream_view_type.unwrap(),
                StreamViewType::NewAndOldImages
            );
            let stream_label = response
                .table_description
                .latest_stream_label
                .as_deref()
                .expect("Should have latest stream label");
            let stream_arn = response
                .table_description
                .latest_stream_arn
                .as_deref()
                .expect("Should have latest stream ARN");
            assert_eq!(
                stream_arn,
                format!(
                    "arn:aws:dynamodb:us-east-1:123456789012:table/StreamTable/stream/\
                     {stream_label}"
                )
            );
        }
        other => panic!("Expected CreateTable response, got: {other:?}"),
    }
}
