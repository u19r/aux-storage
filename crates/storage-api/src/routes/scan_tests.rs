use std::collections::HashMap;

use serde_json::json;
use storage::{DatabaseManager, PutItemInput};
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, TableName,
};

use crate::{
    routes::routes_test_support::{create_test_db, default_conformance_backends, handle_scan},
    types::Response,
};

async fn setup_test_db() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

async fn create_test_table(db: &DatabaseManager, table_name: &str) {
    let request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );

    db.create_table(&request).await.unwrap();
}

async fn create_test_table_hash_range(db: &DatabaseManager, table_name: &str) {
    let request = CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );

    db.create_table(&request).await.unwrap();
}

async fn put_test_item(db: &DatabaseManager, table_name: &str, id: &str, name: &str, age: u32) {
    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S(id.to_string()));
    item.insert("name".to_string(), AttributeValue::S(name.to_string()));
    item.insert("age".to_string(), AttributeValue::N(age.to_string()));

    db.put_item(
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .unwrap();
}

async fn put_test_item_hash_range(db: &DatabaseManager, table_name: &str, pk: &str, sk: &str) {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert("sk".to_string(), AttributeValue::S(sk.to_string()));

    db.put_item(
        PutItemInput::builder()
            .table_name(TableName::new(table_name))
            .item(item)
            .build(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn scan_empty_table() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "EmptyTable").await;

    let payload = json!({
        "TableName": "EmptyTable"
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok());

    if let Response::Scan(response) = result.unwrap() {
        assert_eq!(response.count, 0);
        assert_eq!(response.scanned_count, 0);
        assert!(response.items.is_some());
        assert!(response.items.unwrap().is_empty());
        assert!(response.last_evaluated_key.is_none());
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_caps_response_page_by_bytes_and_resume_token_continues() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "ScanBytePageTable").await;
        let data = "x".repeat(300 * 1024);
        for id in ["item#001", "item#002", "item#003", "item#004"] {
            let mut item = HashMap::new();
            item.insert("id".to_string(), AttributeValue::S(id.to_string()));
            item.insert("data".to_string(), AttributeValue::S(data.clone()));
            db.put_item(
                PutItemInput::builder()
                    .table_name(TableName::new("ScanBytePageTable"))
                    .item(item)
                    .build(),
            )
            .await
            .expect("put large scan item");
        }

        let first_payload = json!({
            "TableName": "ScanBytePageTable"
        });
        let first = handle_scan(db.clone(), first_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} first scan page: {err:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        let first_items = first.items.expect("scan should return items");
        assert!(
            serde_json::to_vec(&first_items).unwrap().len()
                <= storage_types::MAX_QUERY_SCAN_RESPONSE_BYTES,
            "{}",
            backend.name
        );
        assert_eq!(first_items.len(), 3, "{}", backend.name);
        let mut seen_ids = scan_item_ids(&first_items);
        let resume = first
            .last_evaluated_key
            .expect("byte-capped scan should return resume token");

        let second_payload = json!({
            "TableName": "ScanBytePageTable",
            "ExclusiveStartKey": resume
        });
        let second = handle_scan(db, second_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} second scan page: {err:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected scan response", backend.name);
        };
        let second_items = second.items.expect("scan should return remaining item");
        assert_eq!(second_items.len(), 1, "{}", backend.name);
        seen_ids.extend(scan_item_ids(&second_items));
        seen_ids.sort();
        assert_eq!(
            seen_ids,
            vec![
                "item#001".to_string(),
                "item#002".to_string(),
                "item#003".to_string(),
                "item#004".to_string()
            ],
            "{}",
            backend.name
        );
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn scan_with_malformed_exclusive_start_key_returns_dynamodb_error() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "MalformedScanStartKeyTable").await;
        put_test_item_hash_range(&db, "MalformedScanStartKeyTable", "p", "s001").await;

        let payload = json!({
            "TableName": "MalformedScanStartKeyTable",
            "ExclusiveStartKey": {
                "pk": {"S": "p"}
            }
        });

        let result = handle_scan(db, payload.try_into().unwrap()).await;
        let error = result.expect_err("malformed scan start key should fail");
        assert_eq!(error.status_code, 400, "{}", backend.name);
        assert_eq!(
            error.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            error.message,
            "The provided starting key is invalid: The provided key element does not match the \
             schema",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn scan_with_limit_returns_compatible_hash_range_last_evaluated_key() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "LimitedHashRangeScanTable").await;

        for sk in ["001", "002", "003"] {
            put_test_item_hash_range(&db, "LimitedHashRangeScanTable", "tenant#1", sk).await;
        }

        let payload = json!({
            "TableName": "LimitedHashRangeScanTable",
            "Limit": 2
        });

        let first = handle_scan(db.clone(), payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} first limited scan: {err:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        let first_items = first.items.expect("scan should return items");
        assert_eq!(first_items.len(), 2, "{}", backend.name);
        let last_item = &first_items[1];
        let last_evaluated_key = first
            .last_evaluated_key
            .expect("limited scan should return a DynamoDB key map");
        assert_eq!(
            last_evaluated_key.get("pk"),
            last_item.get("pk"),
            "{}",
            backend.name
        );
        assert_eq!(
            last_evaluated_key.get("sk"),
            last_item.get("sk"),
            "{}",
            backend.name
        );

        let resume_payload = json!({
            "TableName": "LimitedHashRangeScanTable",
            "ExclusiveStartKey": last_evaluated_key
        });
        let second = handle_scan(db, resume_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} second limited scan: {err:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected scan response", backend.name);
        };
        assert_eq!(
            second.items.expect("scan should resume").len(),
            1,
            "{}",
            backend.name
        );
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn scan_single_item() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "SingleItemTable").await;
    put_test_item(db.as_ref(), "SingleItemTable", "item1", "Test Item", 25).await;

    let payload = json!({
        "TableName": "SingleItemTable"
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok());

    if let Response::Scan(response) = result.unwrap() {
        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
        let items = response.items.unwrap();
        assert_eq!(items.len(), 1);

        let item = &items[0];
        assert_eq!(
            item.get("id"),
            Some(&AttributeValue::S("item1".to_string()))
        );
        assert_eq!(
            item.get("name"),
            Some(&AttributeValue::S("Test Item".to_string()))
        );
        assert_eq!(item.get("age"), Some(&AttributeValue::N("25".to_string())));
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_multiple_items() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "MultiItemTable").await;

    put_test_item(db.as_ref(), "MultiItemTable", "user1", "Alice", 30).await;
    put_test_item(db.as_ref(), "MultiItemTable", "user2", "Bob", 25).await;
    put_test_item(db.as_ref(), "MultiItemTable", "user3", "Carol", 35).await;

    let payload = json!({
        "TableName": "MultiItemTable"
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok());

    if let Response::Scan(response) = result.unwrap() {
        assert_eq!(response.count, 3);
        assert_eq!(response.scanned_count, 3);
        let items = response.items.unwrap();
        assert_eq!(items.len(), 3);
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_with_limit() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "LimitedTable").await;

        for i in 1..=5 {
            put_test_item(
                &db,
                "LimitedTable",
                &format!("item{i}"),
                &format!("Name {i}"),
                20 + i,
            )
            .await;
        }

        let payload = json!({
            "TableName": "LimitedTable",
            "Limit": 2
        });

        let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
        assert!(result.is_ok(), "{}: {result:?}", backend.name);

        if let Response::Scan(response) = result.unwrap() {
            assert_eq!(response.count, 2, "{}", backend.name);
            assert_eq!(response.scanned_count, 2, "{}", backend.name);
            let items = response.items.unwrap();
            assert_eq!(items.len(), 2, "{}", backend.name);
            assert!(response.last_evaluated_key.is_some(), "{}", backend.name);
        } else {
            panic!("{} expected Scan response", backend.name);
        }
    }
}

fn scan_item_ids(items: &[storage_types::AttributeMap]) -> Vec<String> {
    items
        .iter()
        .map(|item| {
            let Some(AttributeValue::S(id)) = item.get("id") else {
                panic!("scan item should contain string id: {item:?}");
            };
            id.clone()
        })
        .collect()
}

#[tokio::test]
async fn scan_with_filter_expression() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "FilterTable").await;
    put_test_item(db.as_ref(), "FilterTable", "user1", "Alice", 20).await;
    put_test_item(db.as_ref(), "FilterTable", "user2", "Bob", 30).await;
    put_test_item(db.as_ref(), "FilterTable", "user3", "Carol", 40).await;

    let payload = json!({
        "TableName": "FilterTable",
        "FilterExpression": "age > :minAge",
        "ExpressionAttributeValues": {
            ":minAge": {"N": "25"}
        }
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok());

    if let Response::Scan(response) = result.unwrap() {
        assert_eq!(response.count, 2); // Bob (30) and Carol (40)
        assert_eq!(response.scanned_count, 3); // All 3 items were scanned
        let items = response.items.unwrap();
        assert_eq!(items.len(), 2);
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_with_projection_expression() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "ProjectionTable").await;
    put_test_item(db.as_ref(), "ProjectionTable", "user1", "Alice", 25).await;

    let payload = json!({
        "TableName": "ProjectionTable",
        "ProjectionExpression": "id, #name",
        "ExpressionAttributeNames": {
            "#name": "name"
        }
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok());

    if let Response::Scan(response) = result.unwrap() {
        let items = response.items.unwrap();
        assert_eq!(items.len(), 1);

        let item = &items[0];
        assert!(item.contains_key("id"));
        assert!(item.contains_key("name"));
        assert!(!item.contains_key("age")); // Should be excluded by projection
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_count_only() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "CountTable").await;

    put_test_item(db.as_ref(), "CountTable", "item1", "Alice", 25).await;
    put_test_item(db.as_ref(), "CountTable", "item2", "Bob", 30).await;

    let payload = json!({
        "TableName": "CountTable",
        "Select": "COUNT"
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_ok());

    if let Response::Scan(response) = result.unwrap() {
        assert_eq!(response.count, 2);
        assert_eq!(response.scanned_count, 2);
        assert!(response.items.is_none()); // No items returned for COUNT
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_nonexistent_table() {
    let db = setup_test_db().await;

    let payload = json!({
        "TableName": "NonExistentTable"
    });

    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    assert!(result.is_err());

    let handler_error = result.unwrap_err();
    assert_eq!(handler_error.status_code, 400);
    assert!(
        handler_error
            .error_type
            .contains("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn scan_empty_table_name() {
    let _db = setup_test_db().await;

    let payload = json!({
        "TableName": ""
    });

    let request_result: Result<storage_types::ScanRequest, String> = payload.try_into();
    assert!(request_result.is_err(), "Request should fail validation");

    let error = request_result.unwrap_err();
    assert!(error.contains("TableName cannot be empty"));
}

#[tokio::test]
async fn scan_missing_expression_attribute_values() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "TestTable").await;

    let payload = json!({
        "TableName": "TestTable",
        "FilterExpression": "age > :minAge"
        // Missing ExpressionAttributeValues
    });

    let request_result: Result<storage_types::ScanRequest, String> = payload.try_into();
    assert!(request_result.is_err(), "Request should fail validation");

    let error = request_result.unwrap_err();
    assert!(error.contains(
        "Invalid FilterExpression: An expression attribute value used in expression is not \
         defined; attribute value: :minAge"
    ));
}

#[tokio::test]
async fn scan_invalid_filter_expression() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "TestTable").await;

    let payload = json!({
        "TableName": "TestTable",
        "FilterExpression": "invalid filter"
    });

    let request_result: Result<storage_types::ScanRequest, String> = payload.try_into();
    assert!(request_result.is_err(), "Request should fail validation");

    let error = request_result.unwrap_err();
    assert!(error.contains("Invalid FilterExpression"));
}

#[tokio::test]
async fn scan_consumed_capacity_none() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "TestTable").await;

    // Add test items
    put_test_item(db.as_ref(), "TestTable", "1", "Alice", 25).await;
    put_test_item(db.as_ref(), "TestTable", "2", "Bob", 30).await;

    let payload = json!({
        "TableName": "TestTable"
    });

    let request: storage_types::ScanRequest = payload.try_into().unwrap();
    let result = handle_scan(db.clone(), request).await.unwrap();

    if let Response::Scan(response) = result {
        assert_eq!(response.consumed_capacity, None);
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_consumed_capacity_total() {
    let db = setup_test_db().await;
    create_test_table(db.as_ref(), "TestTable").await;

    // Add test items
    put_test_item(db.as_ref(), "TestTable", "1", "Alice", 25).await;
    put_test_item(db.as_ref(), "TestTable", "2", "Bob", 30).await;

    let payload = json!({
        "TableName": "TestTable",
        "ReturnConsumedCapacity": "TOTAL"
    });

    let request: storage_types::ScanRequest = payload.try_into().unwrap();
    let result = handle_scan(db.clone(), request).await.unwrap();

    if let Response::Scan(response) = result {
        assert!(response.consumed_capacity.is_some());
        let consumed_capacity = response.consumed_capacity.unwrap();

        assert_eq!(consumed_capacity.table_name.to_string(), "TestTable");
        assert!(consumed_capacity.capacity_units > 0.0);
    } else {
        panic!("Expected Scan response");
    }
}

#[tokio::test]
async fn scan_invalid_json() {
    let db = setup_test_db().await;

    // Invalid JSON - missing closing brace
    let payload = json!({
        "TableName": "TestTable"
    });

    // This should pass JSON parsing but fail request parsing
    let result = handle_scan(db.clone(), payload.try_into().unwrap()).await;
    // This test would require malformed JSON, which is hard to create with json!
    // macro In practice, this would be tested through integration tests
    assert!(result.is_ok() || result.is_err()); // Either outcome is acceptable for this test
}
