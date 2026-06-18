use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateGlobalSecondaryIndex, CreateTableRequest, IndexName,
    ItemKey, KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType,
    QueryTableRequest, ScanTableRequest, TableName, TableStatus,
};

use crate::{
    keyspace::table_keys,
    kv_support_tests::{TestProvider, create_test_provider as make_test_provider},
    sorted_kv_store::SortedKvStore,
};

fn create_test_provider() -> TestProvider {
    make_test_provider()
}

fn unique_table_name(prefix: &str) -> TableName {
    TableName::new(&format!("{}_{}", prefix, uuid::Uuid::new_v4()))
}

#[tokio::test]
async fn provider_creation() {
    let provider = create_test_provider();
    let result = provider.initialize_storage().await;
    assert!(result.is_ok(), "Initialize should succeed: {result:?}");
}

#[tokio::test]
async fn create_table() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    let table_name = unique_table_name("test_table");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "id".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "name".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );

    let result = provider.create_table(&request).await;
    assert!(result.is_ok(), "Create table should succeed: {result:?}");
    let table_exists = provider.table_exists(&table_name).await.unwrap();
    assert!(table_exists, "Table should exist after creation");
    let table_info = provider.get_table_info(&table_name).await.unwrap();
    assert_eq!(table_info.table_name, table_name);
    assert_eq!(table_info.table_status, TableStatus::Active);
    assert_eq!(table_info.attribute_definitions.len(), 2);
    assert_eq!(table_info.key_schema.len(), 1);
}

#[expect(clippy::similar_names)]
#[tokio::test]
async fn list_tables() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");
    let table1 = unique_table_name("table1");
    let table2 = unique_table_name("table2");

    let request1 = CreateTableRequest::new(
        table1.clone(),
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

    let request2 = CreateTableRequest::new(
        table2.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::N,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );

    provider.create_table(&request1).await.unwrap();
    provider.create_table(&request2).await.unwrap();

    // List tables
    let tables = provider.list_tables(100, None).await.unwrap();
    assert!(tables.len() >= 2, "Should have at least 2 tables");

    let table_names: Vec<TableName> = tables.iter().map(|t| t.table_name.clone()).collect();
    assert!(table_names.contains(&table1));
    assert!(table_names.contains(&table2));
}

#[tokio::test]
async fn delete_table() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    let table_name = unique_table_name("test_delete");
    let request = CreateTableRequest::new(
        table_name.clone(),
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

    provider.create_table(&request).await.unwrap();
    assert!(provider.table_exists(&table_name).await.unwrap());
    provider.delete_table(&table_name).await.unwrap();
    assert!(!provider.table_exists(&table_name).await.unwrap());
}

#[tokio::test]
async fn put_and_get_item() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    let table_name = unique_table_name("items_test");
    let request = CreateTableRequest::new(
        table_name.clone(),
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

    provider.create_table(&request).await.unwrap();
    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("test123".to_string()));
    item.insert(
        "name".to_string(),
        AttributeValue::S("Test Item".to_string()),
    );
    item.insert("count".to_string(), AttributeValue::N("42".to_string()));

    let put_result = provider
        .put_item(table_name.clone(), item.clone(), None, None, None, None)
        .await;
    assert!(put_result.is_ok(), "PutItem should succeed: {put_result:?}");
    let mut key = HashMap::new();
    key.insert("id".to_string(), AttributeValue::S("test123".to_string()));

    let get_result = provider.get_item_map(table_name, key.into(), true).await;
    assert!(get_result.is_ok(), "GetItem should succeed: {get_result:?}");

    let retrieved_item = get_result.unwrap();
    assert!(retrieved_item.is_some(), "GetItem should return the item");

    let retrieved_item = retrieved_item.unwrap();
    assert_eq!(retrieved_item.len(), 3, "Should have 3 attributes");
    assert_eq!(
        retrieved_item.get("id"),
        Some(&AttributeValue::S("test123".to_string()))
    );
    assert_eq!(
        retrieved_item.get("name"),
        Some(&AttributeValue::S("Test Item".to_string()))
    );
    assert_eq!(
        retrieved_item.get("count"),
        Some(&AttributeValue::N("42".to_string()))
    );
}

#[tokio::test]
async fn delete_item() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    let table_name = unique_table_name("delete_items_test");
    let request = CreateTableRequest::new(
        table_name.clone(),
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

    provider.create_table(&request).await.unwrap();
    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("delete_me".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("Important data".to_string()),
    );

    provider
        .put_item(table_name.clone(), item.clone(), None, None, None, None)
        .await
        .unwrap();
    let mut key = HashMap::new();
    key.insert("id".to_string(), AttributeValue::S("delete_me".to_string()));

    let get_result = provider
        .get_item_map(table_name.clone(), key.clone().into(), true)
        .await
        .unwrap();
    assert!(get_result.is_some(), "Item should exist before deletion");
    let delete_result = provider
        .delete_item(table_name.clone(), key.clone().into(), None, None, None)
        .await
        .unwrap();
    assert!(
        delete_result.is_some(),
        "Delete should return the deleted item"
    );

    let deleted_item = delete_result.unwrap();
    assert_eq!(
        deleted_item.get("id"),
        Some(&AttributeValue::S("delete_me".to_string()))
    );
    assert_eq!(
        deleted_item.get("data"),
        Some(&AttributeValue::S("Important data".to_string()))
    );
    let get_after_delete = provider
        .get_item_map(table_name, key.into(), true)
        .await
        .unwrap();
    assert!(
        get_after_delete.is_none(),
        "Item should not exist after deletion"
    );
}

#[tokio::test]
async fn scan_table() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    let table_name = unique_table_name("scan_test");
    let request = CreateTableRequest::new(
        table_name.clone(),
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

    provider.create_table(&request).await.unwrap();
    for i in 1..=5 {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S(format!("item{i}")));
        item.insert("data".to_string(), AttributeValue::S(format!("Data {i}")));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Scan the table
    let scan_request = ScanTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();

    assert_eq!(items.len(), 5, "Should return all 5 items");
    assert!(
        last_evaluated_key.is_none(),
        "Should not have pagination for this test, found {last_evaluated_key:?}"
    );
    let mut found_ids = Vec::new();
    for item in &items {
        if let Some(AttributeValue::S(id)) = item.get("id") {
            found_ids.push(id.clone());
        }
    }
    found_ids.sort();

    let expected_ids: Vec<String> = (1..=5).map(|i| format!("item{i}")).collect();
    let mut expected_sorted = expected_ids.clone();
    expected_sorted.sort();

    for expected_id in expected_sorted {
        assert!(
            found_ids.contains(&expected_id),
            "Should find item {expected_id}"
        );
    }
}

#[tokio::test]
async fn query_between() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");
    let table_name = unique_table_name("TimestampTable");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "timestamp".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "timestamp".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );

    provider.create_table(&request).await.unwrap();
    let mut item1 = HashMap::new();
    item1.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item1.insert(
        "timestamp".to_string(),
        AttributeValue::N("100".to_string()),
    );
    item1.insert("event".to_string(), AttributeValue::S("Login".to_string()));
    provider
        .put_item(table_name.clone(), item1, None, None, None, None)
        .await
        .unwrap();

    let mut item2 = HashMap::new();
    item2.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item2.insert("timestamp".to_string(), AttributeValue::N("99".to_string()));
    item2.insert(
        "event".to_string(),
        AttributeValue::S("View Page".to_string()),
    );
    provider
        .put_item(table_name.clone(), item2, None, None, None, None)
        .await
        .unwrap();

    let mut item3 = HashMap::new();
    item3.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item3.insert(
        "timestamp".to_string(),
        AttributeValue::N("3000".to_string()),
    );
    item3.insert(
        "event".to_string(),
        AttributeValue::S("Purchase".to_string()),
    );
    provider
        .put_item(table_name.clone(), item3, None, None, None, None)
        .await
        .unwrap();

    let mut item4 = HashMap::new();
    item4.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item4.insert(
        "timestamp".to_string(),
        AttributeValue::N("100000".to_string()),
    );
    item4.insert("event".to_string(), AttributeValue::S("Logout".to_string()));
    provider
        .put_item(table_name.clone(), item4, None, None, None, None)
        .await
        .unwrap();

    // Query with BETWEEN condition
    let mut expression_values = HashMap::new();
    expression_values.insert(":pk_val".to_string(), AttributeValue::S("U#1".to_string()));
    expression_values.insert(":start".to_string(), AttributeValue::N("100".to_string()));
    expression_values.insert(":end".to_string(), AttributeValue::N("3500".to_string()));

    // Note: The actual query uses ExpressionAttributeNames, but for the unit test
    // we'll use the simplified form without attribute name substitution
    let query_request = QueryTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        key_condition_expression: "pk = :pk_val AND timestamp BETWEEN :start AND :end".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(expression_values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    let (items, _) = provider.query_table(&query_request).await.unwrap();

    // Should return only 2 items (timestamp 100 and 3000), not 4
    assert_eq!(
        items.len(),
        2,
        "Should return only items with timestamp between 100 and 3500"
    );
    let mut returned_timestamps = Vec::new();
    for item in &items {
        if let Some(AttributeValue::N(timestamp)) = item.get("timestamp") {
            returned_timestamps.push(timestamp.clone());
        }
    }
    returned_timestamps.sort();

    assert_eq!(returned_timestamps, vec!["100", "3000"]);
}

#[tokio::test]
async fn query_less_than() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");
    let table_name = unique_table_name("TimestampTable");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "timestamp".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "timestamp".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );

    provider.create_table(&request).await.unwrap();
    let mut item1 = HashMap::new();
    item1.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item1.insert(
        "timestamp".to_string(),
        AttributeValue::N("1000".to_string()),
    );
    item1.insert("event".to_string(), AttributeValue::S("Login".to_string()));
    provider
        .put_item(table_name.clone(), item1, None, None, None, None)
        .await
        .unwrap();

    let mut item2 = HashMap::new();
    item2.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item2.insert(
        "timestamp".to_string(),
        AttributeValue::N("2000".to_string()),
    );
    item2.insert(
        "event".to_string(),
        AttributeValue::S("Purchase".to_string()),
    );
    provider
        .put_item(table_name.clone(), item2, None, None, None, None)
        .await
        .unwrap();

    let mut item3 = HashMap::new();
    item3.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
    item3.insert(
        "timestamp".to_string(),
        AttributeValue::N("3000".to_string()),
    );
    item3.insert("event".to_string(), AttributeValue::S("Logout".to_string()));
    provider
        .put_item(table_name.clone(), item3, None, None, None, None)
        .await
        .unwrap();

    // Query with less than condition
    let mut expression_values = HashMap::new();
    expression_values.insert(":pk_val".to_string(), AttributeValue::S("U#1".to_string()));
    expression_values.insert(":max_ts".to_string(), AttributeValue::N("2500".to_string()));

    // Note: The actual query uses ExpressionAttributeNames, but for the unit test
    // we'll use the simplified form without attribute name substitution
    let query_request = QueryTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        key_condition_expression: "pk = :pk_val AND timestamp < :max_ts".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(expression_values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    let (items, _) = provider.query_table(&query_request).await.unwrap();

    // Should return only 2 items (timestamp 1000 and 2000), not 3
    assert_eq!(
        items.len(),
        2,
        "Should return only items with timestamp < 2500"
    );
    let mut returned_timestamps = Vec::new();
    for item in &items {
        if let Some(AttributeValue::N(timestamp)) = item.get("timestamp") {
            returned_timestamps.push(timestamp.clone());
        }
    }
    returned_timestamps.sort();

    assert_eq!(returned_timestamps, vec!["1000", "2000"]);
}

#[tokio::test]
async fn query_pagination() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");
    let table_name = unique_table_name("HashRangeTable");
    let request = CreateTableRequest::new(
        table_name.clone(),
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

    provider.create_table(&request).await.unwrap();
    for i in 1..=4 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
        item.insert("sk".to_string(), AttributeValue::S(format!("item#{i:03}")));
        item.insert("data".to_string(), AttributeValue::S(format!("Data {i}")));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Query with limit of 2 (should trigger pagination)
    let mut expression_values = HashMap::new();
    expression_values.insert(":pk_val".to_string(), AttributeValue::S("U#1".to_string()));

    let query_request = QueryTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        key_condition_expression: "pk = :pk_val".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(expression_values),
        limit: Some(2), // Limit to 2 items
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();

    // Should return only 2 items due to limit
    assert_eq!(items.len(), 2, "Should return only 2 items due to limit");

    // Should return LastEvaluatedKey for pagination since there are more items
    assert!(
        last_evaluated_key.is_some(),
        "Should return LastEvaluatedKey for pagination"
    );
    let mut returned_sks = Vec::new();
    for item in &items {
        if let Some(AttributeValue::S(sk)) = item.get("sk") {
            returned_sks.push(sk.clone());
        }
    }
    returned_sks.sort();

    assert_eq!(returned_sks, vec!["item#001", "item#002"]);
}

#[tokio::test]
async fn query_scan_index_forward_false() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table_name = unique_table_name("OrderedTable");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
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

    provider.create_table(&request).await.unwrap();
    let items = vec![("100", "First"), ("200", "Second"), ("300", "Third")];

    for (sk_val, data_val) in items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("U#1".to_string()));
        item.insert("sk".to_string(), AttributeValue::N(sk_val.to_string()));
        item.insert("data".to_string(), AttributeValue::S(data_val.to_string()));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Query with ScanIndexForward = false (should return in reverse order)
    let mut expression_values = HashMap::new();
    expression_values.insert(":pk_val".to_string(), AttributeValue::S("U#1".to_string()));

    let query_request = QueryTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        key_condition_expression: "pk = :pk_val".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(expression_values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(false), // ScanIndexForward = false
        consistent_read: false,
    };
    let (items, _) = provider.query_table(&query_request).await.unwrap();

    // Should return 3 items
    assert_eq!(items.len(), 3, "Should return 3 items");

    // Should be in reverse order: 300, 200, 100
    let mut returned_sks = Vec::new();
    for item in &items {
        if let Some(AttributeValue::N(sk)) = item.get("sk") {
            returned_sks.push(sk.clone());
        }
    }

    assert_eq!(
        returned_sks,
        vec!["300", "200", "100"],
        "Items should be in reverse order when ScanIndexForward=false"
    );
    if let Some(AttributeValue::N(first_sk)) = items[0].get("sk") {
        assert_eq!(first_sk, "300", "First item should have sk=300");
    }
    if let Some(AttributeValue::N(last_sk)) = items[2].get("sk") {
        assert_eq!(last_sk, "100", "Last item should have sk=100");
    }
}

#[tokio::test]
async fn scan_pagination() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table_name = unique_table_name("LimitedTable");
    let request = CreateTableRequest::new(
        table_name.clone(),
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

    provider.create_table(&request).await.unwrap();
    for i in 1..=5 {
        let mut item = HashMap::new();
        item.insert("id".to_string(), AttributeValue::S(format!("item{i}")));
        item.insert("value".to_string(), AttributeValue::S(format!("val{i}")));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Scan with limit of 2 (should trigger pagination)
    let scan_request = ScanTableRequest {
        table_name: table_name.clone(),
        index_name: None,
        limit: Some(2),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();

    // Should return only 2 items due to limit
    assert_eq!(items.len(), 2, "Should return only 2 items due to limit");

    // Should return LastEvaluatedKey for pagination since there are more items
    assert!(
        last_evaluated_key.is_some(),
        "Should return LastEvaluatedKey for pagination"
    );
}

#[tokio::test]
async fn scan_table_with_index() {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    let table_name = unique_table_name("gsi_scan_test");
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));

    provider.create_table(&request).await.unwrap();
    for i in 1..=3 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(format!("item{i}")));
        item.insert("sk".to_string(), AttributeValue::S(format!("sort{i}")));
        item.insert(
            "gsi_pk".to_string(),
            AttributeValue::S(format!("gsi_value{i}")),
        );
        item.insert("data".to_string(), AttributeValue::S(format!("Data {i}")));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Manually create GSI entries for testing
    let table_metadata = provider
        .get_table_metadata_from_name(&table_name)
        .await
        .unwrap()
        .unwrap();
    let table_identity = provider
        .get_table_identity_from_name(&table_name)
        .await
        .unwrap()
        .unwrap()
        .identity
        .clone();

    for i in 1..=3 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(format!("item{i}")));
        item.insert("sk".to_string(), AttributeValue::S(format!("sort{i}")));
        item.insert(
            "gsi_pk".to_string(),
            AttributeValue::S(format!("gsi_value{i}")),
        );
        item.insert("data".to_string(), AttributeValue::S(format!("Data {i}")));

        let gsi_key = ItemKey::from_key_schema_for_index(
            table_metadata.table_name.clone(),
            &table_metadata.key_schema,
            &request.global_secondary_indexes.as_ref().unwrap()[0].index_name,
            &table_metadata.global_secondary_indexes.clone().unwrap()[0].key_schema,
            &item,
        )
        .unwrap()
        .unwrap();
        let gsi_value = storage_types::storage_serde::to_bytes(&item).unwrap();

        provider
            .kv_store
            .put(
                table_keys::item_key(&table_identity, &gsi_key)
                    .unwrap()
                    .as_slice(),
                &gsi_value,
                None,
            )
            .await
            .unwrap();
    }

    // Scan the GSI index
    let scan_request = ScanTableRequest {
        table_name: table_name.clone(),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();

    assert_eq!(items.len(), 3, "Should return all 3 items from GSI scan");
    assert!(
        last_evaluated_key.is_none(),
        "Should not have pagination for this test"
    );
    let mut found_gsi_values = Vec::new();
    for item in &items {
        if let Some(AttributeValue::S(gsi_pk)) = item.get("gsi_pk") {
            found_gsi_values.push(gsi_pk.clone());
        }
    }
    found_gsi_values.sort();

    let expected_gsi_values: Vec<String> = (1..=3).map(|i| format!("gsi_value{i}")).collect();
    let mut expected_sorted = expected_gsi_values.clone();
    expected_sorted.sort();

    assert_eq!(
        found_gsi_values, expected_sorted,
        "Should find all GSI values"
    );
}
