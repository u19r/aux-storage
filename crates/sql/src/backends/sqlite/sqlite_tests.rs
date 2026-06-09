#[cfg(test)]
use std::collections::HashMap;

use storage_provider::StorageProvider as _;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, TableName,
};

use crate::backends::sqlite::SQLiteStorageProvider;

async fn create_test_provider() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("Failed to create provider");
    provider
        .initialize_storage()
        .await
        .expect("Failed to initialize");

    provider
}

async fn create_test_table(provider: &SQLiteStorageProvider, table_name: &str) {
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

    provider
        .create_table(&request)
        .await
        .expect("Failed to create test table");
}

async fn create_hash_range_table(provider: &SQLiteStorageProvider, table_name: &str) {
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

    provider
        .create_table(&request)
        .await
        .expect("Failed to create hash-range table");
}

#[tokio::test]
async fn put_and_get_item_basic() {
    let provider = create_test_provider().await;
    create_test_table(&provider, "test_table").await;
    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("test123".to_string()));
    item.insert(
        "name".to_string(),
        AttributeValue::S("Test Item".to_string()),
    );
    item.insert("count".to_string(), AttributeValue::N("42".to_string()));

    let put_result = provider
        .put_item(
            TableName::new("test_table"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(put_result.is_ok(), "PutItem should succeed: {put_result:?}");
    let mut key = HashMap::new();
    key.insert("id".to_string(), AttributeValue::S("test123".to_string()));

    let get_result = provider
        .get_item_map(TableName::new("test_table"), key, true)
        .await;
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
async fn put_and_get_item_hash_range() {
    let provider = create_test_provider().await;
    create_hash_range_table(&provider, "hash_range_table").await;
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("Some data".to_string()),
    );
    item.insert("active".to_string(), AttributeValue::BOOL(true));

    let put_result = provider
        .put_item(
            TableName::new("hash_range_table"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(put_result.is_ok(), "PutItem should succeed: {put_result:?}");
    let mut key = HashMap::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    key.insert("sk".to_string(), AttributeValue::S("sort1".to_string()));

    let get_result = provider
        .get_item_map(TableName::new("hash_range_table"), key, true)
        .await;
    assert!(get_result.is_ok(), "GetItem should succeed: {get_result:?}");

    let retrieved_item = get_result.unwrap();
    assert!(retrieved_item.is_some(), "GetItem should return the item");

    let retrieved_item = retrieved_item.unwrap();
    assert_eq!(retrieved_item.len(), 4, "Should have 4 attributes");
    assert_eq!(
        retrieved_item.get("pk"),
        Some(&AttributeValue::S("partition1".to_string()))
    );
    assert_eq!(
        retrieved_item.get("sk"),
        Some(&AttributeValue::S("sort1".to_string()))
    );
    assert_eq!(
        retrieved_item.get("data"),
        Some(&AttributeValue::S("Some data".to_string()))
    );
    assert_eq!(
        retrieved_item.get("active"),
        Some(&AttributeValue::BOOL(true))
    );
}

#[tokio::test]
async fn get_item_does_not_exist() {
    let provider = create_test_provider().await;
    create_test_table(&provider, "test_table").await;

    // Try to get an item that doesn't exist
    let mut key = HashMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("does_not_exist".to_string()),
    );

    let get_result = provider
        .get_item_map(TableName::new("test_table"), key, true)
        .await;
    assert!(
        get_result.is_ok(),
        "GetItem should succeed even for items that do not exist: {get_result:?}"
    );

    let retrieved_item = get_result.unwrap();
    assert!(
        retrieved_item.is_none(),
        "GetItem should return None for items that do not exist"
    );
}

#[tokio::test]
async fn put_and_get_different_attribute_types() {
    let provider = create_test_provider().await;
    create_test_table(&provider, "types_table").await;
    let mut item = HashMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("types_test".to_string()),
    );
    item.insert(
        "string_attr".to_string(),
        AttributeValue::S("hello world".to_string()),
    );
    item.insert(
        "number_attr".to_string(),
        AttributeValue::N("123.45".to_string()),
    );
    item.insert(
        "binary_attr".to_string(),
        AttributeValue::B("aGVsbG8=".to_string()),
    );
    item.insert("bool_attr".to_string(), AttributeValue::BOOL(true));
    item.insert("null_attr".to_string(), AttributeValue::NULL(true));
    item.insert(
        "string_set".to_string(),
        AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
    );
    item.insert(
        "number_set".to_string(),
        AttributeValue::NS(vec!["1".to_string(), "2".to_string()]),
    );
    item.insert(
        "binary_set".to_string(),
        AttributeValue::BS(vec!["YWJj".to_string(), "ZGVm".to_string()]),
    );

    let put_result = provider
        .put_item(
            TableName::new("types_table"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(put_result.is_ok(), "PutItem should succeed: {put_result:?}");
    let mut key = HashMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("types_test".to_string()),
    );

    let get_result = provider
        .get_item_map(TableName::new("types_table"), key, true)
        .await;
    assert!(get_result.is_ok(), "GetItem should succeed: {get_result:?}",);

    let retrieved_item = get_result.unwrap();
    assert!(retrieved_item.is_some(), "GetItem should return the item");

    let retrieved_item = retrieved_item.unwrap();
    assert_eq!(retrieved_item.len(), 9, "Should have 9 attributes");
    assert_eq!(
        retrieved_item.get("id"),
        Some(&AttributeValue::S("types_test".to_string()))
    );
    assert_eq!(
        retrieved_item.get("string_attr"),
        Some(&AttributeValue::S("hello world".to_string()))
    );
    assert_eq!(
        retrieved_item.get("number_attr"),
        Some(&AttributeValue::N("123.45".to_string()))
    );
    assert_eq!(
        retrieved_item.get("binary_attr"),
        Some(&AttributeValue::B("aGVsbG8=".to_string()))
    );
    assert_eq!(
        retrieved_item.get("bool_attr"),
        Some(&AttributeValue::BOOL(true))
    );
    assert_eq!(
        retrieved_item.get("null_attr"),
        Some(&AttributeValue::NULL(true))
    );
    assert_eq!(
        retrieved_item.get("string_set"),
        Some(&AttributeValue::SS(vec!["a".to_string(), "b".to_string()]))
    );
    assert_eq!(
        retrieved_item.get("number_set"),
        Some(&AttributeValue::NS(vec!["1".to_string(), "2".to_string()]))
    );
    assert_eq!(
        retrieved_item.get("binary_set"),
        Some(&AttributeValue::BS(vec![
            "YWJj".to_string(),
            "ZGVm".to_string()
        ]))
    );
}

#[test]
fn gsi_attribute_type_conversion_bug() {
    use std::collections::HashMap;

    use rusqlite::Connection;
    use storage_types::{
        AttributeDefinition, KeyAttributeType, KeySchemaElement, KeyType, StoredTableInfo,
        TableStatus,
    };

    use crate::utils::add_gsi_attributes_from_columns_test_helper;

    // Set up a mock SQLite row that simulates GSI data
    let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
    conn.execute(
        "CREATE TABLE test_gsi (
            category TEXT,
            score REAL,
            table_pk TEXT,
            table_sk TEXT,
            attributes_blob TEXT
        )",
        [],
    )
    .expect("Failed to create test table");

    // Insert test data with a numeric score
    conn.execute(
        "INSERT INTO test_gsi (category, score, table_pk, table_sk, attributes_blob) 
         VALUES ('electronics', 95.5, 'item1', '', '{}')",
        [],
    )
    .expect("Failed to insert test data");
    let table_info = StoredTableInfo {
        table_name: TableName::new("test_table"),
        table_status: TableStatus::Active,
        created_at: chrono::Utc::now().timestamp_millis().into(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "category".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "score".to_string(),
                attribute_type: KeyAttributeType::N, // This should be preserved as Number
            },
        ],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    };

    // GSI key schema - this is what the function processes
    let gsi_key_schema = vec![
        KeySchemaElement {
            attribute_name: "category".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "score".to_string(),
            key_type: KeyType::Range,
        },
    ];

    // Query the test data
    let mut stmt = conn
        .prepare("SELECT * FROM test_gsi")
        .expect("Failed to prepare statement");
    let mut rows = stmt.query([]).expect("Failed to query");

    if let Some(row) = rows.next().expect("Failed to get row") {
        let mut result = HashMap::new();

        // This is the function we're testing - it contains the "for now" bug
        add_gsi_attributes_from_columns_test_helper(row, &table_info, &gsi_key_schema, &mut result);
        println!("Result attributes: {result:?}");

        // Fixed: score should now be AttributeValue::N, preserving its numeric type
        match result.get("score") {
            Some(storage_types::AttributeValue::N(val)) => {
                println!("CORRECT: score preserved as number: {val:?}");
                assert_eq!(val, "95.5");
                // This assertion now passes after fixing the bug
            }
            Some(storage_types::AttributeValue::S(val)) => {
                println!("BUG: score is incorrectly converted to string: {val:?}");
                panic!("Score should be numeric but is string - the bug has not been fixed");
            }
            other => {
                panic!("Unexpected value for score: {other:?}");
            }
        }

        // Category should correctly be a string
        match result.get("category") {
            Some(storage_types::AttributeValue::S(val)) => {
                assert_eq!(val, "electronics");
            }
            other => {
                panic!("Category should be string but got: {other:?}");
            }
        }
    } else {
        panic!("No test data found");
    }
}
