use std::collections::HashMap;

use alloc_counter::count_allocations;
use storage_provider::StorageProvider as _;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, TableName,
};

use crate::SQLiteStorageProvider;

const TABLE_NAME: &str = "alloc_get_item";
const ITEM_ID: &str = "item-123";
const ITERATIONS: usize = 256;
const TOP_LEVEL_ATTRIBUTE_COUNT: usize = 14;

async fn create_provider() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize sqlite provider");
    provider
}

async fn create_table(provider: &SQLiteStorageProvider) {
    let request = CreateTableRequest::new(
        TableName::new(TABLE_NAME),
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
        .expect("create test table");
}

fn sample_item() -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("id".to_string(), AttributeValue::S(ITEM_ID.to_string())),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            "table_id".to_string(),
            AttributeValue::S("t-123".to_string()),
        ),
        (
            "owner_id".to_string(),
            AttributeValue::S("u-456".to_string()),
        ),
        (
            "region".to_string(),
            AttributeValue::S("us-east-1".to_string()),
        ),
        ("priority".to_string(), AttributeValue::N("42".to_string())),
        ("attempts".to_string(), AttributeValue::N("7".to_string())),
        ("version".to_string(), AttributeValue::N("99".to_string())),
        ("enabled".to_string(), AttributeValue::BOOL(true)),
        (
            "tags".to_string(),
            AttributeValue::SS(vec!["alpha".to_string(), "beta".to_string()]),
        ),
        (
            "metadata".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "source".to_string(),
                    AttributeValue::S("alloc-test".to_string()),
                ),
                ("shard".to_string(), AttributeValue::N("3".to_string())),
            ])),
        ),
        (
            "history".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("created".to_string()),
                AttributeValue::S("updated".to_string()),
            ]),
        ),
        (
            "checkpoint".to_string(),
            AttributeValue::N("1729".to_string()),
        ),
        (
            "checksum".to_string(),
            AttributeValue::B("YWJjMTIz".to_string()),
        ),
    ])
}

fn sample_key() -> HashMap<String, AttributeValue> {
    HashMap::from([("id".to_string(), AttributeValue::S(ITEM_ID.to_string()))])
}

#[count_allocations(label = "sqlite_get_item_wire_read_path")]
async fn measure_wire_read_path_tests(
    provider: &SQLiteStorageProvider,
    key: &HashMap<String, AttributeValue>,
) {
    let key = KeyAttributes::from(key.clone());
    for _ in 0..ITERATIONS {
        let item = <SQLiteStorageProvider as storage_provider::StorageProvider>::get_item(
            provider,
            TableName::new(TABLE_NAME),
            key.clone(),
            true,
        )
        .await
        .expect("read item");
        let item = item.expect("item should exist");
        assert!(item.payload_len() > 0);
    }
}

#[tokio::test]
async fn get_item_wire_read_path_allocation_stays_within_budget() {
    // Snapshot (2026-02-18, `cargo test -p sqlite get_item_alloc_tests --
    // --nocapture`): sqlite_get_item_wire_read_path: allocation_count=6920,
    // allocated_bytes=619904
    let provider = create_provider().await;
    create_table(&provider).await;

    provider
        .put_item(
            TableName::new(TABLE_NAME),
            sample_item(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert sample item");

    let key = sample_key();
    measure_wire_read_path_tests(&provider, &key).await;

    assert_eq!(sample_item().len(), TOP_LEVEL_ATTRIBUTE_COUNT);
}
