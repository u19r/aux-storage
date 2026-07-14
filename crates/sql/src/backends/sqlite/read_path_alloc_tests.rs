use std::collections::HashMap;

use alloc_counter::count_allocations;
use storage_provider::StorageProvider as _;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateGlobalSecondaryIndex, CreateTableRequest, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, QueryTableRequest,
    ScanTableRequest, TableName, TimeToLiveSpecification, UpdateTimeToLiveRequest,
};

use crate::SQLiteStorageProvider;

const TABLE_NAME: &str = "alloc_read_path";
const PK_VALUE: &str = "tenant#00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001";
const PAGE_LIMIT: u32 = 25;
const ITEM_COUNT: usize = 75;
const ITERATIONS: usize = 32;
const GSI_COUNT: usize = 3;

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
    let mut attribute_definitions = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ];
    let mut gsis = Vec::new();
    for index in 0..GSI_COUNT {
        let gsi_pk = format!("gsi{index}pk");
        let gsi_sk = format!("gsi{index}sk");
        attribute_definitions.push(AttributeDefinition {
            attribute_name: gsi_pk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        attribute_definitions.push(AttributeDefinition {
            attribute_name: gsi_sk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        gsis.push(CreateGlobalSecondaryIndex {
            index_name: IndexName::new(&format!("gsi{index}")),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: gsi_pk,
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: gsi_sk,
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
    }

    let request = CreateTableRequest::new(
        TableName::new(TABLE_NAME),
        attribute_definitions,
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
    let request = request.with_global_secondary_indexes(Some(gsis));
    provider
        .create_table(&request)
        .await
        .expect("create allocation table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: TableName::new(TABLE_NAME),
            time_to_live_specification: TimeToLiveSpecification {
                enabled: true,
                attribute_name: "ttl".to_string(),
            },
        })
        .await
        .expect("enable ttl");
}

fn sample_item(index: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(16);
    item.insert("pk".to_string(), AttributeValue::S(PK_VALUE.to_string()));
    item.insert(
        "sk".to_string(),
        AttributeValue::S(format!(
            "item#{index:04}#\
             sort-key-component-with-realistic-dynamodb-length-000000000000000000000000000000"
        )),
    );
    item.insert(
        "ttl".to_string(),
        AttributeValue::N((2_200_000_000 + index).to_string()),
    );
    item.insert(
        "status".to_string(),
        AttributeValue::S("active".to_string()),
    );
    item.insert("attempts".to_string(), AttributeValue::N(index.to_string()));
    item.insert("payload".to_string(), AttributeValue::S("x".repeat(1_100)));
    item.insert(
        "category".to_string(),
        AttributeValue::S(format!("category#{}", index % 8)),
    );
    item.insert(
        "owner".to_string(),
        AttributeValue::S(format!("owner#{}", index % 16)),
    );
    item.insert(
        "checksum".to_string(),
        AttributeValue::B("YWJjZGVmZ2g=".repeat(10)),
    );
    item.insert(
        "metadata".to_string(),
        AttributeValue::M(HashMap::from([
            (
                "note".to_string(),
                AttributeValue::S("allocation-profile-realistic-read-path".to_string()),
            ),
            (
                "tags".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::S("alpha".to_string()),
                    AttributeValue::S("beta".to_string()),
                    AttributeValue::S("gamma".to_string()),
                ]),
            ),
        ])),
    );
    for gsi in 0..GSI_COUNT {
        item.insert(
            format!("gsi{gsi}pk"),
            AttributeValue::S(format!("gsi{gsi}#partition#{:092}", index % 10)),
        );
        item.insert(
            format!("gsi{gsi}sk"),
            AttributeValue::S(format!("gsi{gsi}#sort#{index:092}")),
        );
    }
    item
}

async fn seed_items(provider: &SQLiteStorageProvider) {
    for idx in 0..ITEM_COUNT {
        provider
            .put_item(
                TableName::new(TABLE_NAME),
                sample_item(idx),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("insert allocation profile item");
    }
}

fn query_request() -> QueryTableRequest {
    QueryTableRequest {
        table_name: TableName::new(TABLE_NAME),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S(PK_VALUE.to_string()),
        )])),
        projection_expression: None,
        limit: Some(PAGE_LIMIT),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: true,
    }
}

fn scan_request() -> ScanTableRequest {
    ScanTableRequest {
        table_name: TableName::new(TABLE_NAME),
        index_name: None,
        limit: Some(PAGE_LIMIT),
        exclusive_start_key: None,
        consistent_read: true,
    }
}

#[count_allocations(label = "sqlite_query_table_page_hot_path")]
async fn measure_query_table_page_hot_path_tests(
    provider: &SQLiteStorageProvider,
    request: &QueryTableRequest,
) {
    for _ in 0..ITERATIONS {
        let (items, lek) = provider
            .query_table(request)
            .await
            .expect("query table page");
        assert_eq!(items.len(), PAGE_LIMIT as usize);
        assert!(lek.is_some());
    }
}

#[count_allocations(label = "sqlite_scan_table_page_hot_path")]
async fn measure_scan_table_page_hot_path_tests(
    provider: &SQLiteStorageProvider,
    request: &ScanTableRequest,
) {
    for _ in 0..ITERATIONS {
        let (items, lek) = provider.scan_table(request).await.expect("scan table page");
        assert_eq!(items.len(), PAGE_LIMIT as usize);
        assert!(lek.is_some());
    }
}

#[tokio::test]
async fn sqlite_read_path_page_allocation_profile_tests() {
    // Snapshot (2026-02-18, `cargo test -p sqlite read_path_alloc_tests --
    // --nocapture`): sqlite_query_table_page_hot_path: allocation_count=18836,
    // allocated_bytes=1612076 sqlite_scan_table_page_hot_path:
    // allocation_count=18434, allocated_bytes=1601568
    let provider = create_provider().await;
    create_table(&provider).await;
    seed_items(&provider).await;

    measure_query_table_page_hot_path_tests(&provider, &query_request()).await;
    measure_scan_table_page_hot_path_tests(&provider, &scan_request()).await;
}
