//! Tests focusing on unified read path parity (scan vs query) and projection
//! Include correctness.
use std::collections::HashMap;

use storage_provider::StorageProvider; // bring trait into scope for methods
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, QueryTableRequest, ScanTableRequest,
    StorageEnum, TableName,
};

use crate::backends::sqlite::SQLiteStorageProvider;

async fn create_table_for_parity() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    // Initialize stream tables needed by GSI update job cursor handling
    let _ = stream_provider::StreamProvider::initialize_stream(&provider).await;
    let request = CreateTableRequest::new(
        TableName::new("ParityTable"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".into(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![storage_types::CreateGlobalSecondaryIndex {
        index_name: IndexName::new("GSI1"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".into(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    provider.create_table(&request).await.unwrap();

    // Insert 6 items with two gsi_pk groups
    for i in 0..6 {
        let mut item = HashMap::new();
        item.insert("pk".into(), AttributeValue::S("fixed".into()));
        item.insert("sk".into(), AttributeValue::S(format!("sort#{i}")));
        let gsi_group = if i < 3 { "grpA" } else { "grpB" };
        item.insert("gsi_pk".into(), AttributeValue::S(gsi_group.into()));
        item.insert("payload".into(), AttributeValue::S(format!("val{i}")));
        provider
            .put_item(TableName::new("ParityTable"), item, None, None, None, None)
            .await
            .unwrap();
    }
    process_gsi_updates_or_panic(&provider).await;
    provider
}

#[tokio::test]
async fn scan_vs_query_parity_same_key_condition() {
    let provider = create_table_for_parity().await;
    // Query grpA via QueryTableRequest on GSI
    let query_req = QueryTableRequest {
        table_name: TableName::new("ParityTable"),
        index_name: Some(IndexName::new("GSI1")),
        key_condition_expression: "gsi_pk = :g".into(),
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":g".into(), AttributeValue::S("grpA".into()));
            m
        }),
        expression_attribute_names: None,
        projection_expression: None,
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (query_items, _) = provider.query_table(&query_req).await.unwrap();

    // Scan the index and manually filter to grpA (less efficient but validates
    // parity)
    let scan_req = ScanTableRequest {
        table_name: TableName::new("ParityTable"),
        index_name: Some(IndexName::new("GSI1")),
        limit: Some(100),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (scan_items, _) = provider.scan_table(&scan_req).await.unwrap();
    let mut filtered: Vec<_> = scan_items
        .into_iter()
        .filter(|it| {
            it.get("gsi_pk")
                .unwrap()
                .inner_string()
                .expect("gsi_pk scalar")
                == "grpA"
        })
        .collect();
    filtered.sort_by_key(|it| it.get("sk").unwrap().inner_string().expect("sk scalar"));

    let mut query_sorted = query_items.clone();
    query_sorted.sort_by_key(|it| it.get("sk").unwrap().inner_string().expect("sk scalar"));

    assert_eq!(query_sorted.len(), 3);
    assert_eq!(filtered.len(), 3);
    for (a, b) in query_sorted.iter().zip(filtered.iter()) {
        assert_eq!(a.get("pk"), b.get("pk"));
        assert_eq!(a.get("sk"), b.get("sk"));
        assert_eq!(a.get("gsi_pk"), b.get("gsi_pk"));
    }
}

#[tokio::test]
async fn projection_include_only_allows_whitelisted_attrs() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    let _ = stream_provider::StreamProvider::initialize_stream(&provider).await;
    let request = CreateTableRequest::new(
        TableName::new("ProjInclTable"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "whitelist".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "other".into(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![storage_types::CreateGlobalSecondaryIndex {
        index_name: IndexName::new("InclGSI"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".into(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::Include),
            non_key_attributes: Some(vec!["whitelist".into()]),
        },
        provisioned_throughput: None,
    }]));
    provider.create_table(&request).await.unwrap();

    let mut item = HashMap::new();
    item.insert("pk".into(), AttributeValue::S("p".into()));
    item.insert("sk".into(), AttributeValue::S("s".into()));
    item.insert("gsi_pk".into(), AttributeValue::S("g".into()));
    item.insert("whitelist".into(), AttributeValue::S("yes".into()));
    item.insert("other".into(), AttributeValue::S("no".into()));
    provider
        .put_item(
            TableName::new("ProjInclTable"),
            item,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    process_gsi_updates_or_panic(&provider).await;

    let scan_req = ScanTableRequest {
        table_name: TableName::new("ProjInclTable"),
        index_name: Some(IndexName::new("InclGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (items, _) = provider.scan_table(&scan_req).await.unwrap();
    assert_eq!(items.len(), 1);
    let gsi_item = &items[0];
    assert!(gsi_item.contains_key("pk"));
    assert!(gsi_item.contains_key("sk"));
    assert!(gsi_item.contains_key("gsi_pk"));
    assert!(gsi_item.contains_key("whitelist"));
    assert!(!gsi_item.contains_key("other"));
}

async fn process_gsi_updates_or_panic(provider: &SQLiteStorageProvider) {
    if let Err(error) = provider.process_gsi_updates().await {
        match error.as_ref() {
            StorageEnum::InternalServerError { message } => {
                panic!("process_gsi_updates failed: {message}");
            }
            other => panic!("process_gsi_updates failed: {other:?}"),
        }
    }
}

#[tokio::test]
async fn pagination_boundary_limit_exact() {
    let provider = create_table_for_parity().await; // 6 items across two gsi groups
    // Scan with limit exactly equal to number of grpA items using Query path first
    // page
    let query_req = QueryTableRequest {
        table_name: TableName::new("ParityTable"),
        index_name: Some(IndexName::new("GSI1")),
        key_condition_expression: "gsi_pk = :g".into(),
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":g".into(), AttributeValue::S("grpA".into()));
            m
        }),
        expression_attribute_names: None,
        projection_expression: None,
        limit: Some(3),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items, lek) = provider.query_table(&query_req).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(
        lek.is_none(),
        "No further page expected when results == limit and no extra row fetched"
    );
}
