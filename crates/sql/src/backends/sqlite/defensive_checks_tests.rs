//! Tests for defensive programming validations (Improvement #13).
use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, CreateGlobalSecondaryIndex, CreateTableRequest, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, TableName,
};

use crate::backends::sqlite::SQLiteStorageProvider; // provider under test

async fn provider() -> SQLiteStorageProvider {
    let p = SQLiteStorageProvider::new(":memory:").await.unwrap();
    p.initialize_storage().await.unwrap();
    let _ = stream_provider::StreamProvider::initialize_stream(&p).await;
    p
}

fn attr(name: &str, t: KeyAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.into(),
        attribute_type: t,
    }
}
fn key_elem(name: &str, t: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.into(),
        key_type: t,
    }
}

#[tokio::test]
async fn duplicate_key_attribute_rejected() {
    let p = provider().await;
    let req = CreateTableRequest::new(
        TableName::new("dup_keys"),
        vec![attr("pk", KeyAttributeType::S)],
        vec![
            key_elem("pk", KeyType::Hash),
            key_elem("pk", KeyType::Range),
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    let err = p.create_table(&req).await.unwrap_err();
    assert!(format!("{err}").contains("Duplicate key schema attribute"));
}

#[tokio::test]
async fn missing_key_attribute_definition_rejected() {
    let p = provider().await;
    let req = CreateTableRequest::new(
        TableName::new("missing_attr_def"),
        vec![attr("pk", KeyAttributeType::S)],
        vec![
            key_elem("pk", KeyType::Hash),
            key_elem("rk", KeyType::Range),
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    let err = p.create_table(&req).await.unwrap_err();
    assert!(format!("{err}").contains("Key attribute 'rk' missing"));
}

#[tokio::test]
async fn gsi_attribute_must_exist() {
    let p = provider().await;
    let req = CreateTableRequest::new(
        TableName::new("gsi_attr_missing"),
        vec![attr("pk", KeyAttributeType::S)],
        vec![key_elem("pk", KeyType::Hash)],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("gsi1"),
        key_schema: vec![key_elem("gsipk", KeyType::Hash)],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    let err = p.create_table(&req).await.unwrap_err();
    assert!(format!("{err}").contains("GSI 'gsi1' key attribute 'gsipk'"));
}

#[tokio::test]
async fn gsi_count_capped() {
    let p = provider().await;
    let mut gsis = Vec::new();
    for i in 0..21 {
        gsis.push(CreateGlobalSecondaryIndex {
            index_name: IndexName::new(&format!("g{i}")),
            key_schema: vec![key_elem("pk", KeyType::Hash)],
            projection: Projection {
                projection_type: Some(ProjectionType::KeysOnly),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
    }
    let req = CreateTableRequest::new(
        TableName::new("gsi_cap"),
        vec![attr("pk", KeyAttributeType::S)],
        vec![key_elem("pk", KeyType::Hash)],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(gsis));
    let err = p.create_table(&req).await.unwrap_err();
    assert!(format!("{err}").contains("Too many global secondary indexes"));
}
