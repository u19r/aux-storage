#[cfg(test)]
use storage_provider::StorageProvider;
#[cfg(test)]
use storage_types::{IndexName, ScanTableRequest, TableName};
#[cfg(test)]
use stream_provider::StreamProvider;

#[cfg(test)]
use crate::SQLiteStorageProvider;

// Basic test to ensure missing index error message is standardized.
#[tokio::test]
async fn missing_index_error_scan() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    // Create table without GSIs
    let create = storage_types::CreateTableRequest::new(
        TableName::new("NoGsiTable"),
        vec![storage_types::AttributeDefinition {
            attribute_name: "id".into(),
            attribute_type: storage_types::KeyAttributeType::S,
        }],
        vec![storage_types::KeySchemaElement {
            attribute_name: "id".into(),
            key_type: storage_types::KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    provider.create_table(&create).await.unwrap();

    let scan = ScanTableRequest {
        table_name: TableName::new("NoGsiTable"),
        index_name: Some(IndexName::new("missing")),
        limit: Some(1),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let err = provider.scan_table(&scan).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("does not have the specified index: missing"),
        "Unexpected error: {msg}"
    );
}
