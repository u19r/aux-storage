use storage_types::{
    AttributeDefinition, GlobalSecondaryIndex, IndexName, KeyAttributeType, KeySchemaElement,
    KeyType, Projection, ProjectionType, StoredTableInfo, StreamSpecification, StreamViewType,
    TableName, TableStatus, TimestampMillis,
};

use crate::manager::StorageApiManagerImpl;

#[test]
fn delete_table_response_reports_deleting_table_with_original_shape() {
    let table_info = sample_table_info();

    let response = StorageApiManagerImpl::build_delete_table_response(
        table_info,
        Some("stream-arn".to_string()),
        Some("stream-label".to_string()),
    );
    let table = response.table_description;

    assert_eq!(table.table_name, TableName::new("Orders"));
    assert_eq!(table.table_status, TableStatus::Deleting);
    assert_eq!(
        table.table_arn,
        "arn:aws:dynamodb:us-east-1:123456789012:table/Orders"
    );
    assert_eq!(table.table_size_bytes, 1024);
    assert_eq!(table.item_count, 12);
    assert_eq!(table.latest_stream_arn.as_deref(), Some("stream-arn"));
    assert_eq!(table.latest_stream_label.as_deref(), Some("stream-label"));
    assert!(matches!(
        table
            .billing_mode_summary
            .expect("billing mode summary")
            .billing_mode,
        Some(storage_types::BillingMode::PayPerRequest)
    ));
    assert!(table.local_secondary_indexes.is_none());
    assert!(table.provisioned_throughput.is_none());
}

#[test]
fn delete_table_response_converts_global_secondary_indexes_to_descriptions() {
    let table_info = sample_table_info();

    let response = StorageApiManagerImpl::build_delete_table_response(table_info, None, None);
    let indexes = response
        .table_description
        .global_secondary_indexes
        .expect("gsi descriptions");

    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].index_name, IndexName::new("ByCustomer"));
    assert_eq!(
        indexes[0].projection.projection_type,
        Some(ProjectionType::All)
    );
    assert!(indexes[0].index_status.is_none());
    assert!(indexes[0].index_arn.is_none());
}

fn sample_table_info() -> StoredTableInfo {
    StoredTableInfo {
        max_indexers: storage_types::MaxIndexers::ZERO,
        table_name: TableName::new("Orders"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "customer_id".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("ByCustomer"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "customer_id".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 1024,
        item_count: 12,
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}
