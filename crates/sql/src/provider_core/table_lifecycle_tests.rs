use storage_types::{
    AttributeDefinition, BillingMode, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, TableName,
};

use crate::provider_core::table_lifecycle::validate_create_table_request;

#[test]
fn validate_create_table_rejects_duplicate_key_schema_attribute() {
    let request = CreateTableRequest {
        table_name: TableName::new("users"),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Range,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: None,
        global_secondary_indexes: None,
        local_secondary_indexes: None,
        stream_specification: None,
        table_class: None,
        tags: None,
        sse_specification: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        resource_policy: None,
    };

    let error = validate_create_table_request(&request).unwrap_err();

    assert!(error.to_string().contains("Duplicate key schema attribute"));
}
