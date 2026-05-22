use storage_types::{
    AttributeDefinition, CreateTableRequest, KeyAttributeType, KeySchemaElement, KeyType, TableName,
};

#[test]
fn gsi_limit() {
    let req = CreateTableRequest::new(
        TableName::new("t"),
        vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![]));
    assert!(req.validate_storage_common().is_ok());
}
