use storage_types::{
    AttributeDefinition, CreateTableRequest, KeyAttributeType, KeySchemaElement, KeyType, TableName,
};

fn base_req() -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new("t"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".into(),
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
}

#[test]
fn ok() {
    assert!(base_req().validate_key_schema().is_ok());
}

#[test]
fn dup_key() {
    let mut r = base_req();
    r.key_schema[1].attribute_name = "pk".into();
    assert!(r.validate_key_schema().is_err());
}

#[test]
fn missing_def() {
    let mut r = base_req();
    r.attribute_definitions.pop();
    assert!(r.validate_key_schema().is_err());
}
