use serde_json::json;

use crate::{
    AttributeDefinition, BillingMode, CreateTableRequest, DescribeTableResponse, KeyAttributeType,
    KeySchemaElement, KeyType, MaxIndexers, TableName, TimestampMillis,
};

#[test]
fn describe_table_response_deserializes_fractional_creation_datetime_tests() {
    let payload = json!({
        "Table": {
            "TableName": "sys",
            "TableStatus": "ACTIVE",
            "CreationDateTime": 1_771_945_767.554,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "TableSizeBytes": 0,
            "ItemCount": 0,
            "MaxIndexers": 0,
            "TableArn": "arn:aws:dynamodb:us-west-2:123456789012:table/sys"
        }
    });

    let decoded: DescribeTableResponse =
        serde_json::from_value(payload).expect("describe table response should decode");
    let created_at = TimestampMillis::from(decoded.table.created_at);
    assert_eq!(created_at.timestamp_millis(), 1_771_945_767_554);
}

#[test]
fn standard_dynamodb_table_description_defaults_absent_max_indexers() {
    let payload = json!({
        "Table": {
            "TableName": "standard",
            "TableStatus": "ACTIVE",
            "CreationDateTime": 1_771_945_767.554,
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "TableSizeBytes": 0,
            "ItemCount": 0,
            "TableArn": "arn:aws:dynamodb:us-west-2:123456789012:table/standard"
        }
    });

    let decoded: DescribeTableResponse =
        serde_json::from_value(payload).expect("standard DynamoDB response");
    assert_eq!(decoded.table.max_indexers, MaxIndexers::ZERO);
}

#[test]
fn standard_create_table_request_omits_zero_max_indexers() {
    let request = CreateTableRequest::new(
        TableName::new("standard"),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    );

    let encoded = serde_json::to_value(request).expect("serialize CreateTable request");
    assert!(encoded.get("MaxIndexers").is_none());
}
