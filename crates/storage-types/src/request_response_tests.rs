use serde_json::json;

use crate::{DescribeTableResponse, TimestampMillis};

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
            "TableArn": "arn:aws:dynamodb:us-west-2:123456789012:table/sys"
        }
    });

    let decoded: DescribeTableResponse =
        serde_json::from_value(payload).expect("describe table response should decode");
    let created_at = TimestampMillis::from(decoded.table.created_at);
    assert_eq!(created_at.timestamp_millis(), 1_771_945_767_554);
}
