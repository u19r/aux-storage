use serde_json::{Value, json};

use crate::BatchGetItemRequest;

#[test]
fn given_batch_request_when_decoded_and_encoded_then_public_shape_is_preserved() {
    let request = json!({
        "RequestItems": {
            "table-001": {
                "Keys": [
                    {"pk": {"S": "tenant#1"}},
                    {"pk": {"S": "tenant#2"}}
                ],
                "ProjectionExpression": "#value",
                "ExpressionAttributeNames": {"#value": "payload"},
                "ConsistentRead": true
            }
        },
        "ReturnConsumedCapacity": "TOTAL"
    });
    let decoded: BatchGetItemRequest =
        serde_json::from_value(request.clone()).expect("valid public request");
    assert_eq!(decoded.request_items.len(), 1);
    assert_eq!(
        decoded
            .request_items
            .values()
            .next()
            .expect("table")
            .keys
            .len(),
        2
    );
    assert_eq!(serde_json::to_value(decoded).expect("encode"), request);
}

#[test]
fn given_duplicate_or_invalid_batch_tables_when_decoded_then_contract_is_preserved() {
    let duplicate = r#"{"RequestItems":{"table-001":{"Keys":[{"pk":{"S":"old"}}]},"table-001":{"Keys":[{"pk":{"S":"new"}}]}}}"#;
    let decoded: BatchGetItemRequest = serde_json::from_str(duplicate).expect("duplicate table");
    let encoded: Value = serde_json::to_value(decoded).expect("encode");
    assert_eq!(
        encoded["RequestItems"]["table-001"]["Keys"][0]["pk"]["S"],
        "new"
    );

    for invalid in [
        r#"{"RequestItems":[]}"#,
        r#"{"RequestItems":null}"#,
        r#"{"RequestItems":{"table-001":{}}}"#,
    ] {
        assert!(serde_json::from_str::<BatchGetItemRequest>(invalid).is_err());
    }
}
