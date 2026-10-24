use serde_json::json;
use storage_provider::{MAX_RANGE_READ_EVALUATED_BYTES, RangeReadBudget};
use storage_types::{AttributeValue, KeyAttributes, KeysAndAttributes, StorageEnum, TableName};

use super::wire_item_helper::{parse_batch_get_wire, parse_get_item_wire, parse_scan_query_wire};

#[test]
fn given_get_item_wire_with_item_when_parsing_then_preserves_dynamo_json_item() {
    let response = serde_json::to_vec(&json!({
        "Item": {
            "pk": { "S": "tenant#1" },
            "count": { "N": "7" }
        }
    }))
    .expect("response");

    let item = parse_get_item_wire(&response)
        .expect("parsed response")
        .expect("item");

    let attributes = item.into_attribute_map().expect("attribute map");
    assert_eq!(
        attributes.get("pk"),
        Some(&AttributeValue::S("tenant#1".to_string()))
    );
    assert_eq!(
        attributes.get("count"),
        Some(&AttributeValue::N("7".to_string()))
    );
}

#[test]
fn given_get_item_wire_without_item_when_parsing_then_returns_none() {
    let item = parse_get_item_wire(br#"{}"#).expect("parsed response");

    assert!(item.is_none());
}

#[test]
fn given_remote_item_is_not_object_when_parsing_then_rejects_payload() {
    let response = br#"{ "Item": "not-an-object" }"#;

    let error = parse_get_item_wire(response).expect_err("object required");

    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message.contains("not a JSON object")
    ));
}

#[test]
fn given_scan_query_wire_with_opaque_string_last_key_then_rejects_payload() {
    let response = serde_json::to_vec(&json!({
        "Items": [
            { "pk": { "S": "tenant#1" } },
            { "pk": { "S": "tenant#2" } }
        ],
        "Count": 2,
        "ScannedCount": 2,
        "LastEvaluatedKey": "opaque-cursor"
    }))
    .expect("response");

    let error = parse_scan_query_wire(&response).expect_err("object required");

    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message.contains("LastEvaluatedKey")
    ));
}

#[test]
fn given_scan_query_wire_with_string_containing_key_object_then_rejects_payload() {
    let response = br#"{
        "Items": [],
        "Count": 0,
        "ScannedCount": 0,
        "LastEvaluatedKey": "{\"pk\":{\"S\":\"tenant#1\"}}"
    }"#;

    let error = parse_scan_query_wire(response).expect_err("object required");

    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message.contains("LastEvaluatedKey")
    ));
}

#[test]
fn given_scan_query_wire_with_object_last_key_when_parsing_then_keeps_raw_json_cursor() {
    let response = br#"{
        "Items": [],
        "Count": 0,
        "ScannedCount": 0,
        "LastEvaluatedKey": { "pk": { "S": "tenant#1" } }
    }"#;

    let parsed = parse_scan_query_wire(response).expect("parsed response");

    assert_eq!(
        parsed.last_evaluated_key.as_deref(),
        Some(r#"{ "pk": { "S": "tenant#1" } }"#)
    );
}

#[test]
fn given_scan_query_wire_with_invalid_counts_when_page_is_built_then_rejects_source_contract() {
    let response = br#"{
        "Items": [{ "pk": { "S": "tenant#1" } }],
        "Count": 1,
        "ScannedCount": 2
    }"#;

    let parsed = parse_scan_query_wire(response).expect("wire response parses");
    let error = parsed
        .into_page(
            None,
            &None,
            RangeReadBudget::try_new(1, MAX_RANGE_READ_EVALUATED_BYTES)
                .expect("valid range budget"),
        )
        .expect_err("count mismatch must fail closed");

    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message }
            if message.contains("counts must equal")
    ));
}

#[test]
fn given_scan_query_wire_without_counts_when_parsing_then_rejects_source_contract() {
    let error = parse_scan_query_wire(br#"{"Items": []}"#)
        .expect_err("missing count fields must fail closed");

    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message.contains("omitted Count")
    ));
}

#[test]
fn given_batch_get_wire_when_parsing_then_decodes_table_responses_and_unprocessed_keys() {
    let table_name = TableName::new("accounts");
    let mut key = KeyAttributes::new();
    key.insert("pk", AttributeValue::S("tenant#2".to_string()));
    let keys = KeysAndAttributes {
        keys: [key].into_iter().collect(),
        attributes_to_get: None,
        projection_expression: Some("pk".to_string()),
        expression_attribute_names: None,
        consistent_read: Some(true),
    };
    let response = serde_json::to_vec(&json!({
        "Responses": {
            "accounts": [
                { "pk": { "S": "tenant#1" } }
            ]
        },
        "UnprocessedKeys": {
            "accounts": keys
        }
    }))
    .expect("response");

    let parsed = parse_batch_get_wire(&response).expect("parsed response");
    let responses = parsed.responses.expect("responses");
    let items = responses.get(&table_name).expect("table responses");
    let attributes = items[0]
        .clone()
        .into_attribute_map()
        .expect("attribute map");

    assert_eq!(
        attributes.get("pk"),
        Some(&AttributeValue::S("tenant#1".to_string()))
    );
    assert_eq!(
        parsed
            .unprocessed_keys
            .as_ref()
            .and_then(|tables| tables.get(&table_name))
            .and_then(|keys| keys.projection_expression.as_deref()),
        Some("pk")
    );
}
