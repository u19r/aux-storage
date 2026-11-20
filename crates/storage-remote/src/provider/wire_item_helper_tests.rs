use serde_json::json;
use storage_provider::{MAX_RANGE_READ_EVALUATED_BYTES, RangeReadBudget};
use storage_types::StorageEnum;

use super::wire_item_helper::parse_scan_query_wire;

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
