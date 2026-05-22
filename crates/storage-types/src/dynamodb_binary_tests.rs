use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    StorageEnum, StorageError, WireAttributeDecode,
    dynamodb_binary::{self, SENTINEL_KEY, parse_required_dynamo_binary},
};

#[derive(Debug, Deserialize, Serialize)]
struct RequiredBinaryPayload {
    #[serde(with = "dynamodb_binary")]
    payload: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
struct OptionalBinaryPayload {
    #[serde(
        default,
        deserialize_with = "dynamodb_binary::deserialize_option",
        serialize_with = "dynamodb_binary::serialize_option"
    )]
    payload: Option<Vec<u8>>,
}

#[test]
fn dynamodb_binary_serializes_bytes_as_sentinel_object() {
    let encoded = serde_json::to_value(RequiredBinaryPayload {
        payload: b"Hello".to_vec(),
    })
    .expect("binary payload should serialize");

    assert_eq!(encoded, json!({ "payload": { SENTINEL_KEY: "SGVsbG8=" } }));
}

#[test]
fn dynamodb_binary_deserializes_sentinel_object_and_legacy_string() {
    let sentinel: RequiredBinaryPayload =
        serde_json::from_value(json!({ "payload": { SENTINEL_KEY: "SGVsbG8=" } }))
            .expect("sentinel payload should deserialize");
    let legacy_string: RequiredBinaryPayload =
        serde_json::from_value(json!({ "payload": "SGVsbG8=" }))
            .expect("legacy string payload should deserialize");

    assert_eq!(sentinel.payload, b"Hello");
    assert_eq!(legacy_string.payload, b"Hello");
}

#[test]
fn dynamodb_binary_deserializes_unpadded_base64_from_json() {
    let encoded = STANDARD_NO_PAD.encode(b"hello");
    let parsed: RequiredBinaryPayload =
        serde_json::from_value(json!({ "payload": encoded })).expect("payload should deserialize");

    assert_eq!(parsed.payload, b"hello");
}

#[test]
fn dynamodb_binary_rejects_ambiguous_or_non_binary_json_shapes() {
    for payload in [
        json!({ SENTINEL_KEY: "SGVsbG8=", "extra": true }),
        json!({ SENTINEL_KEY: 12 }),
        json!(null),
        json!(12),
    ] {
        let error = serde_json::from_value::<RequiredBinaryPayload>(json!({ "payload": payload }))
            .expect_err("invalid binary payload shape must fail");
        assert!(
            error.to_string().contains("DynamoDB binary")
                || error.to_string().contains("binary field cannot be null")
                || error
                    .to_string()
                    .contains("unsupported JSON value for DynamoDB binary field"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn optional_dynamodb_binary_preserves_absence_and_some_values() {
    let absent: OptionalBinaryPayload =
        serde_json::from_value(json!({})).expect("missing optional payload should deserialize");
    let explicit_null: OptionalBinaryPayload = serde_json::from_value(json!({ "payload": null }))
        .expect("null optional payload should deserialize");
    let present: OptionalBinaryPayload =
        serde_json::from_value(json!({ "payload": { SENTINEL_KEY: "SGVsbG8=" } }))
            .expect("present optional payload should deserialize");

    assert_eq!(absent.payload, None);
    assert_eq!(explicit_null.payload, None);
    assert_eq!(present.payload, Some(b"Hello".to_vec()));

    let serialized_none = serde_json::to_value(OptionalBinaryPayload { payload: None })
        .expect("optional none should serialize");
    let serialized_some = serde_json::to_value(OptionalBinaryPayload {
        payload: Some(b"Hello".to_vec()),
    })
    .expect("optional some should serialize");

    assert_eq!(serialized_none, json!({ "payload": null }));
    assert_eq!(
        serialized_some,
        json!({ "payload": { SENTINEL_KEY: "SGVsbG8=" } })
    );
}

#[test]
fn parse_required_dynamo_binary_decodes_padded_base64() {
    let decoded = parse_required_dynamo_binary(Some("SGVsbG8="), "payload")
        .expect("valid padded base64 should decode");
    assert_eq!(decoded, b"Hello");
}

#[test]
fn parse_required_dynamo_binary_decodes_unpadded_base64() {
    let encoded = STANDARD_NO_PAD.encode(b"hello");
    let decoded = parse_required_dynamo_binary(Some(&encoded), "payload")
        .expect("valid unpadded base64 should decode");
    assert_eq!(decoded, b"hello");
}

#[test]
fn parse_required_dynamo_binary_errors_when_missing() {
    let error = parse_required_dynamo_binary(None, "payload").expect_err("missing value must fail");
    assert!(
        matches!(
            error,
            StorageError::Base(StorageEnum::InternalServerError { ref message })
                if message == "missing required field payload"
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn parse_required_dynamo_binary_errors_when_invalid() {
    let error =
        parse_required_dynamo_binary(Some("%%%"), "payload").expect_err("invalid base64 must fail");
    assert!(
        matches!(
            error,
            StorageError::Base(StorageEnum::InternalServerError { ref message })
                if message.starts_with("invalid payload field binary payload:")
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn wire_attribute_decode_option_vec_u8_decodes_some() {
    let decoded = <Option<Vec<u8>> as WireAttributeDecode>::decode(Some("SGVsbG8="), "payload")
        .expect("valid padded base64 should decode");
    assert_eq!(decoded, Some(b"Hello".to_vec()));
}

#[test]
fn wire_attribute_decode_option_vec_u8_decodes_none() {
    let decoded = <Option<Vec<u8>> as WireAttributeDecode>::decode(None, "payload")
        .expect("missing optional value should decode to None");
    assert_eq!(decoded, None);
}
