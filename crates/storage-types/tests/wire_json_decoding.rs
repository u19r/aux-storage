use serde::Deserialize;
use storage_types::{
    AttributeValue, StorageEnum, WireItem, WireItemKeyAttributes, context::WrappedError,
    decode_wire_field, decode_wire_field_json, decode_wire_serde_string,
};

#[derive(Debug, Deserialize, PartialEq)]
struct SavedSettings {
    enabled: bool,
    labels: Vec<String>,
}

fn item_representations() -> [WireItem; 2] {
    let fields = r#"{
        "settings":{"M":{"enabled":{"BOOL":true},"labels":{"L":[{"S":"Müller"},{"S":"second"}]}}},
        "name":{"S":"literal \"quoted\" text"},
        "optional":{"NULL":true},
        "count":{"N":"42"},
        "invalid_number":{"N":"not-a-number"}
    }"#
    .as_bytes();
    [
        WireItem::dynamo_json(fields.to_vec()),
        WireItem::local_split(
            WireItemKeyAttributes::new("pk".into(), AttributeValue::S("key".into()), None, None),
            None,
            Some(fields.to_vec()),
        ),
    ]
}

#[test]
fn given_saved_settings_when_decoded_from_each_wire_representation_then_typed_values_survive() {
    for item in item_representations() {
        assert_eq!(
            decode_wire_field_json::<SavedSettings>(&item, None, "settings").unwrap(),
            SavedSettings {
                enabled: true,
                labels: vec!["Müller".into(), "second".into()]
            },
        );
        assert_eq!(decode_wire_field::<i64>(&item, None, "count").unwrap(), 42);
        assert_eq!(
            decode_wire_field::<String>(&item, None, "name").unwrap(),
            "literal \"quoted\" text"
        );
        assert_eq!(
            decode_wire_field_json::<Option<String>>(&item, None, "optional").unwrap(),
            None
        );
        assert_eq!(
            decode_wire_field_json::<Option<String>>(&item, None, "absent").unwrap(),
            None
        );
        assert_eq!(
            decode_wire_field::<Option<String>>(&item, None, "absent").unwrap(),
            None
        );
    }
}

#[test]
fn given_raw_scalar_when_decoded_then_json_and_literal_fallback_contracts_survive() {
    let invalid_item = WireItem::dynamo_json(b"invalid item".to_vec());
    for (raw, expected) in [
        ("plain", "plain"),
        ("42", "42"),
        ("{not-json}", "{not-json}"),
        ("\"quoted\"", "quoted"),
        ("", ""),
    ] {
        assert_eq!(
            decode_wire_field_json::<String>(&invalid_item, Some(raw), "value").unwrap(),
            expected
        );
    }
    assert_eq!(
        decode_wire_field_json::<i64>(&invalid_item, Some("42"), "value").unwrap(),
        42
    );
    assert_eq!(
        decode_wire_field::<String>(&invalid_item, Some("\"quoted\""), "value").unwrap(),
        "\"quoted\""
    );
    assert_eq!(
        decode_wire_serde_string::<String>("\"quoted\"", "value").unwrap(),
        "\"quoted\""
    );
}

#[test]
fn given_missing_or_invalid_fields_when_decoded_then_errors_keep_their_category_and_diagnostics() {
    for item in item_representations() {
        let missing = decode_wire_field_json::<String>(&item, None, "absent").unwrap_err();
        assert!(
            matches!(missing.to_enum(), StorageEnum::InternalServerError {message} if message == "missing required field absent")
        );
        let wrong_type = decode_wire_field_json::<bool>(&item, None, "name").unwrap_err();
        assert!(
            matches!(wrong_type.to_enum(), StorageEnum::InternalServerError {message} if message.starts_with("decode wire attribute name failed: invalid type: string"))
        );
        let invalid = decode_wire_field_json::<i64>(&item, None, "invalid_number").unwrap_err();
        assert!(
            matches!(invalid.to_enum(), StorageEnum::InternalServerError {message} if message.starts_with("decode wire attribute invalid_number failed: invalid type: string \"not-a-number\", expected i64")),
            "{:?}",
            invalid.to_enum()
        );
        assert!(!missing.to_string().contains("absent"));
        assert!(!wrong_type.to_string().contains("quoted"));
    }
}
