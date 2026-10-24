use std::collections::HashMap;

use serde::Serializer;
use storage_types::{AttributeValue, StorageResult, WireItem, WireItemKeyAttributes};

use super::billing::{
    WriteCostTally, attr_map_payload_bytes, serializable_payload_bytes, storage_payload_bytes,
    wire_items_payload_bytes,
};

#[test]
fn given_value_when_payload_is_measured_then_result_matches_json_length() {
    let value = serde_json::json!({"pk": "a"});

    let bytes = serializable_payload_bytes(&value);

    assert_eq!(bytes, serde_json::to_vec(&value).unwrap().len() as u64);
    assert_eq!(attr_map_payload_bytes(&value), bytes);
}

#[test]
fn given_empty_tally_when_subtracted_then_result_is_empty() {
    let tally = WriteCostTally::default();

    let diff = tally.subtract(&WriteCostTally::default());

    assert_eq!(diff.put_ops, 0);
    assert_eq!(diff.put_bytes, 0);
}

#[test]
fn storage_payload_measurement_matches_the_encoded_storage_format() {
    let empty: HashMap<String, AttributeValue> = HashMap::new();
    let mut nested = HashMap::new();
    nested.insert(
        "label".to_string(),
        AttributeValue::S("東京 🐈".to_string()),
    );
    let item = HashMap::from([
        (
            "unicode".to_string(),
            AttributeValue::S("café 🐈".to_string()),
        ),
        (
            "binary".to_string(),
            AttributeValue::B("AAECAw==".to_string()),
        ),
        (
            "number".to_string(),
            AttributeValue::N("1.2300e+4".to_string()),
        ),
        ("nested".to_string(), AttributeValue::M(nested)),
        (
            "list".to_string(),
            AttributeValue::L(vec![AttributeValue::BOOL(true), AttributeValue::NULL(true)]),
        ),
    ]);
    let compressible = HashMap::from([(
        "payload".to_string(),
        AttributeValue::S("repetitive-value".repeat(700)),
    )]);
    let mut state = 0x1234_5678_u32;
    let incompressible = HashMap::from([(
        "payload".to_string(),
        AttributeValue::S(
            (0..9_000)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    char::from(b'!' + (state % 90) as u8)
                })
                .collect(),
        ),
    )]);

    for value in [&empty, &item, &compressible, &incompressible] {
        let encoded = storage_types::storage_serde::to_bytes(value).unwrap();
        assert_eq!(storage_payload_bytes(value).unwrap(), encoded.len());
    }
}

#[test]
fn storage_payload_measurement_propagates_serialization_errors() {
    struct Invalid;

    impl serde::Serialize for Invalid {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    let measured: StorageResult<usize> = storage_payload_bytes(&Invalid);

    assert!(measured.is_err());
}

#[test]
fn wire_payload_measurement_counts_each_storage_representation() {
    let items = [
        WireItem::dynamo_json(vec![1, 2]),
        WireItem::local_split(
            WireItemKeyAttributes::new(
                "pk".to_string(),
                AttributeValue::S("key".to_string()),
                None,
                None,
            ),
            None,
            Some(vec![3, 4, 5]),
        ),
    ];

    assert_eq!(wire_items_payload_bytes(&[]), 0);
    assert_eq!(wire_items_payload_bytes(&items), 10);
}
