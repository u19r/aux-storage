use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct TestRecord {
    pk: String,
    sk: String,
    payload: String,
}

#[test]
fn small_json_is_stored_raw_and_round_trips() {
    let value = TestRecord {
        pk: "tenant#1".to_string(),
        sk: "item#1".to_string(),
        payload: "short payload".to_string(),
    };
    let json = serde_json::to_vec(&value).expect("serialize fixture");

    let encoded = crate::storage_serde::compress_json_bytes(&json);
    let decoded = crate::storage_serde::decompress_bytes(&encoded).expect("decode raw json");
    let owned_decoded =
        crate::storage_serde::decompress_owned_bytes(encoded).expect("decode owned raw json");
    let round_trip: TestRecord = crate::storage_serde::from_bytes(
        &crate::storage_serde::to_bytes(&value).expect("encode record"),
    )
    .expect("decode record");

    assert_eq!(decoded, json);
    assert_eq!(owned_decoded, json);
    assert_eq!(round_trip, value);
}

#[test]
fn medium_json_is_stored_raw_to_avoid_read_path_decompression() {
    let value = TestRecord {
        pk: "tenant#1".to_string(),
        sk: "item#1".to_string(),
        payload: "same-value-".repeat(250),
    };
    let json = serde_json::to_vec(&value).expect("serialize fixture");

    let encoded = crate::storage_serde::compress_json_bytes(&json);
    let legacy_compressed = lz4_flex::compress_prepend_size(&json);
    let decoded = crate::storage_serde::decompress_owned_bytes(encoded.clone())
        .expect("decode owned medium raw json");

    assert!(json.len() > 1_024);
    assert!(json.len() <= crate::STORAGE_SERDE_RAW_JSON_LIMIT_BYTES);
    assert!(legacy_compressed.len() < json.len());
    assert_eq!(encoded.len(), json.len() + 8);
    assert_eq!(decoded, json);
}

#[test]
fn large_compressible_json_uses_compressed_encoding_and_round_trips() {
    let value = TestRecord {
        pk: "tenant#1".to_string(),
        sk: "item#1".to_string(),
        payload: "same-value-".repeat(1_024),
    };
    let json = serde_json::to_vec(&value).expect("serialize fixture");

    let encoded = crate::storage_serde::compress_json_bytes(&json);
    let decoded = crate::storage_serde::decompress_bytes(&encoded).expect("decode compressed json");

    assert!(encoded.len() < json.len());
    assert_eq!(decoded, json);
}

#[test]
fn legacy_lz4_bytes_still_decode() {
    let json = br#"{"pk":"tenant#1","sk":"item#1","payload":"legacy"}"#;
    let legacy = lz4_flex::compress_prepend_size(json);

    let decoded = crate::storage_serde::decompress_bytes(&legacy).expect("decode legacy lz4");

    assert_eq!(decoded, json);
}
