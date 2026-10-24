use std::collections::HashMap;

use serde::{Deserialize, Serialize, ser::SerializeMap};

use crate::{AttributeValue, context::WrappedError as _, storage_serde};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct TestRecord {
    pk: String,
    sk: String,
    payload: String,
}

struct OrderedEntries(Vec<(&'static str, serde_json::Value)>);

impl Serialize for OrderedEntries {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

fn ordered_entries(order: &[&'static str]) -> OrderedEntries {
    OrderedEntries(
        order
            .iter()
            .map(|name| (*name, serde_json::Value::String((*name).to_string())))
            .collect(),
    )
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
    let decoded = crate::storage_serde::decompress_owned_bytes(encoded.clone())
        .expect("decode owned medium raw json");

    assert!(json.len() > 1_024);
    assert!(json.len() <= crate::STORAGE_SERDE_RAW_JSON_LIMIT_BYTES);
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
fn given_unversioned_lz4_when_decoded_then_unknown_format_is_rejected() {
    let unversioned = lz4_flex::compress_prepend_size(br#"{"pk":"tenant#1"}"#);

    let error = crate::storage_serde::decompress_bytes(&unversioned)
        .expect_err("unversioned storage data must fail closed");

    assert!(matches!(
        error.to_enum(),
        crate::StorageEnum::InternalServerError { message }
            if message == "storage serde: unknown format header"
    ));
}

#[test]
fn given_ordered_maps_when_stored_then_serializer_order_is_preserved() {
    let forward =
        storage_serde::to_bytes(&ordered_entries(&["a", "b", "c"])).expect("encode forward map");
    let reverse =
        storage_serde::to_bytes(&ordered_entries(&["c", "b", "a"])).expect("encode reverse map");

    assert_ne!(forward, reverse);
    assert_eq!(
        storage_serde::decompress_bytes(&forward).expect("decode canonical map"),
        br#"{"a":"a","b":"b","c":"c"}"#
    );
}

#[test]
fn given_nested_dynamo_maps_when_stored_then_values_round_trip() {
    let value = AttributeValue::M(HashMap::from([
        (
            "z".to_string(),
            AttributeValue::L(vec![
                AttributeValue::N("1e+2".to_string()),
                AttributeValue::N("001.00".to_string()),
            ]),
        ),
        (
            "a".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "z-inner".to_string(),
                    AttributeValue::N("-4E-2".to_string()),
                ),
                (
                    "a-inner".to_string(),
                    AttributeValue::S("value".to_string()),
                ),
            ])),
        ),
    ]));
    let encoded = storage_serde::to_bytes(&value).expect("encode nested map");
    let json = storage_serde::decompress_bytes(&encoded).expect("decode nested map");

    assert_eq!(json, serde_json::to_vec(&value).unwrap());
    assert_eq!(
        storage_serde::from_bytes::<AttributeValue>(&encoded).expect("round-trip nested map"),
        value
    );
}

fn decode_copied_payload(bytes: &[u8]) -> crate::StorageResult<serde_json::Value> {
    let json = storage_serde::decompress_bytes(bytes)?;
    serde_json::from_slice(&json)
        .map_err(crate::StorageEnum::from)
        .map_err(Into::into)
}

fn encode_raw_fixture(json: &[u8]) -> Vec<u8> {
    let mut bytes = crate::constants::RAW_JSON_PREFIX.to_vec();
    bytes.extend_from_slice(json);
    bytes
}

fn storage_decode_fixtures() -> Vec<(&'static str, Vec<u8>)> {
    let mut fixtures = vec![
        ("null", encode_raw_fixture(b"null")),
        ("empty_object", encode_raw_fixture(b"{}")),
        (
            "100_bytes",
            encode_raw_fixture(format!("\"{}\"", "x".repeat(98)).as_bytes()),
        ),
    ];
    for (label, json_len) in [
        ("1150_bytes", 1_150),
        ("1500_bytes", 1_500),
        ("4096_bytes", 4_096),
        ("4097_bytes", 4_097),
        ("65536_bytes", 65_536),
    ] {
        let mut record = serde_json::json!({
            "pk": "p".repeat(100), "sk": "s".repeat(100),
            "gsi1pk": "a".repeat(100), "gsi1sk": "b".repeat(100),
            "gsi2pk": "c".repeat(100), "gsi2sk": "d".repeat(100),
            "payload": "", "ttl": 1_800_000_000_u64,
            "sequence": 17, "nested": {"values": [null, true, -12.5, "雪"]},
        });
        let overhead = serde_json::to_vec(&record).unwrap().len();
        record["payload"] = serde_json::Value::String("x".repeat(json_len - overhead));
        let json = serde_json::to_vec(&record).unwrap();
        assert_eq!(json.len(), json_len);
        let encoded = storage_serde::compress_json_bytes(&json);
        assert_eq!(
            encoded.starts_with(&crate::constants::RAW_JSON_PREFIX),
            json_len <= crate::STORAGE_SERDE_RAW_JSON_LIMIT_BYTES,
        );
        fixtures.push((label, encoded));
    }
    let mut state = 17_u32;
    let entropy: String = (0..8_192)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            char::from(b'!' + (state % 90) as u8)
        })
        .collect();
    let json = serde_json::to_vec(&entropy).unwrap();
    let encoded = storage_serde::compress_json_bytes(&json);
    assert!(encoded.starts_with(&crate::constants::RAW_JSON_PREFIX));
    fixtures.push(("8192_entropy", encoded));
    fixtures
}

#[test]
fn given_storage_formats_when_decoded_then_values_and_encoded_bytes_are_preserved() {
    for (label, encoded) in storage_decode_fixtures() {
        let before = encoded.clone();
        let expected = decode_copied_payload(&encoded).unwrap();
        let actual: serde_json::Value = storage_serde::from_bytes(&encoded).unwrap();
        assert_eq!(actual, expected, "{label}");
        assert_eq!(encoded, before, "{label}");
        assert_eq!(
            storage_serde::decompress_bytes(&encoded).unwrap(),
            storage_serde::decompress_owned_bytes(encoded).unwrap(),
            "{label}"
        );
    }
}

#[test]
fn given_invalid_storage_payloads_when_decoded_then_original_errors_are_preserved() {
    let mut invalid = vec![
        encode_raw_fixture(b""),
        encode_raw_fixture(b"{"),
        encode_raw_fixture(b"{} trailing"),
        b"UNKNOWN!{}".to_vec(),
        crate::constants::LZ4_JSON_PREFIX.to_vec(),
    ];
    for prefix in [
        crate::constants::RAW_JSON_PREFIX,
        crate::constants::LZ4_JSON_PREFIX,
    ] {
        for length in 0..prefix.len() {
            invalid.push(prefix[..length].to_vec());
        }
        let mut unknown_version = prefix.to_vec();
        unknown_version[6] = 255;
        unknown_version.extend_from_slice(b"{}");
        invalid.push(unknown_version);
    }
    let mut corrupt_compressed = crate::constants::LZ4_JSON_PREFIX.to_vec();
    corrupt_compressed.extend_from_slice(&[12, 0, 0, 0, 255]);
    invalid.push(corrupt_compressed);
    let mut compressed_invalid_json = crate::constants::LZ4_JSON_PREFIX.to_vec();
    compressed_invalid_json.extend_from_slice(&lz4_flex::compress_prepend_size(b"{"));
    invalid.push(compressed_invalid_json);

    for encoded in invalid {
        let expected = decode_copied_payload(&encoded).unwrap_err();
        let actual = storage_serde::from_bytes::<serde_json::Value>(&encoded).unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string(), "{encoded:?}");
    }
}

#[test]
fn given_raw_and_compressed_wrong_types_when_decoded_then_serialization_errors_are_preserved() {
    let raw = encode_raw_fixture(b"42");
    let mut compressed = crate::constants::LZ4_JSON_PREFIX.to_vec();
    compressed.extend_from_slice(&lz4_flex::compress_prepend_size(b"42"));
    for encoded in [raw, compressed] {
        let actual = storage_serde::from_bytes::<TestRecord>(&encoded).unwrap_err();
        let json = storage_serde::decompress_bytes(&encoded).unwrap();
        let expected = serde_json::from_slice::<TestRecord>(&json).unwrap_err();
        assert!(matches!(
            actual.to_enum(),
            crate::StorageEnum::Serialization(error) if error.as_ref().to_string() == expected.to_string()
        ));
    }
}

fn measure_storage_decode(
    bytes: &[u8],
    decode: fn(&[u8]) -> crate::StorageResult<serde_json::Value>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "storage_decode",
        file!(),
        line!(),
        None,
    );
    for _ in 0..128 {
        std::hint::black_box(decode(std::hint::black_box(bytes)).unwrap());
    }
    guard.finish()
}

#[test]
#[ignore = "manual storage decode allocation and runtime probe"]
fn given_stored_payloads_when_decoded_then_allocation_work_is_measured() {
    for (label, bytes) in storage_decode_fixtures() {
        for repetition in 0..3 {
            let baseline = measure_storage_decode(&bytes, decode_copied_payload);
            let current = measure_storage_decode(&bytes, storage_serde::from_bytes);
            println!(
                "storage_decode {label} rep={repetition} encoded_bytes={} baseline_allocations={} \
                 current_allocations={} baseline_bytes={} current_bytes={}",
                bytes.len(),
                baseline.allocation_count / 128,
                current.allocation_count / 128,
                baseline.allocated_bytes / 128,
                current.allocated_bytes / 128,
            );
            let is_raw = bytes.starts_with(&crate::constants::RAW_JSON_PREFIX);
            assert_eq!(
                baseline.allocation_count - current.allocation_count,
                u64::from(is_raw) * 128,
            );
            let copied_bytes = if is_raw {
                bytes.len() - crate::constants::STORAGE_SERDE_ENCODED_PREFIX_LEN
            } else {
                0
            };
            assert_eq!(
                baseline.allocated_bytes - current.allocated_bytes,
                copied_bytes as u64 * 128,
            );
        }
        for (name, decode) in [
            (
                "baseline",
                decode_copied_payload as fn(&[u8]) -> crate::StorageResult<serde_json::Value>,
            ),
            ("current", storage_serde::from_bytes),
        ] {
            let started = std::time::Instant::now();
            for _ in 0..5_000 {
                std::hint::black_box(decode(std::hint::black_box(&bytes)).unwrap());
            }
            println!(
                "storage_decode_runtime {label} {name} ns_per_op={}",
                started.elapsed().as_nanos() / 5_000
            );
        }
    }
}
