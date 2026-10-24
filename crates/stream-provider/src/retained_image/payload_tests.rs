use std::io::{Read, Write};

use super::{payload::RetainedPayload, payload_read::RetainedPayloadRead};
use crate::constants::{
    RETAINED_PAYLOAD_FORMAT, RETAINED_RAW_PAYLOAD_FORMAT, RETAINED_RAW_PAYLOAD_MAX_BYTES,
};

fn compressed_payload(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = vec![RETAINED_PAYLOAD_FORMAT];
    encoded.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    let mut encoder = flate2::write::ZlibEncoder::new(encoded, flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn given_small_records_when_encoded_then_raw_and_existing_compressed_records_round_trip() {
    for length in [0, 1, 128, RETAINED_RAW_PAYLOAD_MAX_BYTES] {
        let bytes = vec![b'x'; length];
        let raw = RetainedPayload::encode(&bytes, length + 64).unwrap();
        assert_eq!(raw[0], RETAINED_RAW_PAYLOAD_FORMAT);
        assert_eq!(raw.len(), length + 41);
        for encoded in [raw, compressed_payload(&bytes)] {
            assert_eq!(RetainedPayload::decode(&encoded, length).unwrap(), bytes);
            RetainedPayload::require_content(&encoded, &bytes).unwrap();
            let mut reader = RetainedPayloadRead::try_new(&encoded, length).unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).unwrap();
            assert_eq!(actual, bytes);
        }
    }
}

#[test]
fn given_large_or_capacity_constrained_records_when_encoded_then_compression_is_preserved() {
    for (length, maximum) in [(2049, 4096), (1024, 128)] {
        let bytes = vec![b'x'; length];
        let encoded = RetainedPayload::encode(&bytes, maximum).unwrap();
        assert_eq!(encoded[0], RETAINED_PAYLOAD_FORMAT);
        assert!(encoded.len() <= maximum);
        assert_eq!(RetainedPayload::decode(&encoded, length).unwrap(), bytes);
    }
}

#[test]
fn given_retained_payload_when_corrupt_truncated_or_extended_then_no_content_is_accepted() {
    let bytes = b"retained session activity";
    for encoded in [
        RetainedPayload::encode(bytes, 256).unwrap(),
        compressed_payload(bytes),
    ] {
        assert!(RetainedPayload::decode(&encoded, bytes.len() - 1).is_err());
        let mut wrong_content = bytes.to_vec();
        wrong_content[0] ^= 1;
        assert!(RetainedPayload::require_content(&encoded, &wrong_content).is_err());
        let corrupt_offsets = if encoded[0] == RETAINED_RAW_PAYLOAD_FORMAT {
            (0..encoded.len()).collect::<Vec<_>>()
        } else {
            vec![0, 1, 8, encoded.len() - 1]
        };
        for offset in corrupt_offsets {
            let mut corrupt = encoded.clone();
            corrupt[offset] ^= 0x40;
            assert!(
                RetainedPayload::decode(&corrupt, bytes.len()).is_err(),
                "offset {offset}"
            );
        }
        assert!(RetainedPayload::decode(&encoded[..encoded.len() - 1], bytes.len()).is_err());
        let mut extended = encoded;
        extended.push(0);
        assert!(RetainedPayload::decode(&extended, bytes.len()).is_err());
    }
}

#[test]
fn given_incorrect_declared_length_or_native_capacity_when_encoding_then_limits_remain_closed() {
    let emit = |output: &mut dyn Write| {
        output
            .write_all(b"abc")
            .map_err(|_| storage_types::StorageError::internal("test emit failed"))
    };
    assert!(RetainedPayload::encode_with(2, 256, emit).is_err());
    assert!(RetainedPayload::encode_with(4, 256, emit).is_err());
    for maximum in 0..9 {
        assert!(RetainedPayload::encode(b"abc", maximum).is_err());
    }
}

#[test]
fn given_equal_small_records_when_encoding_and_decoding_then_report_codec_cost() {
    const ITERATIONS: usize = 128;
    let bytes = vec![b'x'; 768];
    let started = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let encoded = compressed_payload(&bytes);
        assert_eq!(
            RetainedPayload::decode(&encoded, bytes.len()).unwrap(),
            bytes
        );
    }
    let compressed = started.elapsed();
    let started = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let encoded = RetainedPayload::encode(&bytes, 2048).unwrap();
        assert_eq!(
            RetainedPayload::decode(&encoded, bytes.len()).unwrap(),
            bytes
        );
    }
    let raw = started.elapsed();
    eprintln!(
        "retained_payload_runtime_report {}",
        serde_json::json!({
            "iterations": ITERATIONS, "decoded_bytes": bytes.len(),
            "compressed_ns": compressed.as_nanos(), "raw_ns": raw.as_nanos(),
        })
    );
}
