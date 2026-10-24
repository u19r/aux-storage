use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use storage_types::StorageResult;

use super::key_layout::{decode_key, encode_key};

#[test]
fn given_zero_segments_and_maximum_generation_when_encoded_then_layout_one_golden_bytes_hold()
-> StorageResult<()> {
    let key = key("n", &[0, 255], u64::MAX, &[0, 1])?;
    let mut expected = b"\x7fi/r/n\0\0\0\xff\xff\0\0".to_vec();
    expected.extend_from_slice(&u64::MAX.to_be_bytes());
    expected.extend_from_slice(&[0, 255, 1, 0, 0]);
    assert_eq!(expected, encode_key(&key)?);
    assert_eq!(key, decode_key(&expected)?);
    Ok(())
}

#[test]
fn given_all_address_axes_when_encoded_then_native_order_matches_raw_tuple_order()
-> StorageResult<()> {
    let mut keys = Vec::new();
    for namespace in ["a", "aé", "b"] {
        for resource in [&[0][..], &[0, 0], &[0, 255], &[1], &[255]] {
            for generation in [0, 1, 1_u64 << 63, u64::MAX] {
                for suffix in [&[][..], &[0], &[0, 0], &[0, 255], &[1], &[255]] {
                    keys.push(key(namespace, resource, generation, suffix)?);
                }
            }
        }
    }
    let mut encoded = keys
        .iter()
        .map(encode_key)
        .collect::<StorageResult<Vec<_>>>()?;
    keys.sort();
    encoded.sort();
    let decoded = encoded
        .iter()
        .map(|bytes| decode_key(bytes))
        .collect::<StorageResult<Vec<_>>>()?;
    assert_eq!(keys, decoded);
    Ok(())
}

#[test]
fn given_malformed_tuple_keys_when_decoded_then_no_ambiguous_address_is_returned()
-> StorageResult<()> {
    let valid = encode_key(&key("n", b"r", 1, b"s")?)?;
    for length in 0..valid.len() {
        assert!(decode_key(&valid[..length]).is_err());
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(decode_key(&trailing).is_err());
    let mut invalid_escape = valid.clone();
    invalid_escape[7] = 1;
    assert!(decode_key(&invalid_escape).is_err());
    let mut invalid_utf8 = valid;
    invalid_utf8[5] = 255;
    assert!(decode_key(&invalid_utf8).is_err());
    Ok(())
}

fn key(
    namespace: &str,
    resource: &[u8],
    generation: u64,
    suffix: &[u8],
) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: namespace.into(),
        resource: resource.into(),
        generation,
        suffix: suffix.into(),
    }
    .try_into()
}
