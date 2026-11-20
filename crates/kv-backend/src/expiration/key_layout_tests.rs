use storage_common::TtlIndexKey;

use super::key_layout::*;

#[test]
fn given_signed_expiry_keys_when_encoding_then_existing_kv_bytes_are_unchanged() {
    for seconds in [i64::MIN, -1, 0, 1, i64::MAX] {
        for token in ["a", "aa", "é", "表/\0"] {
            let key = TtlIndexKey::new(seconds, token.to_owned().into());
            let actual = encode_index_key(0x0102_0304, &key).unwrap();
            let mut expected = vec![b'T', 1, 2, 3, 4, b'e', 0];
            let mut ordered_seconds = seconds.to_be_bytes();
            ordered_seconds[0] ^= 0x80;
            expected.extend_from_slice(&ordered_seconds);
            expected.extend_from_slice(token.as_bytes());
            assert_eq!(actual, expected);
            let decoded = decode_index_key(0x0102_0304, &actual).unwrap();
            assert_eq!(decoded.indexed_expiration_seconds, seconds);
            assert_eq!(decoded.ttl_item_key, token);
        }
    }
}
