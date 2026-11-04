use bytes::Bytes;
use scheduled_bg_jobs::store::StoreKey;
use storage_types::{AttributeMap, AttributeValue, KeyAttributes};

use super::encoding::{data_item, decode_continuation, decode_data_item};
use crate::provider::scheduled_job_store::constants::{
    KEY_ATTRIBUTE, PARTITION_KEY, SORT_ATTRIBUTE, VALUE_ATTRIBUTE,
};

#[test]
fn given_binary_data_when_encoding_then_dynamodb_base64_round_trips() {
    let key = StoreKey::try_from(b"key".as_slice()).expect("key is valid");
    let item = AttributeMap::try_from(data_item(&key, b"value").expect("encode row"))
        .expect("decode wire row");
    assert_eq!(
        item.get(VALUE_ATTRIBUTE),
        Some(&AttributeValue::B("dmFsdWU=".to_string()))
    );
    let decoded = decode_data_item(&item).expect("valid data row");
    assert_eq!(decoded, (key, Bytes::from_static(b"value")));
}

#[test]
fn given_a_foreign_partition_when_decoding_data_then_the_row_is_rejected() {
    let mut item = AttributeMap::try_from(
        data_item(&StoreKey::try_from(b"key".as_slice()).unwrap(), b"value").expect("encode row"),
    )
    .expect("decode wire row");
    item.insert(
        KEY_ATTRIBUTE.to_string(),
        AttributeValue::S("other-partition".to_string()),
    );
    assert!(decode_data_item(&item).is_err());
}

#[test]
fn given_a_non_data_cursor_when_decoding_a_range_then_the_cursor_is_rejected() {
    let key = KeyAttributes::from([
        (
            KEY_ATTRIBUTE.to_string(),
            AttributeValue::S(PARTITION_KEY.to_string()),
        ),
        (
            SORT_ATTRIBUTE.to_string(),
            AttributeValue::S("R#receipt".to_string()),
        ),
    ]);
    assert!(decode_continuation(&key).is_err());
}
