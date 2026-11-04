use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use scheduled_bg_jobs::store::StoreKey;
use serde::Serialize;
use storage_provider::RawOrderedJobStoreError;
use storage_types::{
    AttributeMap, AttributeValue, KeyAttributes, StorageError, StorageResult, WireItem,
};

use crate::provider::scheduled_job_store::constants::{
    DATA_KEY_PREFIX, KEY_ATTRIBUTE, MAX_KEY_BYTES, MAX_VALUE_BYTES, PARTITION_KEY, SORT_ATTRIBUTE,
    VALUE_ATTRIBUTE,
};

pub(crate) fn data_sort_key(key: &StoreKey) -> String {
    format!("{DATA_KEY_PREFIX}{}", hex_encode(key.as_bytes()))
}

pub(crate) fn partition_key() -> AttributeValue {
    AttributeValue::S(PARTITION_KEY.to_string())
}

pub(crate) fn key_attributes(sort_key: String) -> KeyAttributes {
    KeyAttributes::from([
        (KEY_ATTRIBUTE.to_string(), partition_key()),
        (SORT_ATTRIBUTE.to_string(), AttributeValue::S(sort_key)),
    ])
}

pub(crate) fn value_attribute(value: &[u8]) -> AttributeValue {
    AttributeValue::B(STANDARD.encode(value))
}

#[derive(Serialize)]
struct DataRow {
    pk: AttributeValue,
    sk: AttributeValue,
    v: AttributeValue,
}

pub(crate) fn data_item(key: &StoreKey, value: &[u8]) -> StorageResult<WireItem> {
    let row = DataRow {
        pk: partition_key(),
        sk: AttributeValue::S(data_sort_key(key)),
        v: value_attribute(value),
    };
    serde_json::to_vec(&row)
        .map(WireItem::dynamo_json)
        .map_err(|_| StorageError::internal("scheduled data row cannot be encoded"))
}

pub(crate) fn decode_data_item(
    item: &AttributeMap,
) -> Result<(StoreKey, Bytes), RawOrderedJobStoreError> {
    if item.get(KEY_ATTRIBUTE) != Some(&partition_key()) {
        return Err(corrupt("scheduled data row has an invalid partition key"));
    }
    let sort_key = string_attribute(item, SORT_ATTRIBUTE)?;
    let Some(encoded) = sort_key.strip_prefix(DATA_KEY_PREFIX) else {
        return Err(corrupt("scheduled data row has an invalid sort key"));
    };
    let key = decode_hex_key(encoded)?;
    let value = decode_value(item, VALUE_ATTRIBUTE)?;
    Ok((key, value))
}

pub(crate) fn string_attribute<'a>(
    item: &'a AttributeMap,
    name: &'static str,
) -> Result<&'a str, RawOrderedJobStoreError> {
    let Some(AttributeValue::S(value)) = item.get(name) else {
        return Err(corrupt("scheduled row is missing a string attribute"));
    };
    Ok(value)
}

fn decode_value(item: &AttributeMap, name: &'static str) -> Result<Bytes, RawOrderedJobStoreError> {
    decode_binary_attribute(item, name, MAX_VALUE_BYTES).map(Bytes::from)
}

fn decode_binary_attribute(
    item: &AttributeMap,
    name: &'static str,
    max_bytes: usize,
) -> Result<Vec<u8>, RawOrderedJobStoreError> {
    let Some(AttributeValue::B(encoded)) = item.get(name) else {
        return Err(corrupt("scheduled row is missing a binary attribute"));
    };
    if encoded.len() > max_bytes.div_ceil(3) * 4 {
        return Err(corrupt(
            "scheduled row encoded value exceeds the reserved limit",
        ));
    }
    let decoded = storage_types::dynamodb_binary::decode_base64_string(encoded)
        .map_err(|_| corrupt("scheduled row has invalid bytes"))?;
    if decoded.len() > max_bytes {
        return Err(corrupt("scheduled row value exceeds the reserved limit"));
    }
    Ok(decoded)
}

fn decode_hex_key(encoded: &str) -> Result<StoreKey, RawOrderedJobStoreError> {
    let decoded =
        hex_decode(encoded).ok_or_else(|| corrupt("scheduled row has invalid key bytes"))?;
    if decoded.is_empty() || decoded.len() > MAX_KEY_BYTES {
        return Err(corrupt("scheduled row key exceeds the reserved limit"));
    }
    StoreKey::try_from(decoded).map_err(|_| corrupt("scheduled row key is empty"))
}

pub(crate) fn decode_continuation(
    key: &KeyAttributes,
) -> Result<StoreKey, RawOrderedJobStoreError> {
    let Some(AttributeValue::S(partition)) = key.get(KEY_ATTRIBUTE) else {
        return Err(corrupt(
            "scheduled range cursor is missing its partition key",
        ));
    };
    if partition != PARTITION_KEY {
        return Err(corrupt(
            "scheduled range cursor has an invalid partition key",
        ));
    }
    let Some(AttributeValue::S(sort_key)) = key.get(SORT_ATTRIBUTE) else {
        return Err(corrupt("scheduled range cursor is missing its sort key"));
    };
    let Some(encoded) = sort_key.strip_prefix(DATA_KEY_PREFIX) else {
        return Err(corrupt("scheduled range cursor is outside the data range"));
    };
    decode_hex_key(encoded)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        output.push((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?);
    }
    Some(output)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn corrupt(message: &'static str) -> RawOrderedJobStoreError {
    RawOrderedJobStoreError::Corrupt(message)
}
