use std::borrow::Cow;

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    STORAGE_SERDE_RAW_JSON_LIMIT_BYTES, StorageEnum, StorageError, StorageResult,
    constants::{
        LZ4_JSON_PREFIX, RAW_JSON_PREFIX, STORAGE_SERDE_ENCODED_PREFIX_LEN,
        STORAGE_SERDE_MIN_COMPRESSION_SAVINGS_BYTES, STORAGE_SERDE_MIN_COMPRESSION_SAVINGS_DIVISOR,
    },
};

pub fn to_bytes<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    let json = serde_json::to_vec(value).map_err(StorageEnum::from)?;
    let json_len = json.len();
    if json_len <= STORAGE_SERDE_RAW_JSON_LIMIT_BYTES {
        return Ok(tagged_json_bytes(RAW_JSON_PREFIX, &json));
    }

    let compressed = lz4_flex::compress_prepend_size(&json);
    if should_keep_compressed(json_len, compressed.len()) {
        Ok(tagged_json_bytes(LZ4_JSON_PREFIX, &compressed))
    } else {
        Ok(tagged_json_bytes(RAW_JSON_PREFIX, &json))
    }
}

#[must_use]
pub fn compress_json_bytes(json: &[u8]) -> Vec<u8> {
    if should_store_raw_without_compression(json) {
        return tagged_json_bytes(RAW_JSON_PREFIX, json);
    }

    let compressed = lz4_flex::compress_prepend_size(json);
    if should_keep_compressed(json.len(), compressed.len()) {
        tagged_json_bytes(LZ4_JSON_PREFIX, &compressed)
    } else {
        tagged_json_bytes(RAW_JSON_PREFIX, json)
    }
}

pub fn decompress_bytes(bytes: &[u8]) -> StorageResult<Vec<u8>> {
    decompress_payload(bytes).map(Cow::into_owned)
}

pub fn decompress_owned_bytes(mut bytes: Vec<u8>) -> StorageResult<Vec<u8>> {
    if bytes.starts_with(&RAW_JSON_PREFIX) {
        bytes.drain(..STORAGE_SERDE_ENCODED_PREFIX_LEN);
        return Ok(bytes);
    }

    if bytes.starts_with(&LZ4_JSON_PREFIX) {
        return decompress_lz4_json_bytes(&bytes[STORAGE_SERDE_ENCODED_PREFIX_LEN..]);
    }

    Err(StorageError::internal(
        "storage serde: unknown format header",
    ))
}

pub fn from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    let decompressed = decompress_payload(bytes)?;
    let value = serde_json::from_slice::<T>(&decompressed).map_err(StorageEnum::from)?;
    Ok(value)
}

fn decompress_payload(bytes: &[u8]) -> StorageResult<Cow<'_, [u8]>> {
    if let Some(encoded) = bytes.strip_prefix(&RAW_JSON_PREFIX) {
        return Ok(Cow::Borrowed(encoded));
    }

    if let Some(encoded) = bytes.strip_prefix(&LZ4_JSON_PREFIX) {
        return decompress_lz4_json_bytes(encoded).map(Cow::Owned);
    }

    Err(StorageError::internal(
        "storage serde: unknown format header",
    ))
}

fn tagged_json_bytes(prefix: [u8; STORAGE_SERDE_ENCODED_PREFIX_LEN], json: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(STORAGE_SERDE_ENCODED_PREFIX_LEN + json.len());
    bytes.extend_from_slice(&prefix);
    bytes.extend_from_slice(json);
    bytes
}

fn should_store_raw_without_compression(json: &[u8]) -> bool {
    json.len() <= STORAGE_SERDE_RAW_JSON_LIMIT_BYTES
}

fn should_keep_compressed(original_len: usize, compressed_len: usize) -> bool {
    compressed_len + STORAGE_SERDE_ENCODED_PREFIX_LEN < original_len
        && original_len.saturating_sub(compressed_len)
            >= STORAGE_SERDE_MIN_COMPRESSION_SAVINGS_BYTES
        && compressed_len
            <= original_len - (original_len / STORAGE_SERDE_MIN_COMPRESSION_SAVINGS_DIVISOR)
}

fn decompress_lz4_json_bytes(bytes: &[u8]) -> StorageResult<Vec<u8>> {
    lz4_flex::decompress_size_prepended(bytes)
        .map_err(|e| StorageError::internal(&format!("lz4 decompress failed: {e}")))
}
