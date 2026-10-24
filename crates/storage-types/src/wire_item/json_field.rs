use serde::de::DeserializeOwned;

use super::{WireAttributeDecode, WireItem};
use crate::{StorageError, StorageResult};

pub fn decode_wire_field<T>(item: &WireItem, raw: Option<&str>, field: &str) -> StorageResult<T>
where T: WireAttributeDecode + DeserializeOwned {
    if raw.is_some() {
        return T::decode(raw, field);
    }
    match encode_field_json(item, field)? {
        Some(json) => parse_field_json(&json, field),
        None => T::decode(None, field),
    }
}

pub fn decode_wire_field_json<T>(
    item: &WireItem,
    raw: Option<&str>,
    field: &str,
) -> StorageResult<T>
where
    T: DeserializeOwned,
{
    if let Some(raw) = raw {
        if let Ok(decoded) = serde_json::from_str(raw) {
            return Ok(decoded);
        }
        return decode_quoted_scalar_json(raw, field);
    }
    match encode_field_json(item, field)? {
        Some(json) => parse_field_json(&json, field),
        None => serde_json::from_str("null").map_err(|_| missing_field_error(field)),
    }
}

pub fn decode_wire_serde_string<T>(raw: &str, field: &str) -> StorageResult<T>
where T: DeserializeOwned {
    decode_quoted_scalar_json(raw, field)
        .map_err(|err| StorageError::internal(&format!("invalid {field} field: {err}")))
}

fn encode_field_json(item: &WireItem, field: &str) -> StorageResult<Option<String>> {
    item.attribute_value(field)?
        .map(|value| {
            value.to_plain_json().map_err(|err| {
                StorageError::internal(&format!(
                    "decode wire attribute {field} into plain json failed: {err}"
                ))
            })
        })
        .transpose()
}

fn decode_quoted_scalar_json<T>(raw: &str, field: &str) -> StorageResult<T>
where T: DeserializeOwned {
    let quoted = quote_scalar_json(raw, field)?;
    parse_field_json(&quoted, field)
}

fn quote_scalar_json(raw: &str, field: &str) -> StorageResult<String> {
    serde_json::to_string(raw).map_err(|err| {
        StorageError::internal(&format!(
            "decode wire attribute {field} scalar quoting failed: {err}"
        ))
    })
}

fn parse_field_json<T: DeserializeOwned>(json: &str, field: &str) -> StorageResult<T> {
    serde_json::from_str(json).map_err(|err| invalid_field_error(field, &err))
}

#[cold]
#[inline(never)]
fn invalid_field_error(field: &str, err: &serde_json::Error) -> StorageError {
    StorageError::internal(&format!("decode wire attribute {field} failed: {err}"))
}

#[cold]
#[inline(never)]
fn missing_field_error(field: &str) -> StorageError {
    StorageError::internal(&format!("missing required field {field}"))
}
