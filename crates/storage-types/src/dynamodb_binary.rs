use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use serde::{Deserialize, Deserializer, Serializer, ser::SerializeMap};

use crate::{StorageError, StorageResult};

pub const SENTINEL_KEY: &str = "__dynamodb_binary";

pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    serialize_inner(bytes, serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where D: Deserializer<'de> {
    let value = serde_json::Value::deserialize(deserializer)?;
    decode_value(value)
}

pub fn serialize_option<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    match bytes {
        Some(inner) => serialize_inner(inner, serializer),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize_option<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where D: Deserializer<'de> {
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    value.map(decode_value).transpose()
}

pub fn decode_base64_string(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    decode_base64(input)
}

pub fn parse_required_dynamo_binary(raw: Option<&str>, field: &str) -> StorageResult<Vec<u8>> {
    let payload_b64 =
        raw.ok_or_else(|| StorageError::internal(&format!("missing required field {field}")))?;
    decode_base64(payload_b64).map_err(|err| {
        StorageError::internal(&format!("invalid {field} field binary payload: {err}"))
    })
}

fn serialize_inner<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    let encoded = STANDARD.encode(bytes);
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(SENTINEL_KEY, &encoded)?;
    map.end()
}

fn decode_value<E>(value: serde_json::Value) -> Result<Vec<u8>, E>
where E: serde::de::Error {
    match value {
        serde_json::Value::String(s) => decode_base64(&s)
            .map_err(|err| E::custom(format!("invalid base64 for DynamoDB binary field: {err}"))),
        serde_json::Value::Object(mut obj) => {
            if obj.len() == 1
                && let Some(serde_json::Value::String(s)) = obj.remove(SENTINEL_KEY)
            {
                return decode_base64(&s).map_err(|err| {
                    E::custom(format!("invalid base64 for DynamoDB binary field: {err}"))
                });
            }
            Err(E::custom("expected DynamoDB binary sentinel object"))
        }
        serde_json::Value::Null => Err(E::custom("binary field cannot be null")),
        other => Err(E::custom(format!(
            "unsupported JSON value for DynamoDB binary field: {other}"
        ))),
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    STANDARD
        .decode(input)
        .or_else(|_| STANDARD_NO_PAD.decode(input))
}
