use std::borrow::Cow;

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use serde::{Deserializer, Serializer, de, ser::SerializeMap};

pub use crate::constants::SENTINEL_KEY;
use crate::{StorageError, StorageResult};

pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    serialize_inner(bytes, serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where D: Deserializer<'de> {
    deserializer.deserialize_any(BinaryVisitor)
}

pub fn serialize_option<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    match bytes {
        Some(inner) => serialize_inner(inner, serializer),
        None => serializer.serialize_none(),
    }
}

/// Encode an optional binary entity field directly as a Dynamo attribute.
pub fn encode_optional(
    bytes: &Option<Vec<u8>>,
    _field: &str,
) -> StorageResult<crate::AttributeValue> {
    Ok(match bytes {
        Some(bytes) => crate::AttributeValue::B(STANDARD.encode(bytes)),
        None => crate::AttributeValue::NULL(true),
    })
}

pub fn deserialize_option<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where D: Deserializer<'de> {
    deserializer.deserialize_option(OptionalBinaryVisitor)
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

struct BinaryVisitor;

impl<'de> de::Visitor<'de> for BinaryVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a DynamoDB binary string or sentinel object")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where E: de::Error {
        decode_encoded(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where E: de::Error {
        decode_encoded(&value)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where E: de::Error {
        Err(E::custom("binary field cannot be null"))
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where E: de::Error {
        Err(E::custom(format!(
            "unsupported JSON value for DynamoDB binary field: {value}"
        )))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where E: de::Error {
        Err(E::custom(format!(
            "unsupported JSON value for DynamoDB binary field: {value}"
        )))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where E: de::Error {
        Err(E::custom(format!(
            "unsupported JSON value for DynamoDB binary field: {value}"
        )))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where E: de::Error {
        Err(E::custom(format!(
            "unsupported JSON value for DynamoDB binary field: {value}"
        )))
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: de::MapAccess<'de> {
        let Some(key) = map.next_key::<Cow<'de, str>>()? else {
            return Err(de::Error::custom(
                "expected DynamoDB binary sentinel object",
            ));
        };
        if key != SENTINEL_KEY {
            return Err(de::Error::custom(
                "expected DynamoDB binary sentinel object",
            ));
        }
        let encoded = map
            .next_value::<Cow<'de, str>>()
            .map_err(|_| de::Error::custom("expected DynamoDB binary sentinel object"))?;
        if map
            .next_entry::<de::IgnoredAny, de::IgnoredAny>()?
            .is_some()
        {
            return Err(de::Error::custom(
                "expected DynamoDB binary sentinel object",
            ));
        }
        decode_encoded(encoded.as_ref())
    }
}

struct OptionalBinaryVisitor;

impl<'de> de::Visitor<'de> for OptionalBinaryVisitor {
    type Value = Option<Vec<u8>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an optional DynamoDB binary value")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where E: de::Error {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_any(BinaryVisitor).map(Some)
    }
}

fn decode_encoded<E>(encoded: &str) -> Result<Vec<u8>, E>
where E: de::Error {
    decode_base64(encoded)
        .map_err(|err| E::custom(format!("invalid base64 for DynamoDB binary field: {err}")))
}

fn decode_base64(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    STANDARD
        .decode(input)
        .or_else(|_| STANDARD_NO_PAD.decode(input))
}
