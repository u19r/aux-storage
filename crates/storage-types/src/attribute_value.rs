use std::collections::{BTreeMap, HashMap};

use serde::{
    Deserialize, Serialize,
    ser::{SerializeMap as _, SerializeSeq as _},
};
use serde_json::Value as JsonValue;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    dynamodb_binary::SENTINEL_KEY,
    single_table_entity::{
        CREATED_AT_ALIAS_ATTR, CREATED_AT_ATTR, EXPIRES_AT_ALIAS_ATTR, EXPIRES_AT_ATTR,
        UPDATED_AT_ALIAS_ATTR, UPDATED_AT_ATTR,
    },
};

#[derive(Debug, Clone, ToSchema, PartialEq)]
#[schema(no_recursion)]
pub enum AttributeValue {
    S(String),
    N(String),
    B(String),
    SS(Vec<String>),
    NS(Vec<String>),
    BS(Vec<String>),
    BOOL(bool),
    NULL(bool),
    L(Vec<AttributeValue>),
    M(HashMap<String, AttributeValue>),
}

impl AttributeValue {
    pub fn inner_str(&self) -> Result<&str, ConversionError> {
        use AttributeValue as AV;
        match self {
            AV::S(s) | AV::B(s) | AV::N(s) => Ok(s.as_str()),
            _ => Err(ConversionError::TypeMismatch {
                expected: "S/N/B scalar".to_string(),
                got: self.variant_name().to_string(),
            }),
        }
    }

    pub fn inner_string(&self) -> Result<String, ConversionError> {
        use AttributeValue as AV;
        match self {
            AV::S(s) | AV::B(s) | AV::N(s) => Ok(s.clone()),
            AV::BOOL(s) => Ok(s.to_string()),
            _ => Err(ConversionError::TypeMismatch {
                expected: "scalar".to_string(),
                got: self.variant_name().to_string(),
            }),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            AttributeValue::S(_) => "S",
            AttributeValue::N(_) => "N",
            AttributeValue::B(_) => "B",
            AttributeValue::SS(_) => "SS",
            AttributeValue::NS(_) => "NS",
            AttributeValue::BS(_) => "BS",
            AttributeValue::BOOL(_) => "BOOL",
            AttributeValue::NULL(_) => "NULL",
            AttributeValue::L(_) => "L",
            AttributeValue::M(_) => "M",
        }
    }
}

/// Convert a plain `serde_json::Value` into a Dynamo-style `AttributeValue`.
///
/// This is the write-path mirror of `to_hashmap` field conversion and is used
/// by typed wire-item encoders to avoid constructing intermediary
/// `HashMap<String, AttributeValue>` objects.
pub fn attribute_value_from_json_value(
    value: JsonValue,
) -> Result<AttributeValue, ConversionError> {
    AttributeValue::from_json_value(value)
}

pub fn canonical_dynamo_json(value: &AttributeValue) -> Result<String, ConversionError> {
    serde_json::to_string(value).map_err(|err| ConversionError::Serialization(err.to_string()))
}

pub fn canonical_dynamo_map_json(
    map: &HashMap<String, AttributeValue>,
) -> Result<String, ConversionError> {
    let ordered = map
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&ordered).map_err(|err| ConversionError::Serialization(err.to_string()))
}

impl Serialize for AttributeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            AttributeValue::S(value) => map.serialize_entry("S", value)?,
            AttributeValue::N(value) => map.serialize_entry("N", value)?,
            AttributeValue::B(value) => map.serialize_entry("B", value)?,
            AttributeValue::SS(value) => map.serialize_entry("SS", value)?,
            AttributeValue::NS(value) => map.serialize_entry("NS", value)?,
            AttributeValue::BS(value) => map.serialize_entry("BS", value)?,
            AttributeValue::BOOL(value) => map.serialize_entry("BOOL", value)?,
            AttributeValue::NULL(value) => map.serialize_entry("NULL", value)?,
            AttributeValue::L(value) => map.serialize_entry("L", value)?,
            AttributeValue::M(value) => map.serialize_entry("M", value)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        use std::fmt;

        use serde::de::{MapAccess, Visitor};

        struct AttributeValueVisitor;

        impl<'de> Visitor<'de> for AttributeValueVisitor {
            type Value = AttributeValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a DynamoDB AttributeValue object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<AttributeValue, M::Error>
            where M: MapAccess<'de> {
                if let Some(key) = map.next_key::<String>()? {
                    let value = match key.as_str() {
                        "S" => AttributeValue::S(map.next_value()?),
                        "N" => AttributeValue::N(map.next_value()?),
                        "B" => AttributeValue::B(map.next_value()?),
                        "SS" => AttributeValue::SS(map.next_value()?),
                        "NS" => AttributeValue::NS(map.next_value()?),
                        "BS" => AttributeValue::BS(map.next_value()?),
                        "BOOL" => AttributeValue::BOOL(map.next_value()?),
                        "NULL" => AttributeValue::NULL(map.next_value()?),
                        "L" => AttributeValue::L(map.next_value()?),
                        "M" => AttributeValue::M(map.next_value()?),
                        _ => {
                            return Err(serde::de::Error::unknown_field(
                                &key,
                                &["S", "N", "B", "SS", "NS", "BS", "BOOL", "NULL", "L", "M"],
                            ));
                        }
                    };

                    // Ensure there are no additional fields
                    if map.next_key::<String>()?.is_some() {
                        return Err(serde::de::Error::custom(
                            "AttributeValue must have exactly one field",
                        ));
                    }

                    Ok(value)
                } else {
                    Err(serde::de::Error::custom("AttributeValue cannot be empty"))
                }
            }
        }

        deserializer.deserialize_map(AttributeValueVisitor)
    }
}

/// Error type for conversion between structs and `HashMap`<AttributeValue>
#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
}

impl AttributeValue {
    /// Convert a `serde_json::Value` to `AttributeValue`
    fn from_json_value(value: JsonValue) -> Result<Self, ConversionError> {
        match value {
            JsonValue::String(s) => Ok(AttributeValue::S(s)),
            JsonValue::Number(n) => Ok(AttributeValue::N(n.to_string())),
            JsonValue::Bool(b) => Ok(AttributeValue::BOOL(b)),
            JsonValue::Null => Ok(AttributeValue::NULL(true)),

            JsonValue::Array(arr) => {
                let mut items = Vec::with_capacity(arr.len());
                for item in arr {
                    items.push(Self::from_json_value(item)?);
                }
                Ok(AttributeValue::L(items))
            }
            JsonValue::Object(obj) => {
                if obj.len() == 1
                    && let Some(JsonValue::String(encoded)) = obj.get(SENTINEL_KEY)
                {
                    return Ok(AttributeValue::B(encoded.clone()));
                }
                let mut map = HashMap::with_capacity(obj.len());
                for (key, value) in obj {
                    map.insert(key, Self::from_json_value(value)?);
                }
                Ok(AttributeValue::M(map))
            }
        }
    }

    /// Convert `AttributeValue` to a plain JSON string without `DynamoDB`
    /// structure
    pub fn to_plain_json(&self) -> Result<String, ConversionError> {
        serde_json::to_string(&PlainJsonAttributeValue(self))
            .map_err(|e| ConversionError::Serialization(e.to_string()))
    }

    /// Convert `AttributeValue` to `serde_json::Value`
    pub fn to_json_value(&self) -> Result<JsonValue, ConversionError> {
        match self {
            AttributeValue::S(s) => Ok(JsonValue::String(s.clone())),
            AttributeValue::N(n) => {
                // Try to parse as number, fallback to string
                if let Ok(num) = n.parse::<serde_json::Number>() {
                    Ok(JsonValue::Number(num))
                } else {
                    Ok(JsonValue::String(n.clone()))
                }
            }
            AttributeValue::B(b) => Ok(JsonValue::String(b.clone())),
            AttributeValue::SS(ss) => Ok(JsonValue::Array(
                ss.iter().map(|s| JsonValue::String(s.clone())).collect(),
            )),
            AttributeValue::NS(ns) => Ok(JsonValue::Array(
                ns.iter()
                    .map(|n| {
                        if let Ok(num) = n.parse::<serde_json::Number>() {
                            JsonValue::Number(num)
                        } else {
                            JsonValue::String(n.clone())
                        }
                    })
                    .collect(),
            )),
            AttributeValue::BS(bs) => Ok(JsonValue::Array(
                bs.iter().map(|b| JsonValue::String(b.clone())).collect(),
            )),
            AttributeValue::BOOL(b) => Ok(JsonValue::Bool(*b)),
            AttributeValue::NULL(n) => {
                if *n {
                    Ok(JsonValue::Null)
                } else {
                    Err(ConversionError::UnsupportedType("NULL(false)".to_string()))
                }
            }
            AttributeValue::L(list) => {
                let mut items = Vec::with_capacity(list.len());
                for item in list {
                    items.push(item.to_json_value()?);
                }
                Ok(JsonValue::Array(items))
            }
            AttributeValue::M(map) => {
                let mut obj = serde_json::Map::with_capacity(map.len());
                let ordered: BTreeMap<_, _> = map.iter().collect();
                for (key, value) in ordered {
                    obj.insert(key.clone(), value.to_json_value()?);
                }
                Ok(JsonValue::Object(obj))
            }
        }
    }
}

struct PlainJsonAttributeValue<'a>(&'a AttributeValue);

impl Serialize for PlainJsonAttributeValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self.0 {
            AttributeValue::S(value) | AttributeValue::B(value) => serializer.serialize_str(value),
            AttributeValue::N(value) => {
                if let Ok(num) = value.parse::<serde_json::Number>() {
                    num.serialize(serializer)
                } else {
                    serializer.serialize_str(value)
                }
            }
            AttributeValue::SS(values) | AttributeValue::BS(values) => values.serialize(serializer),
            AttributeValue::NS(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    if let Ok(num) = value.parse::<serde_json::Number>() {
                        seq.serialize_element(&num)?;
                    } else {
                        seq.serialize_element(value)?;
                    }
                }
                seq.end()
            }
            AttributeValue::BOOL(value) => serializer.serialize_bool(*value),
            AttributeValue::NULL(is_null) => {
                if *is_null {
                    serializer.serialize_none()
                } else {
                    Err(serde::ser::Error::custom("NULL(false)"))
                }
            }
            AttributeValue::L(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&PlainJsonAttributeValue(item))?;
                }
                seq.end()
            }
            AttributeValue::M(map) => {
                let mut keys = map.keys().collect::<Vec<_>>();
                keys.sort();
                let mut json_map = serializer.serialize_map(Some(keys.len()))?;
                for key in keys {
                    if let Some(value) = map.get(key) {
                        json_map.serialize_entry(key, &PlainJsonAttributeValue(value))?;
                    }
                }
                json_map.end()
            }
        }
    }
}

/// Convert a Rust struct to `HashMap`<String, `AttributeValue`>
pub fn to_hashmap<T>(item: &T) -> Result<HashMap<String, AttributeValue>, ConversionError>
where T: Serialize {
    let json_value =
        serde_json::to_value(item).map_err(|e| ConversionError::Serialization(e.to_string()))?;

    match json_value {
        JsonValue::Object(obj) => {
            let mut map = HashMap::new();
            for (key, value) in obj {
                map.insert(key, AttributeValue::from_json_value(value)?);
            }
            Ok(map)
        }
        _ => Err(ConversionError::TypeMismatch {
            expected: "Object".to_string(),
            got: format!("{json_value:?}"),
        }),
    }
}

/// Convert `HashMap`<String, `AttributeValue`> to a Rust struct
pub fn from_hashmap<T>(map: HashMap<String, AttributeValue>) -> Result<T, ConversionError>
where T: for<'de> Deserialize<'de> {
    let mut obj = serde_json::Map::new();
    for (key, attr_value) in map {
        obj.insert(key, attr_value.to_json_value()?);
    }
    normalize_timestamp_attribute_aliases(&mut obj);

    serde_json::from_value(JsonValue::Object(obj))
        .map_err(|e| ConversionError::Deserialization(e.to_string()))
}

fn normalize_timestamp_attribute_aliases(obj: &mut serde_json::Map<String, JsonValue>) {
    normalize_timestamp_attribute_alias(obj, CREATED_AT_ALIAS_ATTR, CREATED_AT_ATTR);
    normalize_timestamp_attribute_alias(obj, UPDATED_AT_ALIAS_ATTR, UPDATED_AT_ATTR);
    normalize_timestamp_attribute_alias(obj, EXPIRES_AT_ALIAS_ATTR, EXPIRES_AT_ATTR);
}

fn normalize_timestamp_attribute_alias(
    obj: &mut serde_json::Map<String, JsonValue>,
    short_name: &str,
    long_name: &str,
) {
    if obj.contains_key(long_name) {
        let _ = obj.remove(short_name);
        return;
    }
    if let Some(value) = obj.remove(short_name) {
        obj.insert(long_name.to_string(), value);
    }
}
