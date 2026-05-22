use std::borrow::Cow;

use serde::{
    Deserialize,
    de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor},
};

use crate::{AttributeValue, StorageEnum, StorageError, StorageResult};

struct ScalarFieldSeed<'a> {
    fields: &'a [&'a str],
}

impl<'de> DeserializeSeed<'de> for ScalarFieldSeed<'_> {
    type Value = Vec<Option<Cow<'de, str>>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(ScalarFieldVisitor {
            fields: self.fields,
        })
    }
}

struct ScalarFieldVisitor<'a> {
    fields: &'a [&'a str],
}

impl<'de> Visitor<'de> for ScalarFieldVisitor<'_> {
    type Value = Vec<Option<Cow<'de, str>>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut values = vec![None; self.fields.len()];
        while let Some(key) = map.next_key::<&str>()? {
            if let Some(index) = self.fields.iter().position(|field| *field == key) {
                let value = map.next_value::<BorrowedScalarAttribute<'de>>()?;
                if let Some(scalar) = value.into_scalar() {
                    values[index] = Some(scalar);
                }
                continue;
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(values)
    }
}

#[derive(Deserialize)]
struct BorrowedScalarAttribute<'a> {
    #[serde(rename = "S", default, borrow)]
    string: Option<Cow<'a, str>>,
    #[serde(rename = "N", default, borrow)]
    number: Option<Cow<'a, str>>,
    #[serde(rename = "B", default, borrow)]
    binary: Option<Cow<'a, str>>,
}

impl<'a> BorrowedScalarAttribute<'a> {
    fn into_scalar(self) -> Option<Cow<'a, str>> {
        if let Some(value) = self.string {
            return Some(value);
        }
        if let Some(value) = self.number {
            return Some(value);
        }
        self.binary
    }
}

pub(super) fn parse_dynamo_scalar_fields<'a>(
    bytes: &'a [u8],
    fields: &[&str],
) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    ScalarFieldSeed { fields }
        .deserialize(&mut deserializer)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))
}

struct NumberFieldSeed<'a> {
    field: &'a str,
}

impl<'de> DeserializeSeed<'de> for NumberFieldSeed<'_> {
    type Value = Option<i64>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(NumberFieldVisitor { field: self.field })
    }
}

struct NumberFieldVisitor<'a> {
    field: &'a str,
}

impl<'de> Visitor<'de> for NumberFieldVisitor<'_> {
    type Value = Option<i64>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut found = None;
        while let Some(key) = map.next_key::<&str>()? {
            if key == self.field {
                let value = map.next_value::<NumberAttributeProjection<'de>>()?;
                found = match value.number {
                    Some(raw) => raw.parse::<i64>().ok(),
                    None => None,
                };
                continue;
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(found)
    }
}

#[derive(Deserialize)]
struct NumberAttributeProjection<'a> {
    #[serde(rename = "N", borrow)]
    number: Option<Cow<'a, str>>,
}

pub(super) fn parse_dynamo_number_field_i64(
    bytes: &[u8],
    field: &str,
) -> StorageResult<Option<i64>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    NumberFieldSeed { field }
        .deserialize(&mut deserializer)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))
}

struct BoolFieldSeed<'a> {
    field: &'a str,
}

impl<'de> DeserializeSeed<'de> for BoolFieldSeed<'_> {
    type Value = Option<bool>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(BoolFieldVisitor { field: self.field })
    }
}

struct BoolFieldVisitor<'a> {
    field: &'a str,
}

impl<'de> Visitor<'de> for BoolFieldVisitor<'_> {
    type Value = Option<bool>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut found = None;
        while let Some(key) = map.next_key::<&str>()? {
            if key == self.field {
                let value = map.next_value::<BoolAttributeProjection>()?;
                found = value.value;
                continue;
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(found)
    }
}

#[derive(Deserialize)]
struct BoolAttributeProjection {
    #[serde(rename = "BOOL")]
    value: Option<bool>,
}

pub(super) fn parse_dynamo_bool_field(bytes: &[u8], field: &str) -> StorageResult<Option<bool>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    BoolFieldSeed { field }
        .deserialize(&mut deserializer)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))
}

struct StringFieldSeed<'a> {
    field: &'a str,
}

impl<'de> DeserializeSeed<'de> for StringFieldSeed<'_> {
    type Value = Option<Cow<'de, str>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(StringFieldVisitor { field: self.field })
    }
}

struct StringFieldVisitor<'a> {
    field: &'a str,
}

impl<'de> Visitor<'de> for StringFieldVisitor<'_> {
    type Value = Option<Cow<'de, str>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut found = None;
        while let Some(key) = map.next_key::<&str>()? {
            if key == self.field {
                let value = map.next_value::<StringAttributeProjection<'de>>()?;
                found = value.string;
                continue;
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(found)
    }
}

#[derive(Deserialize)]
struct StringAttributeProjection<'a> {
    #[serde(rename = "S", borrow)]
    string: Option<Cow<'a, str>>,
}

pub(super) fn parse_dynamo_string_field<'a>(
    bytes: &'a [u8],
    field: &str,
) -> StorageResult<Option<Cow<'a, str>>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    StringFieldSeed { field }
        .deserialize(&mut deserializer)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))
}

struct AttributeValueFieldSeed<'a> {
    field: &'a str,
}

impl<'de> DeserializeSeed<'de> for AttributeValueFieldSeed<'_> {
    type Value = Option<AttributeValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(AttributeValueFieldVisitor { field: self.field })
    }
}

struct AttributeValueFieldVisitor<'a> {
    field: &'a str,
}

impl<'de> Visitor<'de> for AttributeValueFieldVisitor<'_> {
    type Value = Option<AttributeValue>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut found = None;
        while let Some(key) = map.next_key::<&str>()? {
            if key == self.field {
                found = Some(map.next_value::<AttributeValue>()?);
                continue;
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(found)
    }
}

pub(super) fn parse_dynamo_attribute_value_field(
    bytes: &[u8],
    field: &str,
) -> StorageResult<Option<AttributeValue>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    AttributeValueFieldSeed { field }
        .deserialize(&mut deserializer)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))
}
