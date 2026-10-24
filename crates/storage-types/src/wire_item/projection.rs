use std::borrow::Cow;

use serde::{
    Deserialize, Serialize,
    de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, Visitor},
    ser::SerializeMap,
};

use super::text::BorrowedText;
use crate::{AttributeMap, AttributeValue, StorageError, StorageResult};

struct AttributeFieldSeed<'a> {
    fields: &'a [&'a str],
}

impl<'de> DeserializeSeed<'de> for AttributeFieldSeed<'_> {
    type Value = AttributeMap;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(AttributeFieldVisitor {
            fields: self.fields,
        })
    }
}

struct AttributeFieldVisitor<'a> {
    fields: &'a [&'a str],
}

impl<'de> Visitor<'de> for AttributeFieldVisitor<'_> {
    type Value = AttributeMap;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut attributes = AttributeMap::with_capacity(self.fields.len());
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
            if self.fields.contains(&key) {
                attributes.insert(key, map.next_value::<AttributeValue>()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(attributes)
    }
}

pub(crate) fn parse_dynamo_attribute_fields(
    bytes: &[u8],
    fields: &[&str],
) -> StorageResult<AttributeMap> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    AttributeFieldSeed { fields }
        .deserialize(&mut deserializer)
        .map_err(StorageError::from)
}

struct AttributeValuesSeed<'a> {
    fields: &'a [&'a str],
}

impl<'de> DeserializeSeed<'de> for AttributeValuesSeed<'_> {
    type Value = Vec<Option<AttributeValue>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(AttributeValuesVisitor {
            fields: self.fields,
        })
    }
}

struct AttributeValuesVisitor<'a> {
    fields: &'a [&'a str],
}

impl<'de> Visitor<'de> for AttributeValuesVisitor<'_> {
    type Value = Vec<Option<AttributeValue>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut values = vec![None; self.fields.len()];
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
            if let Some(index) = self.fields.iter().position(|field| *field == key) {
                values[index] = Some(map.next_value::<AttributeValue>()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(values)
    }
}

pub(crate) fn parse_dynamo_attribute_values(
    bytes: &[u8],
    fields: &[&str],
) -> StorageResult<Vec<Option<AttributeValue>>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    AttributeValuesSeed { fields }
        .deserialize(&mut deserializer)
        .map_err(StorageError::from)
}

struct ProjectedAttributeValues<'a> {
    fields: &'a [&'a str],
    values: &'a [Option<AttributeValue>],
}

impl Serialize for ProjectedAttributeValues<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let present = self
            .fields
            .iter()
            .zip(self.values)
            .enumerate()
            .filter(|(index, (field, value))| {
                value.is_some() && !self.fields[..*index].contains(field)
            })
            .count();
        let mut map = serializer.serialize_map(Some(present))?;
        for (index, (field, value)) in self.fields.iter().zip(self.values).enumerate() {
            if self.fields[..index].contains(field) {
                continue;
            }
            if let Some(value) = value {
                map.serialize_entry(field, value)?;
            }
        }
        map.end()
    }
}

pub(crate) fn encode_dynamo_attribute_values(
    fields: &[&str],
    values: &[Option<AttributeValue>],
) -> StorageResult<Vec<u8>> {
    if fields.len() != values.len() {
        return Err(StorageError::internal(
            "wire projection field and value counts differ",
        ));
    }
    serde_json::to_vec(&ProjectedAttributeValues { fields, values })
        .map_err(|err| StorageError::internal(&format!("encode projected wire item failed: {err}")))
}

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
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
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

pub(crate) fn parse_dynamo_scalar_fields<'a>(
    bytes: &'a [u8],
    fields: &[&str],
) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    ScalarFieldSeed { fields }
        .deserialize(&mut deserializer)
        .map_err(StorageError::from)
}

struct ScalarFieldAliasSeed<'a> {
    fields: &'a [&'a str],
    aliases: &'a [Option<&'a str>],
}

impl<'de> DeserializeSeed<'de> for ScalarFieldAliasSeed<'_> {
    type Value = Vec<Option<Cow<'de, str>>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(ScalarFieldAliasVisitor {
            fields: self.fields,
            aliases: self.aliases,
        })
    }
}

struct ScalarFieldAliasVisitor<'a> {
    fields: &'a [&'a str],
    aliases: &'a [Option<&'a str>],
}

impl<'de> Visitor<'de> for ScalarFieldAliasVisitor<'_> {
    type Value = Vec<Option<Cow<'de, str>>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dynamodb attribute map object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut values = vec![None; self.fields.len()];
        let mut alias_values =
            smallvec::SmallVec::<[Option<Cow<'de, str>>; 8]>::with_capacity(self.fields.len());
        alias_values.resize_with(self.fields.len(), || None);

        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
            let direct_index = self.fields.iter().position(|field| *field == key);
            let alias_indices = self
                .aliases
                .iter()
                .enumerate()
                .filter_map(|(index, alias)| {
                    alias.is_some_and(|alias| alias == key).then_some(index)
                })
                .collect::<smallvec::SmallVec<[usize; 8]>>();
            if direct_index.is_none() && alias_indices.is_empty() {
                map.next_value::<IgnoredAny>()?;
                continue;
            }

            let Some(scalar) = map
                .next_value::<BorrowedScalarAttribute<'de>>()?
                .into_scalar()
            else {
                continue;
            };
            if let Some(direct_index) = direct_index {
                values[direct_index] = Some(scalar);
                for alias_index in alias_indices {
                    if alias_index != direct_index {
                        alias_values[alias_index] = values[direct_index].clone();
                    }
                }
            } else if let Some((&first_alias_index, rest)) = alias_indices.split_first() {
                alias_values[first_alias_index] = Some(scalar);
                for alias_index in rest {
                    alias_values[*alias_index] = alias_values[first_alias_index].clone();
                }
            }
        }

        for (index, value) in values.iter_mut().enumerate() {
            if value.is_none() {
                *value = alias_values[index].take();
            }
        }
        Ok(values)
    }
}

pub(crate) fn parse_dynamo_scalar_fields_with_aliases<'a>(
    bytes: &'a [u8],
    fields: &[&str],
    aliases: &[Option<&str>],
) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
    debug_assert_eq!(fields.len(), aliases.len());
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    ScalarFieldAliasSeed { fields, aliases }
        .deserialize(&mut deserializer)
        .map_err(StorageError::from)
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
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
            if key == self.field {
                let value = map.next_value::<NumberAttributeProjection<'de>>()?;
                found = match value.number {
                    Some(raw) => Some(raw.parse::<i64>().map_err(M::Error::custom)?),
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
        .map_err(StorageError::from)
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
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
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
        .map_err(StorageError::from)
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
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
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
        .map_err(StorageError::from)
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
        while let Some(key) = map.next_key::<BorrowedText<'de>>()? {
            let key = key.0.as_ref();
            if key == self.field {
                if found.is_some() {
                    return Err(serde::de::Error::custom(
                        "duplicate DynamoDB attribute name in wire item",
                    ));
                }
                found = Some(map.next_value::<AttributeValue>()?);
                continue;
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(found)
    }
}

// TODO: benchmark field extraction before replacing the streaming JSON parser.
pub(super) fn parse_dynamo_attribute_value_field(
    bytes: &[u8],
    field: &str,
) -> StorageResult<Option<AttributeValue>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    AttributeValueFieldSeed { field }
        .deserialize(&mut deserializer)
        .map_err(StorageError::from)
}

// TODO: benchmark field extraction before replacing the streaming JSON parser.
pub(super) fn parse_complete_dynamo_attribute_value_field(
    bytes: &[u8],
    field: &str,
) -> StorageResult<Option<AttributeValue>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = AttributeValueFieldSeed { field }
        .deserialize(&mut deserializer)
        .map_err(StorageError::from)?;
    deserializer.end().map_err(StorageError::from)?;
    Ok(value)
}
