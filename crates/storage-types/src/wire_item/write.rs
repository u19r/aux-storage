use std::{borrow::Cow, fmt};

use serde::{
    Deserialize,
    de::{DeserializeSeed, Error, IgnoredAny, MapAccess, Visitor},
};

use super::{
    WireItem,
    text::{AttributeNames, BorrowedText},
};
use crate::{
    AttributeMap, AttributeValue, LogicalItemSize, StorageError, StorageResult,
    attribute_value::{ATTRIBUTE_VALUE_EMPTY_MESSAGE, ATTRIBUTE_VALUE_MULTIPLE_MESSAGE},
    dynamodb_limits,
};

impl WireItem {
    pub fn to_dynamo_json_string(&self) -> StorageResult<String> {
        match self {
            Self::DynamoJson { data } => String::from_utf8(data.clone())
                .map_err(|_| StorageError::internal("wire item JSON is not UTF-8")),
        }
    }

    /// Validate every write attribute without retaining a decoded full-item
    /// map.
    pub fn validate_for_write(&self, field: &str) -> Result<LogicalItemSize, String> {
        match self {
            Self::DynamoJson { data } => {
                inspect_write_item::<false>(data, field).map(|(size, _)| size)
            }
        }
    }

    /// Inspect split keys directly before encoding the canonical write item.
    pub fn normalized_for_write(self) -> StorageResult<(Self, LogicalItemSize)> {
        match self {
            Self::DynamoJson { data } => {
                let (size, replacements) =
                    inspect_write_item::<true>(&data, "Item").map_err(StorageError::validation)?;
                let data = replace_write_attributes(data, &replacements)?;
                Ok((Self::dynamo_json(data), size))
            }
        }
    }
}

fn replace_write_attributes(data: Vec<u8>, replacements: &AttributeMap) -> StorageResult<Vec<u8>> {
    let mut data = if replacements.is_empty() {
        data
    } else {
        super::attributes::update_attribute_bytes(&data, &[], replacements)?
    };
    compact_write_json(&mut data);
    Ok(data)
}

/// Called only after complete write validation; retained originals bypass this
/// path.
fn compact_write_json(data: &mut Vec<u8>) {
    let mut is_string = false;
    let mut is_escaped = false;
    data.retain(|&byte| {
        if is_string {
            if is_escaped {
                is_escaped = false;
            } else if byte == b'\\' {
                is_escaped = true;
            } else if byte == b'"' {
                is_string = false;
            }
            return true;
        }
        if byte == b'"' {
            is_string = true;
        }
        !matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
    });
}

fn inspect_write_item<const NORMALIZE: bool>(
    bytes: &[u8],
    field: &str,
) -> Result<(LogicalItemSize, AttributeMap), String> {
    let (size, replacements) = inspect_write_attributes::<NORMALIZE>(bytes, field, &[])?;
    if size == 0 {
        return Err(format!("{field} cannot be empty"));
    }
    LogicalItemSize::try_from(size)
        .map(|size| (size, replacements))
        .map_err(|error| error.to_string())
}

pub(crate) fn inspect_write_attributes<const NORMALIZE: bool>(
    bytes: &[u8],
    field: &str,
    excluded_names: &[&str],
) -> Result<(u64, AttributeMap), String> {
    let mut error = None;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let result = WriteVisitor::<NORMALIZE> {
        field,
        excluded_names,
        error: &mut error,
    }
    .deserialize(&mut deserializer);
    let result = result.map_err(|parse| {
        error.unwrap_or_else(|| {
            format!(
                "{:?} at {}:{}",
                parse.classify(),
                parse.line(),
                parse.column()
            )
        })
    })?;
    deserializer.end().map_err(|error| {
        format!(
            "{:?} at {}:{}",
            error.classify(),
            error.line(),
            error.column()
        )
    })?;
    Ok(result)
}

struct WriteVisitor<'a, const NORMALIZE: bool> {
    field: &'a str,
    excluded_names: &'a [&'a str],
    error: &'a mut Option<String>,
}

impl<'de, const NORMALIZE: bool> DeserializeSeed<'de> for WriteVisitor<'_, NORMALIZE> {
    type Value = (u64, AttributeMap);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(self)
    }
}

impl<'de, const NORMALIZE: bool> Visitor<'de> for WriteVisitor<'_, NORMALIZE> {
    type Value = (u64, AttributeMap);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB item")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut names = AttributeNames::default();
        let mut size = 0u64;
        let mut replacements = AttributeMap::new();
        while let Some(name) = map.next_key::<BorrowedText<'de>>()? {
            if !names.insert(name.0.clone()) {
                return Err(M::Error::custom(
                    "duplicate DynamoDB attribute name in wire item",
                ));
            }
            if self.excluded_names.contains(&name.0.as_ref()) {
                map.next_value::<IgnoredAny>()?;
                continue;
            }
            let value = map
                .next_value::<WriteAttribute<'de>>()
                .inspect_err(|error| {
                    if let Some(message) =
                        crate::attribute_value::attribute_value_arity_message(&error.to_string())
                    {
                        *self.error = Some(message.to_owned());
                    }
                })?;
            let attribute_size = inspect_attribute::<NORMALIZE>(value, name.0.as_ref(), self.field);
            let (value_size, replacement) = attribute_size.map_err(|error| {
                *self.error = Some(error.clone());
                M::Error::custom(error)
            })?;
            size = size
                .checked_add(value_size)
                .ok_or_else(|| M::Error::custom("item size overflow"))?;
            if let Some(replacement) = replacement {
                replacements.insert(name.0.into_owned(), replacement);
            }
        }
        Ok((size, replacements))
    }
}

fn inspect_attribute<const NORMALIZE: bool>(
    value: WriteAttribute<'_>,
    name: &str,
    field: &str,
) -> Result<(u64, Option<AttributeValue>), String> {
    dynamodb_limits::validate_item_attribute_name(name, || format!("{field} attribute name"))?;
    match value {
        WriteAttribute::String(text) => Ok(((name.len() + text.len()) as u64, None)),
        WriteAttribute::Number(number) => inspect_number::<NORMALIZE>(&number, name),
        WriteAttribute::Other(mut value) => {
            let size = LogicalItemSize::try_from_entries_with_field(
                std::iter::once((name, &value)),
                field,
            )
            .map(u64::from)
            .map_err(|error| error.to_string())?;
            let replacement = if NORMALIZE
                && dynamodb_limits::normalize_attribute_value_numbers_for_write(&mut value)
            {
                Some(value)
            } else {
                None
            };
            Ok((size, replacement))
        }
    }
}

fn inspect_number<const NORMALIZE: bool>(
    number: &str,
    name: &str,
) -> Result<(u64, Option<AttributeValue>), String> {
    dynamodb_limits::validate_number(number)?;
    let size = (name.len() + dynamodb_limits::dynamodb_number_size(number)) as u64;
    let replacement = if NORMALIZE {
        match dynamodb_limits::normalize_dynamodb_number_for_write(number) {
            Cow::Owned(number) => Some(AttributeValue::N(number)),
            Cow::Borrowed(_) => None,
        }
    } else {
        None
    };
    Ok((size, replacement))
}

enum WriteAttribute<'a> {
    String(Cow<'a, str>),
    Number(Cow<'a, str>),
    Other(AttributeValue),
}

impl<'de> Deserialize<'de> for WriteAttribute<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(AttributeVisitor)
    }
}

struct AttributeVisitor;

impl<'de> Visitor<'de> for AttributeVisitor {
    type Value = WriteAttribute<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("one DynamoDB attribute value")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let tag = map
            .next_key::<BorrowedText<'de>>()?
            .ok_or_else(|| M::Error::custom(ATTRIBUTE_VALUE_EMPTY_MESSAGE))?;
        let value = if tag.0 == "S" {
            WriteAttribute::String(map.next_value::<BorrowedText<'de>>()?.0)
        } else if tag.0 == "N" {
            WriteAttribute::Number(map.next_value::<BorrowedText<'de>>()?.0)
        } else {
            WriteAttribute::Other(crate::attribute_value::deserialize_tagged_attribute(
                &tag.0, &mut map,
            )?)
        };
        if map.next_key::<BorrowedText<'de>>()?.is_some() {
            return Err(M::Error::custom(ATTRIBUTE_VALUE_MULTIPLE_MESSAGE));
        }
        Ok(value)
    }
}
