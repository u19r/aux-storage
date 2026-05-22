use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, KeySchemaElement, KeyType, StorageError, StorageResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireItemKeyAttributes {
    pub hash_key_name: Cow<'static, str>,
    pub hash_key: AttributeValue,
    pub sort_key_name: Option<Cow<'static, str>>,
    pub sort_key: Option<AttributeValue>,
}

impl WireItemKeyAttributes {
    #[must_use]
    pub fn new(
        hash_key_name: String,
        hash_key: AttributeValue,
        sort_key_name: Option<String>,
        sort_key: Option<AttributeValue>,
    ) -> Self {
        Self::new_with_names(
            Cow::Owned(hash_key_name),
            hash_key,
            sort_key_name.map(Cow::Owned),
            sort_key,
        )
    }

    #[must_use]
    pub fn new_with_names(
        hash_key_name: Cow<'static, str>,
        hash_key: AttributeValue,
        sort_key_name: Option<Cow<'static, str>>,
        sort_key: Option<AttributeValue>,
    ) -> Self {
        Self {
            hash_key_name,
            hash_key,
            sort_key_name,
            sort_key,
        }
    }

    pub fn from_key_schema(
        key_schema: &[KeySchemaElement],
        attributes: &HashMap<String, AttributeValue>,
    ) -> StorageResult<Self> {
        let Some(hash_key_name) = key_schema.iter().find_map(|key| {
            if key.key_type == KeyType::Hash {
                Some(key.attribute_name.as_str())
            } else {
                None
            }
        }) else {
            return Err(StorageError::invalid_or_missing_key());
        };
        let Some(hash_key) = attributes.get(hash_key_name) else {
            return Err(StorageError::invalid_or_missing_key());
        };

        let sort_key_name = key_schema.iter().find_map(|key| {
            if key.key_type == KeyType::Range {
                Some(intern_common_key_name(&key.attribute_name))
            } else {
                None
            }
        });
        let sort_key = match &sort_key_name {
            Some(name) => Some(
                attributes
                    .get(name.as_ref())
                    .ok_or_else(StorageError::invalid_or_missing_key)?
                    .clone(),
            ),
            None => None,
        };

        Ok(Self::new_with_names(
            intern_common_key_name(hash_key_name),
            hash_key.clone(),
            sort_key_name,
            sort_key,
        ))
    }

    pub(super) fn payload_len(&self) -> usize {
        let mut len = self.hash_key_name.len() + scalar_attribute_len(&self.hash_key);
        if let (Some(name), Some(value)) = (&self.sort_key_name, &self.sort_key) {
            len += name.len() + scalar_attribute_len(value);
        }
        len
    }

    pub(super) fn append_to_attribute_map(self, map: &mut HashMap<String, AttributeValue>) {
        map.insert(self.hash_key_name.into_owned(), self.hash_key);
        if let (Some(name), Some(value)) = (self.sort_key_name, self.sort_key) {
            map.insert(name.into_owned(), value);
        }
    }
}

fn intern_common_key_name(name: &str) -> Cow<'static, str> {
    match name {
        "pk" => Cow::Borrowed("pk"),
        "sk" => Cow::Borrowed("sk"),
        _ => Cow::Owned(name.to_string()),
    }
}

pub(super) fn scalar_attribute_len(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => {
            value.len()
        }
        AttributeValue::BOOL(true) => 4,
        AttributeValue::BOOL(false) => 5,
        _ => 0,
    }
}

pub(super) fn blob_is_empty_json_object(blob: &[u8]) -> bool {
    trim_ascii_whitespace(blob) == b"{}"
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

pub(super) fn number_from_key_attributes(
    key_attributes: &WireItemKeyAttributes,
    field: &str,
) -> Option<i64> {
    if key_attributes.hash_key_name == field {
        return number_from_attribute_value(&key_attributes.hash_key);
    }
    if key_attributes.sort_key_name.as_deref() == Some(field)
        && let Some(sort_key) = key_attributes.sort_key.as_ref()
    {
        return number_from_attribute_value(sort_key);
    }
    None
}

fn number_from_attribute_value(value: &AttributeValue) -> Option<i64> {
    match value {
        AttributeValue::N(raw) => raw.parse::<i64>().ok(),
        _ => None,
    }
}

pub(super) fn string_from_key_attributes<'a>(
    key_attributes: &'a WireItemKeyAttributes,
    field: &str,
) -> Option<&'a str> {
    if key_attributes.hash_key_name == field {
        return string_from_attribute_value(&key_attributes.hash_key);
    }
    if key_attributes.sort_key_name.as_deref() == Some(field)
        && let Some(sort_key) = key_attributes.sort_key.as_ref()
    {
        return string_from_attribute_value(sort_key);
    }
    None
}

fn string_from_attribute_value(value: &AttributeValue) -> Option<&str> {
    match value {
        AttributeValue::S(raw) => Some(raw.as_str()),
        _ => None,
    }
}

pub(super) fn scalar_from_key_attributes<'a>(
    key_attributes: &'a WireItemKeyAttributes,
    field: &str,
) -> Option<&'a str> {
    if key_attributes.hash_key_name == field {
        return key_attributes.hash_key.inner_str().ok();
    }
    if key_attributes.sort_key_name.as_deref() == Some(field) {
        return key_attributes
            .sort_key
            .as_ref()
            .and_then(|value| value.inner_str().ok());
    }
    None
}

pub(super) fn attribute_from_key_attributes<'a>(
    key_attributes: &'a WireItemKeyAttributes,
    field: &str,
) -> Option<&'a AttributeValue> {
    if key_attributes.hash_key_name == field {
        return Some(&key_attributes.hash_key);
    }
    if key_attributes.sort_key_name.as_deref() == Some(field) {
        return key_attributes.sort_key.as_ref();
    }
    None
}
