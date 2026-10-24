use std::{borrow::Cow, collections::HashMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};
use smallvec::SmallVec;
use utoipa::ToSchema;

use crate::{AttributeValue, StorageError, StorageResult};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct KeyAttribute {
    pub name: String,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, Default, ToSchema)]
#[schema(value_type = HashMap<String, AttributeValue>)]
/// Equality treats key attributes as a map; iteration and serialization retain
/// insertion order.
pub struct KeyAttributes(SmallVec<[KeyAttribute; 2]>);

impl PartialEq for KeyAttributes {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(name, value)| other.get(name) == Some(value))
    }
}

impl KeyAttributes {
    #[must_use]
    pub fn new() -> Self {
        Self(SmallVec::new())
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(SmallVec::with_capacity(capacity))
    }

    pub fn insert(&mut self, name: impl Into<String>, value: AttributeValue) {
        let name = name.into();
        if let Some(attribute) = self.0.iter_mut().find(|attribute| attribute.name == name) {
            attribute.value = value;
            return;
        }
        self.0.push(KeyAttribute { name, value });
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&AttributeValue> {
        self.0
            .iter()
            .find(|attribute| attribute.name == name)
            .map(|attribute| &attribute.value)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut AttributeValue> {
        self.0
            .iter_mut()
            .find(|attribute| attribute.name == name)
            .map(|attribute| &mut attribute.value)
    }

    #[must_use]
    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &AttributeValue)> {
        self.0
            .iter()
            .map(|attribute| (attribute.name.as_str(), &attribute.value))
    }

    /// Normalize owned write keys after validation, before native or original
    /// preparation.
    pub fn normalize_numbers_for_write(&mut self) {
        for attribute in &mut self.0 {
            crate::dynamodb_limits::normalize_attribute_value_numbers_for_write(
                &mut attribute.value,
            );
        }
    }

    /// Validate source keys when their historical hash/range schema is
    /// unavailable. Exact table binding still owns component names, types
    /// and the sort-key limit.
    pub fn validate_source_components(&self) -> StorageResult<()> {
        if !matches!(self.len(), 1 | 2) {
            return Err(StorageError::invalid_or_missing_key());
        }
        for (name, value) in self.iter() {
            crate::dynamodb_limits::validate_attribute_name(name, || {
                "Key attribute name".to_owned()
            })
            .map_err(StorageError::validation)?;
            crate::dynamodb_limits::validate_key_value_size(&crate::KeyType::Hash, name, value)?;
        }
        Ok(())
    }

    pub fn canonical_dynamo_json(&self) -> Result<String, crate::ConversionError> {
        serde_json::to_string(&CanonicalKeyAttributes::try_from(self)?)
            .map_err(|err| crate::ConversionError::Serialization(err.to_string()))
    }
}

/// Compare two values used to address an item. Numeric key spellings such as
/// `1`, `1.0`, and `1E0` identify the same DynamoDB key; malformed numbers
/// remain validation errors instead of accidentally comparing equal.
pub fn key_attribute_values_match(
    left: &AttributeValue,
    right: &AttributeValue,
) -> StorageResult<bool> {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            let left = crate::canonical_dynamodb_number(left).map_err(|error| {
                StorageError::validation(format!("Invalid numeric key: {error}"))
            })?;
            let right = crate::canonical_dynamodb_number(right).map_err(|error| {
                StorageError::validation(format!("Invalid numeric key: {error}"))
            })?;
            Ok(left == right)
        }
        _ => Ok(left == right),
    }
}

/// Verify every stored key component before any caller evaluates a condition.
/// The result is false only when at least one component is absent.
pub fn verify_key_attributes(
    key: &KeyAttributes,
    mut value_for_name: impl FnMut(&str) -> StorageResult<Option<AttributeValue>>,
) -> StorageResult<bool> {
    let mut complete = true;
    for (name, requested) in key.iter() {
        match value_for_name(name)? {
            Some(stored) if !key_attribute_values_match(&stored, requested)? => {
                return Err(StorageError::internal(&format!(
                    "Key attribute mismatch for {name}"
                )));
            }
            Some(_) => {}
            None => complete = false,
        }
    }
    Ok(complete)
}

/// Expand scientific notation in a key value before storing a completed item.
#[must_use]
pub fn normalize_key_attribute_value_for_write(value: &AttributeValue) -> AttributeValue {
    match value {
        AttributeValue::N(number) => {
            AttributeValue::N(crate::normalize_dynamodb_number_for_write(number).into_owned())
        }
        _ => value.clone(),
    }
}

/// Canonicalize top-level `N` and `NS` values while preserving invalid
/// spellings for serializers whose validation runs separately.
pub fn canonicalize_top_level_number_value(value: &mut AttributeValue) {
    match value {
        AttributeValue::N(number) => {
            if let Ok(canonical) = crate::canonical_dynamodb_number(number) {
                *number = canonical;
            }
        }
        AttributeValue::NS(values) => {
            for number in values {
                if let Ok(canonical) = crate::canonical_dynamodb_number(number) {
                    *number = canonical;
                }
            }
        }
        _ => {}
    }
}

/// Sorted canonical Dynamo JSON, borrowing values except normalized numeric
/// keys.
pub struct CanonicalKeyAttributes<'a>(SmallVec<[(&'a str, Cow<'a, AttributeValue>); 2]>);

impl<'a> TryFrom<&'a KeyAttributes> for CanonicalKeyAttributes<'a> {
    type Error = crate::ConversionError;

    fn try_from(key: &'a KeyAttributes) -> Result<Self, Self::Error> {
        let mut canonical = SmallVec::new();
        for (name, value) in key.iter() {
            let value = match value {
                AttributeValue::N(number) => Cow::Owned(AttributeValue::N(
                    crate::canonical_dynamodb_number(number).map_err(|error| {
                        crate::ConversionError::Serialization(error.to_string())
                    })?,
                )),
                other => Cow::Borrowed(other),
            };
            canonical.push((name, value));
        }
        canonical.sort_unstable_by_key(|(name, _)| *name);
        Ok(Self(canonical))
    }
}

impl Serialize for CanonicalKeyAttributes<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

impl Serialize for KeyAttributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for attribute in &self.0 {
            map.serialize_entry(&attribute.name, &attribute.value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for KeyAttributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_map(KeyAttributesVisitor)
    }
}

struct KeyAttributesVisitor;

impl<'de> Visitor<'de> for KeyAttributesVisitor {
    type Value = KeyAttributes;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB key attribute map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut attributes = KeyAttributes::with_capacity(access.size_hint().unwrap_or(2));
        while let Some(name) = access.next_key::<String>()? {
            if attributes.contains_key(&name) {
                return Err(serde::de::Error::custom(
                    "duplicate DynamoDB key attribute name",
                ));
            }
            let value = access.next_value::<AttributeValue>()?;
            attributes.0.push(KeyAttribute { name, value });
        }
        Ok(attributes)
    }
}

impl FromIterator<(String, AttributeValue)> for KeyAttributes {
    fn from_iter<T: IntoIterator<Item = (String, AttributeValue)>>(iter: T) -> Self {
        let iterator = iter.into_iter();
        let mut attributes = Self::with_capacity(iterator.size_hint().0);
        for (name, value) in iterator {
            attributes.insert(name, value);
        }
        attributes
    }
}

impl From<HashMap<String, AttributeValue>> for KeyAttributes {
    fn from(value: HashMap<String, AttributeValue>) -> Self {
        value.into_iter().collect()
    }
}

impl<const N: usize> From<[(String, AttributeValue); N]> for KeyAttributes {
    fn from(value: [(String, AttributeValue); N]) -> Self {
        value.into_iter().collect()
    }
}

impl IntoIterator for KeyAttributes {
    type IntoIter = smallvec::IntoIter<[KeyAttribute; 2]>;
    type Item = KeyAttribute;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub trait AttributeValueLookup {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue>;

    fn attribute_count(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.attribute_count() == 0
    }
}

impl<T> AttributeValueLookup for &T
where T: AttributeValueLookup + ?Sized
{
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        (*self).get_attribute_value(name)
    }

    fn attribute_count(&self) -> usize {
        (*self).attribute_count()
    }
}

impl AttributeValueLookup for KeyAttributes {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.get(name)
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}

// Legacy SQL/KV key validation still passes HashMap. New operation paths use
// AttributeMap or KeyAttributes; remove this implementation at their cutover.
impl AttributeValueLookup for HashMap<String, AttributeValue> {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.get(name)
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}
