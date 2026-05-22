use std::{collections::HashMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};
use smallvec::SmallVec;
use utoipa::ToSchema;

use crate::AttributeValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct KeyAttribute {
    pub name: String,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, Default, PartialEq, ToSchema)]
#[schema(value_type = HashMap<String, AttributeValue>)]
pub struct KeyAttributes(SmallVec<[KeyAttribute; 2]>);

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

    pub fn canonical_dynamo_json(&self) -> Result<String, crate::ConversionError> {
        serde_json::to_string(&SortedKeyAttributes(self))
            .map_err(|err| crate::ConversionError::Serialization(err.to_string()))
    }

    #[must_use]
    pub fn to_attribute_map(&self) -> HashMap<String, AttributeValue> {
        self.iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    }
}

struct SortedKeyAttributes<'a>(&'a KeyAttributes);

impl Serialize for SortedKeyAttributes<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut entries = SmallVec::<[(&str, &AttributeValue); 2]>::from_iter(self.0.iter());
        entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (name, value) in entries {
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
        while let Some((name, value)) = access.next_entry::<String, AttributeValue>()? {
            attributes.insert(name, value);
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

impl AttributeValueLookup for KeyAttributes {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.get(name)
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}

impl AttributeValueLookup for HashMap<String, AttributeValue> {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.get(name)
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}
