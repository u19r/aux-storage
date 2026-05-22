use std::{collections::HashMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};
use utoipa::ToSchema;

use crate::{AttributeValue, AttributeValueLookup};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AttributeMapEntry {
    pub name: String,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, Default, PartialEq, ToSchema)]
#[schema(value_type = HashMap<String, AttributeValue>)]
pub struct AttributeMap(Vec<AttributeMapEntry>);

impl AttributeMap {
    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn insert(&mut self, name: impl Into<String>, value: AttributeValue) {
        let name = name.into();
        if let Some(attribute) = self.0.iter_mut().find(|attribute| attribute.name == name) {
            attribute.value = value;
            return;
        }
        self.0.push(AttributeMapEntry { name, value });
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&AttributeValue> {
        self.0
            .iter()
            .find(|attribute| attribute.name == name)
            .map(|attribute| &attribute.value)
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

    #[must_use]
    pub fn to_hashmap(&self) -> HashMap<String, AttributeValue> {
        self.iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    }

    #[must_use]
    pub fn into_hashmap(self) -> HashMap<String, AttributeValue> {
        self.0
            .into_iter()
            .map(|attribute| (attribute.name, attribute.value))
            .collect()
    }
}

impl Serialize for AttributeMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for attribute in &self.0 {
            map.serialize_entry(&attribute.name, &attribute.value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for AttributeMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_map(AttributeMapVisitor)
    }
}

struct AttributeMapVisitor;

impl<'de> Visitor<'de> for AttributeMapVisitor {
    type Value = AttributeMap;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB attribute map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut attributes = AttributeMap::with_capacity(access.size_hint().unwrap_or(8));
        while let Some((name, value)) = access.next_entry::<String, AttributeValue>()? {
            attributes.insert(name, value);
        }
        Ok(attributes)
    }
}

impl FromIterator<(String, AttributeValue)> for AttributeMap {
    fn from_iter<T: IntoIterator<Item = (String, AttributeValue)>>(iter: T) -> Self {
        let iterator = iter.into_iter();
        let mut attributes = Self::with_capacity(iterator.size_hint().0);
        for (name, value) in iterator {
            attributes.insert(name, value);
        }
        attributes
    }
}

impl From<HashMap<String, AttributeValue>> for AttributeMap {
    fn from(value: HashMap<String, AttributeValue>) -> Self {
        value.into_iter().collect()
    }
}

impl From<AttributeMap> for HashMap<String, AttributeValue> {
    fn from(value: AttributeMap) -> Self {
        value.into_hashmap()
    }
}

impl IntoIterator for AttributeMap {
    type IntoIter = std::vec::IntoIter<AttributeMapEntry>;
    type Item = AttributeMapEntry;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl AttributeValueLookup for AttributeMap {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.get(name)
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}
