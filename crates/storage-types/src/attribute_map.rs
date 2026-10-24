use std::{collections::HashMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};
use utoipa::ToSchema;

use crate::{
    AttributeValue, AttributeValueLookup, KeyAttributes, StorageResult,
    normalize_key_attribute_value_for_write, verify_key_attributes,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AttributeMapEntry {
    pub name: String,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, Default, ToSchema)]
#[schema(value_type = HashMap<String, AttributeValue>)]
/// Equality treats attributes as a map; iteration and serialization retain
/// insertion order.
pub struct AttributeMap(Vec<AttributeMapEntry>);

impl PartialEq for AttributeMap {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(name, value)| other.get(name) == Some(value))
    }
}

impl AttributeMap {
    pub fn from_dynamo_json(data: &[u8]) -> StorageResult<Self> {
        serde_json::from_slice(data).map_err(|error| {
            crate::StorageError::internal(&format!("decode wire item attributes failed: {error}"))
        })
    }

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

    pub fn remove(&mut self, name: &str) -> Option<AttributeValue> {
        let index = self.0.iter().position(|attribute| attribute.name == name)?;
        Some(self.0.remove(index).value)
    }

    pub fn normalize_numbers_for_write(&mut self) -> bool {
        let mut changed = false;
        for attribute in &mut self.0 {
            changed |= crate::dynamodb_limits::normalize_attribute_value_numbers_for_write(
                &mut attribute.value,
            );
        }
        changed
    }

    /// Complete an item with its physically addressed key and reject rows
    /// whose stored key attributes disagree with that address.
    pub fn with_verified_keys(mut self, key: &KeyAttributes) -> StorageResult<Self> {
        if verify_key_attributes(key, |name| Ok(self.get(name).cloned()))? {
            return Ok(self);
        }
        for (name, value) in key.iter() {
            if !self.contains_key(name) {
                self.insert(name, normalize_key_attribute_value_for_write(value));
            }
        }
        Ok(self)
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

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|attribute| attribute.name.as_str())
    }

    pub fn values(&self) -> impl Iterator<Item = &AttributeValue> {
        self.0.iter().map(|attribute| &attribute.value)
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut AttributeValue> {
        self.0.iter_mut().map(|attribute| &mut attribute.value)
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

    /// Retain entries in their existing wire order without rebuilding the
    /// map through `insert`, which would rescan earlier names.
    pub fn retain<F>(&mut self, mut keep: F)
    where F: FnMut(&str, &AttributeValue) -> bool {
        self.0
            .retain(|entry| keep(entry.name.as_str(), &entry.value));
    }

    pub(crate) fn order_single_table_attributes(&mut self, indexers: &[crate::EntityIndexer]) {
        self.0.sort_unstable_by(|left, right| {
            let priority = |name: &str| {
                let ordinal = indexers
                    .iter()
                    .find(|indexer| indexer.attribute_name() == name)
                    .map(|indexer| usize::from(indexer.ordinal()));
                crate::wire_attribute_priority(name, ordinal)
            };
            priority(&left.name)
                .cmp(&priority(&right.name))
                .then_with(|| left.name.cmp(&right.name))
        });
    }
}

// TODO: benchmark output-buffer capacity and string escaping in complete
// response serialization.
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
        while let Some(name) = access.next_key::<String>()? {
            if attributes.get(&name).is_some() {
                return Err(serde::de::Error::custom(
                    "duplicate DynamoDB attribute name",
                ));
            }
            let value = access.next_value::<AttributeValue>()?;
            attributes.0.push(AttributeMapEntry { name, value });
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

// VecMap already enforces unique names; conversion need not re-scan them.
impl From<crate::VecMap<&str, AttributeValue>> for AttributeMap {
    fn from(value: crate::VecMap<&str, AttributeValue>) -> Self {
        Self(
            value
                .into_iter()
                .map(|(name, value)| AttributeMapEntry {
                    name: name.to_owned(),
                    value,
                })
                .collect(),
        )
    }
}

impl From<HashMap<String, AttributeValue>> for AttributeMap {
    fn from(value: HashMap<String, AttributeValue>) -> Self {
        let attributes = value
            .into_iter()
            .map(|(name, value)| AttributeMapEntry { name, value })
            .collect();
        Self(attributes)
    }
}

impl From<&HashMap<String, AttributeValue>> for AttributeMap {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        value
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
}

impl From<&KeyAttributes> for AttributeMap {
    fn from(value: &KeyAttributes) -> Self {
        value
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
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
