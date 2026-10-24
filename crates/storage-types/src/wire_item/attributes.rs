use std::{borrow::Cow, fmt};

use serde::{
    Serialize,
    de::{DeserializeSeed, MapAccess, Visitor},
    ser::SerializeMap,
};
use serde_json::value::RawValue;

use super::{WireItem, text::BorrowedText};
use crate::{AttributeMap, StorageError, StorageResult};

pub(super) type RawAttributes<'a> = Vec<(Cow<'a, str>, &'a RawValue)>;

pub(super) fn parse_attributes<'a>(
    bytes: &'a [u8],
    excluded: &[&str],
) -> StorageResult<RawAttributes<'a>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let entries = AttributeSeed(excluded)
        .deserialize(&mut deserializer)
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    deserializer
        .end()
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    Ok(entries)
}

impl WireItem {
    /// Compare attribute representations, excluding the WireItem envelope.
    /// Different JSON ordering or whitespace conservatively counts as changed.
    pub fn has_same_attributes(&self, other: &Self) -> StorageResult<bool> {
        let left = self.dynamo_json_bytes()?;
        let right = other.dynamo_json_bytes()?;
        if left.len() != right.len() {
            return Ok(false);
        }
        if xxhash_rust::xxh3::xxh3_64(&left) != xxhash_rust::xxh3::xxh3_64(&right) {
            return Ok(false);
        }
        // A hash collision must never suppress a required projection write.
        Ok(left == right)
    }

    /// Replace or remove top-level values while retaining unselected wire
    /// attributes.
    pub fn with_attribute_updates(
        &self,
        removed: &[&str],
        replacements: &AttributeMap,
    ) -> StorageResult<Self> {
        let bytes = self.dynamo_json_bytes()?;
        update_attribute_bytes(&bytes, removed, replacements).map(Self::dynamo_json)
    }

    pub fn dynamo_json_bytes(&self) -> StorageResult<Cow<'_, [u8]>> {
        match self {
            Self::DynamoJson { data } => Ok(Cow::Borrowed(data)),
            Self::LocalSplit { .. } => serde_json::to_vec(&super::dynamo_json::ItemRef(self))
                .map(Cow::Owned)
                .map_err(|error| StorageError::internal(&error.to_string())),
        }
    }
}

pub(super) fn update_attribute_bytes(
    bytes: &[u8],
    removed: &[&str],
    replacements: &AttributeMap,
) -> StorageResult<Vec<u8>> {
    let entries = parse_attributes(bytes, removed)?;
    let mut data = Vec::with_capacity(bytes.len());
    serde_json::to_writer(
        &mut data,
        &UpdatedAttributes {
            entries,
            replacements,
        },
    )
    .map_err(|error| StorageError::internal(&error.to_string()))?;
    Ok(data)
}

struct AttributeSeed<'a>(&'a [&'a str]);

impl<'de> DeserializeSeed<'de> for AttributeSeed<'_> {
    type Value = RawAttributes<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for AttributeSeed<'_> {
    type Value = RawAttributes<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB item")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut entries = Vec::new();
        while let Some(name) = map.next_key::<BorrowedText<'de>>()? {
            let value = map.next_value::<&RawValue>()?;
            if !self.0.contains(&name.0.as_ref()) {
                entries.push((name.0, value));
            }
        }
        Ok(entries)
    }
}

pub(super) struct UpdatedAttributes<'a> {
    pub entries: RawAttributes<'a>,
    pub replacements: &'a AttributeMap,
}

impl Serialize for UpdatedAttributes<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in &self.entries {
            if let Some(replacement) = self.replacements.get(name) {
                map.serialize_entry(name, replacement)?;
            } else {
                map.serialize_entry(name, value)?;
            }
        }
        for (name, value) in self.replacements.iter() {
            if !self.entries.iter().any(|(existing, _)| existing == name) {
                map.serialize_entry(name, value)?;
            }
        }
        map.end()
    }
}

/// Generated keys lead the payload and replace fields with the same wire name.
/// Ordinary metadata follows the payload so TTL and indexers stay ahead of it.
pub(crate) fn merge_attribute_bytes(bytes: &[u8], metadata: &[u8]) -> StorageResult<Vec<u8>> {
    let replacements = parse_attributes(metadata, &[])?;
    let excluded = replacements
        .iter()
        .map(|(name, _)| name.as_ref())
        .collect::<smallvec::SmallVec<[&str; 13]>>();
    let entries = parse_attributes(bytes, &excluded)?;
    let is_key = |name: &str| crate::wire_attribute_priority(name, None).0 < 2;
    let mut data = Vec::with_capacity(bytes.len() + metadata.len());
    let mut serializer = serde_json::Serializer::new(&mut data);
    let mut map = serde::Serializer::serialize_map(
        &mut serializer,
        Some(entries.len() + replacements.len()),
    )?;
    for (name, value) in replacements
        .iter()
        .filter(|(name, _)| is_key(name))
        .chain(&entries)
        .chain(replacements.iter().filter(|(name, _)| !is_key(name)))
    {
        map.serialize_entry(name, value)?;
    }
    map.end()?;
    Ok(data)
}
