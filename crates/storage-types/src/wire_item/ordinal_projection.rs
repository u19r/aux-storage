//! Stream selected source fields without persisting or reordering their names.
use std::{borrow::Cow, fmt};

use serde::{
    Serialize,
    de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, Visitor},
    ser::SerializeMap,
};

use super::text::BorrowedText;
use crate::{AttributeValue, StorageResult};

pub(crate) fn bind_source_fields(bytes: &[u8], fields: &[String]) -> StorageResult<Vec<u32>> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let positions = BindFields(fields).deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(positions.into_iter().flatten().collect())
}

struct BindFields<'a>(&'a [String]);
impl<'de> DeserializeSeed<'de> for BindFields<'_> {
    type Value = Vec<Option<u32>>;
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for BindFields<'_> {
    type Value = Vec<Option<u32>>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a source attribute map")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut positions = vec![None; self.0.len()];
        let mut ordinal = 0u32;
        while let Some(name) = map.next_key::<BorrowedText<'de>>()? {
            if let Some(position) = self.0.iter().position(|field| field == name.0.as_ref())
                && positions[position].replace(ordinal).is_some()
            {
                return Err(M::Error::custom("Repeated selected source field"));
            }
            map.next_value::<IgnoredAny>()?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| M::Error::custom("Source field count overflow"))?;
        }
        Ok(positions)
    }
}

pub(crate) fn project_source_fields(bytes: &[u8], ordinals: &[u32]) -> StorageResult<Vec<u8>> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let fields = ProjectFields(ordinals).deserialize(&mut decoder)?;
    decoder.end()?;
    serde_json::to_vec(&SelectedFields(fields)).map_err(Into::into)
}

struct ProjectFields<'a>(&'a [u32]);
impl<'de> DeserializeSeed<'de> for ProjectFields<'_> {
    type Value = Vec<(Cow<'de, str>, AttributeValue)>;
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for ProjectFields<'_> {
    type Value = Vec<(Cow<'de, str>, AttributeValue)>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a source attribute map")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut fields = (0..self.0.len()).map(|_| None).collect::<Vec<_>>();
        let mut ordinal = 0u32;
        while let Some(name) = map.next_key::<BorrowedText<'de>>()? {
            if let Some(position) = self.0.iter().position(|selected| *selected == ordinal) {
                fields[position] = Some((name.0, map.next_value::<AttributeValue>()?));
            } else {
                map.next_value::<IgnoredAny>()?;
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| M::Error::custom("Source field count overflow"))?;
        }
        fields
            .into_iter()
            .map(|field| field.ok_or_else(|| M::Error::custom("Selected source ordinal is absent")))
            .collect()
    }
}

struct SelectedFields<'a>(Vec<(Cow<'a, str>, AttributeValue)>);
impl Serialize for SelectedFields<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}
