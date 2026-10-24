//! DynamoDB attribute serde for public responses and stored stream images.

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::value::RawValue;

use super::WireItem;
use crate::{TableName, VecMap};

pub struct ItemRef<'a>(pub &'a WireItem);

impl Serialize for ItemRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        match self.0 {
            WireItem::DynamoJson { data } => {
                let json = std::str::from_utf8(data).map_err(serde::ser::Error::custom)?;
                let raw: &RawValue =
                    serde_json::from_str(json).map_err(serde::ser::Error::custom)?;
                raw.serialize(serializer)
            }
            WireItem::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let keys = super::item::canonical_key_entries(primary_key, secondary_key.as_ref());
                let excluded = keys.iter().map(|(name, _)| *name).collect::<Vec<_>>();
                let blob = non_key_attributes_blob
                    .as_deref()
                    .filter(|blob| !blob.is_empty())
                    .unwrap_or(b"{}");
                let entries = super::attributes::parse_attributes(blob, &excluded)
                    .map_err(serde::ser::Error::custom)?;
                let mut map = serializer.serialize_map(Some(entries.len() + keys.len()))?;
                for (name, value) in keys.iter() {
                    map.serialize_entry(name, value)?;
                }
                for (name, value) in entries {
                    map.serialize_entry(&name, value)?;
                }
                map.end()
            }
        }
    }
}

pub(crate) struct Item(pub WireItem);

impl<'de> Deserialize<'de> for Item {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if !raw.get().trim_start().starts_with('{') {
            return Err(serde::de::Error::custom("wire item must be a JSON object"));
        }
        let boxed: Box<str> = raw.into();
        Ok(Self(WireItem::dynamo_json(
            String::from(boxed).into_bytes(),
        )))
    }
}

pub mod optional {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{Item, ItemRef, WireItem};

    pub fn serialize<S>(item: &Option<WireItem>, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        item.as_ref().map(ItemRef).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<WireItem>, D::Error>
    where D: Deserializer<'de> {
        Ok(Option::<Item>::deserialize(deserializer)?.map(|item| item.0))
    }
}

struct ItemsRef<'a>(&'a [WireItem]);

impl Serialize for ItemsRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            sequence.serialize_element(&ItemRef(item))?;
        }
        sequence.end()
    }
}

pub(crate) mod optional_items {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{Item, ItemsRef, WireItem};

    pub fn serialize<S>(items: &Option<Vec<WireItem>>, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        items.as_deref().map(ItemsRef).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<WireItem>>, D::Error>
    where D: Deserializer<'de> {
        Ok(Option::<Vec<Item>>::deserialize(deserializer)?
            .map(|items| items.into_iter().map(|item| item.0).collect()))
    }
}

struct TableItemsRef<'a>(&'a VecMap<TableName, Vec<WireItem>>);

impl Serialize for TableItemsRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (table, items) in self.0 {
            map.serialize_entry(table, &ItemsRef(items))?;
        }
        map.end()
    }
}

pub(crate) mod optional_table_items {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{Item, TableItemsRef, TableName, VecMap, WireItem};

    pub fn serialize<S>(
        tables: &Option<VecMap<TableName, Vec<WireItem>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        tables.as_ref().map(TableItemsRef).serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<VecMap<TableName, Vec<WireItem>>>, D::Error>
    where D: Deserializer<'de> {
        Ok(
            Option::<VecMap<TableName, Vec<Item>>>::deserialize(deserializer)?.map(|tables| {
                tables
                    .into_iter()
                    .map(|(table, items)| (table, items.into_iter().map(|item| item.0).collect()))
                    .collect()
            }),
        )
    }
}

pub(crate) fn serialize<S>(item: &WireItem, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    ItemRef(item).serialize(serializer)
}

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<WireItem, D::Error>
where D: serde::Deserializer<'de> {
    Item::deserialize(deserializer).map(|item| item.0)
}
