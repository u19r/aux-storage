//! DynamoDB attribute serde for public responses and stored stream images.

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::value::RawValue;

use crate::{TableName, VecMap, wire_item::WireItem};

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

pub(super) struct ItemsRef<'a>(pub(super) &'a [WireItem]);

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

pub(super) struct TableItemsRef<'a>(pub(super) &'a VecMap<TableName, Vec<WireItem>>);

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

pub(crate) fn serialize<S>(item: &WireItem, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    ItemRef(item).serialize(serializer)
}

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<WireItem, D::Error>
where D: serde::Deserializer<'de> {
    Item::deserialize(deserializer).map(|item| item.0)
}
