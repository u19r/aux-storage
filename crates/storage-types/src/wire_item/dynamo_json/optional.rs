use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Item, ItemRef};
use crate::wire_item::WireItem;

pub fn serialize<S>(item: &Option<WireItem>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    item.as_ref().map(ItemRef).serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<WireItem>, D::Error>
where D: Deserializer<'de> {
    Ok(Option::<Item>::deserialize(deserializer)?.map(|item| item.0))
}
