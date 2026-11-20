use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Item, item_json::ItemsRef};
use crate::wire_item::WireItem;

pub fn serialize<S>(items: &Option<Vec<WireItem>>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    items.as_deref().map(ItemsRef).serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<WireItem>>, D::Error>
where D: Deserializer<'de> {
    Ok(Option::<Vec<Item>>::deserialize(deserializer)?
        .map(|items| items.into_iter().map(|item| item.0).collect()))
}
