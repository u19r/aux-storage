use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Item, item_json::TableItemsRef};
use crate::{TableName, VecMap, wire_item::WireItem};

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
