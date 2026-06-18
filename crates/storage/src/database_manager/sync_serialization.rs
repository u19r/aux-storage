use std::collections::HashMap;

use serde::{Serialize, Serializer, ser::SerializeMap};
use storage_types::{AttributeValue, KeyAttributes, StorageEnum, StorageError, StorageResult};

pub(super) fn stable_attribute_json(
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    Ok(serde_json::to_string(&SortedAttributeMap(item)).map_err(StorageEnum::Serialization)?)
}

pub(super) fn stable_key_json(key: &KeyAttributes) -> StorageResult<String> {
    key.canonical_dynamo_json()
        .map_err(|error| StorageError::internal(&error.to_string()))
}

struct SortedAttributeMap<'a>(&'a HashMap<String, AttributeValue>);

impl Serialize for SortedAttributeMap<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut entries = self.0.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(left, _)| *left);

        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}
