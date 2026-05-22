use std::collections::{BTreeMap, HashMap};

use storage_types::{AttributeValue, StorageError, StorageResult};

pub(crate) fn canonical_dynamo_json(value: &AttributeValue) -> StorageResult<String> {
    serde_json::to_string(value)
        .map_err(|error| StorageError::internal(&format!("serialize attribute value: {error}")))
}

pub(crate) fn canonical_dynamo_map_json(
    map: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    let ordered = map
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&ordered)
        .map_err(|error| StorageError::internal(&format!("serialize attribute map: {error}")))
}
