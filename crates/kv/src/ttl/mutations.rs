use std::collections::HashMap;

use storage_common::ttl::{self, TtlConfigRecord};
use storage_types::{AttributeValue, StorageResult, StoredTableInfo, TableName, TimeToLiveStatus};

pub(crate) enum TtlIndexMutation {
    Delete(Vec<u8>),
    Put(Vec<u8>),
}

pub(crate) fn plan_ttl_index_mutations(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    ttl_config: Option<&TtlConfigRecord>,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Vec<TtlIndexMutation>> {
    let Some(config) = ttl_config else {
        return Ok(Vec::new());
    };
    if !matches!(
        config.status,
        TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
    ) {
        return Ok(Vec::new());
    }

    let old_item = old_item.filter(|item| !item.is_empty());
    let new_item = new_item.filter(|item| !item.is_empty());

    let old_key = old_item
        .map(|item| {
            ttl::ttl_index_key_for_item(table_name, table_info, &config.attribute_name, item)
        })
        .transpose()?
        .flatten();
    let new_key = new_item
        .map(|item| {
            ttl::ttl_index_key_for_item(table_name, table_info, &config.attribute_name, item)
        })
        .transpose()?
        .flatten();

    if old_key.is_some() && old_key == new_key {
        return Ok(Vec::new());
    }

    let mut mutations = Vec::new();
    if let Some(key) = old_key {
        mutations.push(TtlIndexMutation::Delete(key));
    }
    if let Some(key) = new_key {
        mutations.push(TtlIndexMutation::Put(key));
    }
    Ok(mutations)
}
