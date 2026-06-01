use std::collections::HashMap;

use storage_types::{
    AttributeMap, AttributeValue, ExclusiveStartKey, IndexName, ItemKey, KeyAttributes,
    MAX_QUERY_SCAN_RESPONSE_BYTES, StoredTableInfo, TableName, WireItem,
    validate_key_attributes_for_schema, validate_transact_key,
};

use crate::raw_dynamodb_response::wire_item_json_len_upper_bound;

pub(crate) fn resolve_exclusive_start_key(
    exclusive_start_key: Option<&ExclusiveStartKey>,
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    invalid_starting_key_message: &'static str,
) -> storage_types::StorageResult<Option<String>> {
    exclusive_start_key
        .map(|key| {
            if index_name.is_none()
                && let ExclusiveStartKey::Key(key_attributes) = key
            {
                validate_transact_key(table_info, key_attributes).map_err(|_| {
                    storage_types::StorageError::validation(invalid_starting_key_message)
                })?;
                validate_key_attributes_for_schema(&table_info.key_schema, key_attributes)
                    .map_err(|_| {
                        storage_types::StorageError::validation(invalid_starting_key_message)
                    })?;
            }
            key.to_page_token(table_info, index_name)
                .map_err(|_| storage_types::StorageError::validation(invalid_starting_key_message))
        })
        .transpose()
}

pub(crate) fn page_token_to_key_attributes(
    token: Option<&str>,
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
) -> storage_types::StorageResult<Option<KeyAttributes>> {
    let Some(token) = token else {
        return Ok(None);
    };
    let index_name = index_name.cloned();
    let Some(item_key) = ItemKey::item_key_from_next_page_token(token, table_info, &index_name)?
    else {
        return Ok(None);
    };
    Ok(Some(item_key_to_key_attributes(&item_key, table_info)))
}

pub(crate) fn paginate_items_by_response_bytes(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    source_items: &[&HashMap<String, AttributeValue>],
    projected_items: Vec<HashMap<String, AttributeValue>>,
    provider_last_evaluated_key: Option<String>,
) -> storage_types::StorageResult<(Vec<AttributeMap>, Option<String>)> {
    let mut page_items = Vec::with_capacity(projected_items.len());
    let mut page_bytes = 2usize;

    for (index, projected_item) in projected_items.into_iter().enumerate() {
        let item = AttributeMap::from(projected_item);
        let item_bytes = serde_json::to_vec(&item)?.len();
        let separator_bytes = usize::from(!page_items.is_empty());
        if !page_items.is_empty()
            && page_bytes + separator_bytes + item_bytes > MAX_QUERY_SCAN_RESPONSE_BYTES
        {
            let token = last_evaluated_key_for_item(
                table_name,
                table_info,
                index_name,
                source_items[index - 1],
            )?;
            return Ok((page_items, Some(token)));
        }
        page_bytes += separator_bytes + item_bytes;
        page_items.push(item);
    }

    Ok((page_items, provider_last_evaluated_key))
}

pub(crate) fn paginate_wire_items_by_response_bytes(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    items: Vec<WireItem>,
    provider_last_evaluated_key: Option<String>,
) -> storage_types::StorageResult<(Vec<WireItem>, Option<String>)> {
    let mut page_items: Vec<WireItem> = Vec::with_capacity(items.len());
    let mut page_bytes = 2usize;

    for (index, item) in items.into_iter().enumerate() {
        let item_bytes = wire_item_json_len_upper_bound(&item)?;
        let separator_bytes = usize::from(!page_items.is_empty());
        if !page_items.is_empty()
            && page_bytes + separator_bytes + item_bytes > MAX_QUERY_SCAN_RESPONSE_BYTES
        {
            let source_item = page_items[index - 1].clone().into_attribute_map()?;
            let token =
                last_evaluated_key_for_item(table_name, table_info, index_name, &source_item)?;
            return Ok((page_items, Some(token)));
        }
        page_bytes += separator_bytes + item_bytes;
        page_items.push(item);
    }

    Ok((page_items, provider_last_evaluated_key))
}

fn item_key_to_key_attributes(item_key: &ItemKey, table_info: &StoredTableInfo) -> KeyAttributes {
    let mut attributes = KeyAttributes::new();
    match item_key {
        ItemKey::Table(key) => {
            insert_schema_key_values(
                &mut attributes,
                &table_info.key_schema,
                &key.hash_key,
                key.range_key.as_ref(),
            );
        }
        ItemKey::Index(key) => {
            insert_schema_key_values(
                &mut attributes,
                &table_info.key_schema,
                &key.table_key.hash_key,
                key.table_key.range_key.as_ref(),
            );
            let index_key_schema = table_info
                .global_secondary_indexes
                .as_ref()
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find(|index| index.index_name == key.index_id)
                })
                .map_or(&table_info.key_schema, |index| &index.key_schema);
            insert_schema_key_values(
                &mut attributes,
                index_key_schema,
                &key.hash_key,
                key.range_key.as_ref(),
            );
        }
        ItemKey::IndexPrefix(key) => {
            let index_key_schema = table_info
                .global_secondary_indexes
                .as_ref()
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find(|index| index.index_name == key.index_id)
                })
                .map_or(&table_info.key_schema, |index| &index.key_schema);
            insert_schema_key_values(
                &mut attributes,
                index_key_schema,
                &key.hash_key,
                key.range_key.as_ref(),
            );
        }
    }
    attributes
}

fn insert_schema_key_values(
    attributes: &mut KeyAttributes,
    key_schema: &[storage_types::KeySchemaElement],
    hash_key: &AttributeValue,
    range_key: Option<&AttributeValue>,
) {
    for key_element in key_schema {
        match key_element.key_type {
            storage_types::KeyType::Hash => {
                attributes.insert(key_element.attribute_name.clone(), hash_key.clone());
            }
            storage_types::KeyType::Range => {
                if let Some(range_key) = range_key {
                    attributes.insert(key_element.attribute_name.clone(), range_key.clone());
                }
            }
        }
    }
}

fn last_evaluated_key_for_item(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    item: &HashMap<String, AttributeValue>,
) -> storage_types::StorageResult<String> {
    let key = if let Some(index_name) = index_name {
        let index = table_info
            .global_secondary_indexes
            .as_ref()
            .and_then(|indexes| {
                indexes
                    .iter()
                    .find(|index| index.index_name.as_ref() == index_name.as_ref())
            })
            .ok_or_else(|| {
                storage_types::StorageError::validation(format!(
                    "One or more parameter values were invalid: The table does not have the \
                     specified index: {index_name}"
                ))
            })?;
        ItemKey::from_key_schema_for_index(
            table_name.clone(),
            &table_info.key_schema,
            index_name,
            &index.key_schema,
            item,
        )
        .map_err(|err| storage_types::StorageError::validation(err.to_string()))?
        .ok_or_else(storage_types::StorageError::invalid_or_missing_key)?
    } else {
        ItemKey::from_key_schema(table_name.clone(), &table_info.key_schema, item)
            .map_err(|err| storage_types::StorageError::validation(err.to_string()))?
    };

    key.next_page_token()
        .map_err(|err| storage_types::StorageError::validation(err.to_string()))
}
