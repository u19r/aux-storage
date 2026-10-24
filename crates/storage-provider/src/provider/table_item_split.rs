use storage_types::{
    AttributeMap, AttributeValueLookup, KeyAttributes, SplitDynamoItem, StorageError,
    StorageResult, StoredTableInfo,
};

pub fn split_item_into_key_and_attributes_sync(
    item: impl Into<AttributeMap>,
    table_info: &StoredTableInfo,
) -> StorageResult<SplitDynamoItem> {
    let item = item.into();
    let mut key_attributes = KeyAttributes::with_capacity(table_info.key_schema.len());
    let mut non_key_attributes =
        AttributeMap::with_capacity(item.len().saturating_sub(table_info.key_schema.len()));

    for (attr_name, attr_value) in item.iter() {
        let is_key = table_info
            .key_schema
            .iter()
            .any(|key| key.attribute_name == attr_name);
        if is_key {
            key_attributes.insert(attr_name.to_string(), attr_value.clone());
        } else {
            non_key_attributes.insert(attr_name, attr_value.clone());
        }
    }

    for table_schema_key in &table_info.key_schema {
        if !key_attributes.contains_key(&table_schema_key.attribute_name) {
            return Err(StorageError::invalid_or_missing_key());
        }
    }

    Ok(SplitDynamoItem {
        key_attributes,
        all_attributes: item,
        non_key_attributes,
    })
}

/// Extract only the declared key attributes without materializing the full
/// split views used by write callers.
pub fn key_attributes_from_item(
    item: &impl AttributeValueLookup,
    key_schema: &[storage_types::KeySchemaElement],
) -> StorageResult<KeyAttributes> {
    let mut key_attributes = KeyAttributes::with_capacity(key_schema.len());
    for key in key_schema {
        let value = item
            .get_attribute_value(&key.attribute_name)
            .ok_or_else(|| StorageError::validation("One or more parameter values were invalid"))?;
        key_attributes.insert(key.attribute_name.clone(), value.clone());
    }
    Ok(key_attributes)
}
