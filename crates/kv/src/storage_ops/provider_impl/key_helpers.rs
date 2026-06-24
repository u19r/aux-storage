use crate::storage_ops::provider_impl::*;

pub(super) fn key_attributes_for_item(
    table_info: &StoredTableInfo,
    item: &impl storage_types::AttributeValueLookup,
) -> StorageResult<KeyAttributes> {
    let mut key_attributes = KeyAttributes::with_capacity(table_info.key_schema.len());
    for key in &table_info.key_schema {
        let value = item
            .get_attribute_value(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        key_attributes.insert(key.attribute_name.clone(), value.clone());
    }
    Ok(key_attributes)
}
