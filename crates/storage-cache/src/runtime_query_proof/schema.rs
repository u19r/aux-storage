use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use storage_types::{
    AttributeValue, KeyAttributeType, KeySchemaElement, KeyType, StorageError, StorageResult,
    StoredTableInfo, numeric::SortableNumeric,
};

pub fn hash_key_name(key_schema: &[KeySchemaElement]) -> StorageResult<&str> {
    key_schema
        .iter()
        .find(|element| element.key_type == KeyType::Hash)
        .map(|element| element.attribute_name.as_str())
        .ok_or_else(|| StorageError::internal("missing HASH key schema element"))
}

#[must_use]
pub fn range_key_name(key_schema: &[KeySchemaElement]) -> Option<&str> {
    key_schema
        .iter()
        .find(|element| element.key_type == KeyType::Range)
        .map(|element| element.attribute_name.as_str())
}

pub fn query_space_key_schema<'a>(
    table_info: &'a StoredTableInfo,
    index_name: Option<&str>,
) -> StorageResult<&'a [KeySchemaElement]> {
    let Some(index_name) = index_name else {
        return Ok(&table_info.key_schema);
    };
    table_info
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| {
            indexes
                .iter()
                .find(|index| index.index_name.as_ref() == index_name)
        })
        .map(|index| index.key_schema.as_slice())
        .ok_or_else(|| {
            StorageError::validation(format!(
                "query proof cache index '{index_name}' not found in table schema",
            ))
        })
}

pub fn stable_query_space_schema_fingerprint(
    table_info: &StoredTableInfo,
    index_name: Option<&str>,
) -> StorageResult<u64> {
    let query_key_schema = query_space_key_schema(table_info, index_name)?;
    let mut hasher = DefaultHasher::new();
    index_name.hash(&mut hasher);
    for definition in &table_info.attribute_definitions {
        definition.attribute_name.hash(&mut hasher);
        match definition.attribute_type {
            storage_types::KeyAttributeType::S => "S".hash(&mut hasher),
            storage_types::KeyAttributeType::N => "N".hash(&mut hasher),
            storage_types::KeyAttributeType::B => "B".hash(&mut hasher),
        }
    }
    for element in &table_info.key_schema {
        element.attribute_name.hash(&mut hasher);
        match element.key_type {
            KeyType::Hash => "HASH".hash(&mut hasher),
            KeyType::Range => "RANGE".hash(&mut hasher),
        }
    }
    for element in query_key_schema {
        element.attribute_name.hash(&mut hasher);
        match element.key_type {
            KeyType::Hash => "HASH".hash(&mut hasher),
            KeyType::Range => "RANGE".hash(&mut hasher),
        }
    }
    Ok(hasher.finish())
}

pub fn key_attribute_type_for_name(
    table_info: &StoredTableInfo,
    attribute_name: &str,
) -> StorageResult<KeyAttributeType> {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == attribute_name)
        .map(|definition| definition.attribute_type.clone())
        .ok_or_else(|| {
            StorageError::internal(&format!(
                "missing attribute definition for query manifest key '{attribute_name}'",
            ))
        })
}

pub fn scalar_order_repr_for_type(
    attribute_type: &KeyAttributeType,
    value: &AttributeValue,
) -> StorageResult<String> {
    let scalar = value.inner_str().map_err(|err| {
        StorageError::internal(&format!("query manifest scalar key encoding failed: {err}"))
    })?;
    match attribute_type {
        KeyAttributeType::S => Ok(format!("s:{scalar}")),
        KeyAttributeType::B => Ok(format!("b:{scalar}")),
        KeyAttributeType::N => {
            let sortable = SortableNumeric::ascending(scalar).map_err(|err| {
                StorageError::internal(&format!("encode numeric query manifest key: {err:?}"))
            })?;
            Ok(format!("n:{}", sortable.as_str()))
        }
    }
}

pub fn sort_key_order_repr_for_schema_value(
    table_info: &StoredTableInfo,
    attribute_name: &str,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<Option<String>> {
    let Some(value) = item.get(attribute_name) else {
        return Ok(None);
    };
    let attribute_type = key_attribute_type_for_name(table_info, attribute_name)?;
    scalar_order_repr_for_type(&attribute_type, value).map(Some)
}

pub fn primary_key_from_schema(
    key_schema: &[KeySchemaElement],
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<HashMap<String, AttributeValue>> {
    let mut key = HashMap::with_capacity(key_schema.len());
    for element in key_schema {
        let Some(value) = item.get(&element.attribute_name) else {
            return Err(StorageError::internal(&format!(
                "missing key attribute '{}' while deriving query manifest state",
                element.attribute_name
            )));
        };
        key.insert(element.attribute_name.clone(), value.clone());
    }
    Ok(key)
}
