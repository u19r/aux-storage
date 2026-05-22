use std::{borrow::Cow, collections::HashMap};

use rusqlite::{Row, types::ValueRef};
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType, StorageError,
    StorageResult, StoredTableInfo, WireItemKeyAttributes,
};

/// Adds key attributes from individual columns to the result `HashMap`
///
/// # Arguments
///
/// * `row` - The database row to read from
/// * `table_info` - The table's information
/// * `result` - The `HashMap` to add attributes to
///
/// # Returns
///
/// The updated `HashMap` with key attributes added
pub fn add_key_attributes_from_columns(
    row: &Row,
    table_info: &StoredTableInfo,
    result: &mut HashMap<String, AttributeValue>,
) {
    for key_elem in &table_info.key_schema {
        let column_name = &key_elem.attribute_name;

        let value_string = if let Ok(s) = row.get::<_, String>(column_name.as_str()) {
            Some(s)
        } else if let Ok(i) = row.get::<_, i64>(column_name.as_str()) {
            Some(i.to_string())
        } else if let Ok(f) = row.get::<_, f64>(column_name.as_str()) {
            Some(f.to_string())
        } else if let Ok(Some(s)) = row.get::<_, Option<String>>(column_name.as_str()) {
            Some(s)
        } else if let Ok(Some(i)) = row.get::<_, Option<i64>>(column_name.as_str()) {
            Some(i.to_string())
        } else if let Ok(Some(f)) = row.get::<_, Option<f64>>(column_name.as_str()) {
            Some(f.to_string())
        } else {
            // If column doesn't exist, try to read from the attributes_blob
            // This handles the case where GSI key attributes weren't stored as columns
            // when the item was originally inserted
            if let Ok(blob) = row.get::<_, String>("attributes_blob") {
                if let Ok(attributes) = serde_json::from_str::<
                    std::collections::HashMap<String, serde_json::Value>,
                >(&blob)
                {
                    if let Some(value) = attributes.get(column_name) {
                        match value {
                            serde_json::Value::String(s) => Some(s.clone()),
                            serde_json::Value::Number(n) => Some(n.to_string()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        let Some(v) = value_string else { continue };
        let Some(attr_def) = table_info
            .attribute_definitions
            .iter()
            .find(|attr| attr.attribute_name == *column_name)
        else {
            continue;
        };

        let attr_value = match attr_def.attribute_type {
            KeyAttributeType::S => AttributeValue::S(v),
            KeyAttributeType::N => AttributeValue::N(v),
            KeyAttributeType::B => AttributeValue::B(v),
        };
        result.insert(column_name.clone(), attr_value);
    }
}

pub(crate) fn wire_item_key_attributes_from_row(
    row: &Row,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
    column_prefix: Option<&str>,
) -> StorageResult<WireItemKeyAttributes> {
    let Some(hash_key) = key_schema.iter().find(|key| key.key_type == KeyType::Hash) else {
        return Err(StorageError::invalid_or_missing_key());
    };
    let hash_key_value = read_typed_key_attribute(
        row,
        &hash_key.attribute_name,
        attribute_definitions,
        column_prefix,
    )?;

    let range_key = key_schema.iter().find(|key| key.key_type == KeyType::Range);
    let sort_key_name = range_key.map(|key| intern_common_key_name(&key.attribute_name));
    let sort_key = if let Some(range_key) = range_key {
        Some(read_typed_key_attribute(
            row,
            &range_key.attribute_name,
            attribute_definitions,
            column_prefix,
        )?)
    } else {
        None
    };

    Ok(WireItemKeyAttributes::new_with_names(
        intern_common_key_name(&hash_key.attribute_name),
        hash_key_value,
        sort_key_name,
        sort_key,
    ))
}

fn read_typed_key_attribute(
    row: &Row,
    attribute_name: &str,
    attribute_definitions: &[AttributeDefinition],
    column_prefix: Option<&str>,
) -> StorageResult<AttributeValue> {
    let value = read_key_column_as_string(row, attribute_name, column_prefix)?;
    let key_type = attribute_definitions
        .iter()
        .find(|def| def.attribute_name == attribute_name)
        .map_or(KeyAttributeType::S, |def| def.attribute_type.clone());
    Ok(match key_type {
        KeyAttributeType::S => AttributeValue::S(value),
        KeyAttributeType::N => AttributeValue::N(value),
        KeyAttributeType::B => AttributeValue::B(value),
    })
}

fn read_key_column_as_string(
    row: &Row,
    attribute_name: &str,
    column_prefix: Option<&str>,
) -> StorageResult<String> {
    let value = if let Some(prefix) = column_prefix {
        let mut prefixed = String::with_capacity(prefix.len() + attribute_name.len());
        prefixed.push_str(prefix);
        prefixed.push_str(attribute_name);
        row.get_ref(prefixed.as_str())
    } else {
        row.get_ref(attribute_name)
    }
    .map_err(|_| StorageError::invalid_or_missing_key())?;
    match value {
        ValueRef::Null => Err(StorageError::invalid_or_missing_key()),
        ValueRef::Integer(raw) => Ok(raw.to_string()),
        ValueRef::Real(raw) => Ok(raw.to_string()),
        ValueRef::Text(raw) => std::str::from_utf8(raw)
            .map(std::string::ToString::to_string)
            .map_err(|_| StorageError::invalid_or_missing_key()),
        ValueRef::Blob(raw) => std::str::from_utf8(raw)
            .map(std::string::ToString::to_string)
            .map_err(|_| StorageError::invalid_or_missing_key()),
    }
}

fn intern_common_key_name(name: &str) -> Cow<'static, str> {
    match name {
        "pk" => Cow::Borrowed("pk"),
        "sk" => Cow::Borrowed("sk"),
        _ => Cow::Owned(name.to_string()),
    }
}
