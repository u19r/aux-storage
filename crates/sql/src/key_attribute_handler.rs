use rusqlite::{Row, types::ValueRef};
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeyAttributes, KeySchemaElement,
    StorageError, StorageResult,
};

pub(crate) fn key_attributes_from_row(
    row: &Row,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
    column_prefix: Option<&str>,
) -> StorageResult<KeyAttributes> {
    let mut attributes = KeyAttributes::with_capacity(key_schema.len());
    for key in key_schema {
        attributes.insert(
            key.attribute_name.clone(),
            read_typed_key_attribute(
                row,
                &key.attribute_name,
                attribute_definitions,
                column_prefix,
            )?,
        );
    }
    Ok(attributes)
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
