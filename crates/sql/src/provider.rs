use std::collections::HashMap;

use storage_types::{AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, TableName};

use crate::{names::AttributeName, sql_types::SqlIdentifier};

pub fn normalized_table_identifier(table_name: &TableName) -> SqlIdentifier {
    let raw = format!("table_{}", table_name.sanitized_name());
    SqlIdentifier::new(raw)
}

pub fn normalized_column_identifier(attribute_name: &str) -> SqlIdentifier {
    SqlIdentifier::new(AttributeName::new(attribute_name).sanitized().to_string())
}

pub fn collect_key_columns(key_schema: &[KeySchemaElement]) -> Vec<String> {
    key_schema
        .iter()
        .map(|key| key.attribute_name.clone())
        .collect()
}

pub fn collect_declared_attributes(
    attribute_definitions: &[AttributeDefinition],
) -> HashMap<String, String> {
    attribute_definitions
        .iter()
        .map(|definition| {
            (
                definition.attribute_name.clone(),
                match definition.attribute_type {
                    storage_types::KeyAttributeType::S => "S",
                    storage_types::KeyAttributeType::N => "N",
                    storage_types::KeyAttributeType::B => "B",
                }
                .to_string(),
            )
        })
        .collect()
}

pub fn has_any_gsis(global_secondary_indexes: Option<&Vec<GlobalSecondaryIndex>>) -> bool {
    global_secondary_indexes.is_some_and(|gsis| !gsis.is_empty())
}
