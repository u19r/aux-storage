use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{IndexName, ItemKey, KeyAttributes, StorageError, StoredTableInfo};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ExclusiveStartKey {
    Key(KeyAttributes),
    Token(String),
}

impl ExclusiveStartKey {
    pub fn to_page_token(
        &self,
        table_info: &StoredTableInfo,
        index_name: Option<&IndexName>,
    ) -> Result<String, StorageError> {
        match self {
            Self::Token(token) => Ok(token.clone()),
            Self::Key(key) => {
                let schema_error = || {
                    StorageError::validation("The provided key element does not match the schema")
                };
                let index_schema = index_name
                    .map(|name| {
                        table_info
                            .global_secondary_indexes
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .find(|index| index.index_name == *name)
                            .map(|index| index.key_schema.as_slice())
                            .ok_or_else(schema_error)
                    })
                    .transpose()?
                    .unwrap_or_default();
                let schema = || table_info.key_schema.iter().chain(index_schema);
                if key
                    .iter()
                    .any(|(name, _)| !schema().any(|element| element.attribute_name == name))
                {
                    return Err(schema_error());
                }
                for element in schema() {
                    let value = key.get(&element.attribute_name).ok_or_else(schema_error)?;
                    let definition = table_info
                        .attribute_definitions
                        .iter()
                        .find(|definition| definition.attribute_name == element.attribute_name)
                        .ok_or_else(schema_error)?;
                    if !value.matches_key_type(&definition.attribute_type) {
                        return Err(schema_error());
                    }
                    crate::validate_key_attribute_value_for_schema(element, value)?;
                }
                let item_key = if let Some(index_name) = index_name {
                    ItemKey::from_key_schema_for_index(
                        table_info.table_name.clone(),
                        &table_info.key_schema,
                        index_name,
                        index_schema,
                        key,
                    )?
                    .ok_or_else(schema_error)?
                } else {
                    ItemKey::from_key_schema(
                        table_info.table_name.clone(),
                        &table_info.key_schema,
                        key,
                    )?
                };
                Ok(item_key.next_page_token()?)
            }
        }
    }
}

impl From<String> for ExclusiveStartKey {
    fn from(value: String) -> Self {
        Self::Token(value)
    }
}

impl From<KeyAttributes> for ExclusiveStartKey {
    fn from(value: KeyAttributes) -> Self {
        Self::Key(value)
    }
}
