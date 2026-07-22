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
                let item_key = if let Some(index_name) = index_name {
                    let index_key_schema = table_info
                        .global_secondary_indexes
                        .as_ref()
                        .and_then(|indexes| indexes.iter().find(|i| i.index_name == *index_name))
                        .map_or(&table_info.key_schema, |idx| &idx.key_schema);
                    ItemKey::from_key_schema_for_index(
                        table_info.table_name.clone(),
                        &table_info.key_schema,
                        index_name,
                        index_key_schema,
                        key,
                    )?
                    .ok_or_else(StorageError::invalid_or_missing_key)?
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
