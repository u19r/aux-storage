use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{IndexName, ItemKey, KeyAttributes, StorageError, StoredTableInfo};

/// The refusal text of a start key that does not fit the key schema; Scan
/// appends the cause except for a key that names an attribute outside the
/// schema.
pub const INVALID_STARTING_KEY: &str = "The provided starting key is invalid";

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
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
                    return Err(StorageError::validation(INVALID_STARTING_KEY));
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

const EMPTY_KEY_PREFIX: &str = "One or more parameter values are not valid. The AttributeValue \
                                for a key attribute cannot contain an empty ";

impl ExclusiveStartKey {
    /// The public error of a start key a Query refuses: AWS names the empty key
    /// attribute and otherwise answers with the plain invalid-key text.
    #[must_use]
    pub fn query_error(error: StorageError) -> StorageError {
        if error.to_string().starts_with(EMPTY_KEY_PREFIX) {
            return error;
        }
        StorageError::validation(INVALID_STARTING_KEY)
    }

    /// The public error of a start key a Scan refuses. AWS names a secondary
    /// index of the table for an empty key attribute, even a base-table one.
    #[must_use]
    pub fn scan_error(
        error: StorageError,
        table_info: &StoredTableInfo,
        index_name: Option<&IndexName>,
    ) -> StorageError {
        let message = error.to_string();
        if message == INVALID_STARTING_KEY {
            return error;
        }
        let empty = message
            .strip_prefix(EMPTY_KEY_PREFIX)
            .and_then(|rest| rest.split_once(" value. Key: "));
        let index = index_name.map(ToString::to_string).or_else(|| {
            table_info
                .global_secondary_indexes
                .as_deref()
                .and_then(<[_]>::first)
                .map(|index| index.index_name.to_string())
        });
        match (empty, index) {
            (Some((kind, attribute)), Some(index)) => StorageError::validation(format!(
                "One or more parameter values are not valid. A value specified for a secondary \
                 index key is not supported. The AttributeValue for a key attribute cannot \
                 contain an empty {kind} value. IndexName: {index}, IndexKey: {attribute}"
            )),
            (Some(_), None) => error,
            (None, _) => StorageError::validation(format!("{INVALID_STARTING_KEY}: {message}")),
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
