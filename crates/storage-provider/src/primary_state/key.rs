use storage_types::{
    BoundTableInfo, KeyAttributes, StorageError, StorageResult, validate_transact_key,
};

/// A primary identity has exactly the schema's components, their declared
/// types and valid DynamoDB scalar values. Attribute maps may contain more.
pub(crate) fn validate_primary_state_key(
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<()> {
    if key.len() != table.info.key_schema.len() {
        return Err(StorageError::validation(
            "primary state key does not match its schema",
        ));
    }
    validate_transact_key(&table.info, key)
}
