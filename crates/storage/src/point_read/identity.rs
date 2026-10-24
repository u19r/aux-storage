use crate::types::{CanonicalKeyAttributes, KeyAttributes, StorageError, StorageResult, TableName};

/// Physical identity for strong full-item reads on one database client.
/// AuxStorage owns numeric normalization; JSON frames names and values.
pub fn key_fingerprint(table_name: &TableName, key: &KeyAttributes) -> StorageResult<String> {
    let canonical = CanonicalKeyAttributes::try_from(key)
        .map_err(|_| StorageError::invalid_or_missing_key())?;
    serde_json::to_string(&(table_name, canonical)).map_err(|error| StorageError::internal(&error))
}
