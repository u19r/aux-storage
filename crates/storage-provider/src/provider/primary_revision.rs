use storage_types::{ConversionError, KeyAttributes, StorageError, StorageResult};

pub fn canonical_revision_key(key: &KeyAttributes) -> StorageResult<String> {
    if key.is_empty() {
        return Err(StorageError::invalid_or_missing_key());
    }
    key.canonical_dynamo_json().map_err(|error| match error {
        ConversionError::Serialization(message) => StorageError::validation(message),
        other => StorageError::validation(format!(
            "revision key must be Dynamo JSON encodable: {other}"
        )),
    })
}
