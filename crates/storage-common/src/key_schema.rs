//! Key schema validation & utilities.
//!
//! Keep purely about logical validation, independent of persistence.
use storage_types::{CreateTableRequest, StorageError};

/// Validate a CreateTableRequest's key schema & attribute definitions.
/// Returns Ok(()) if valid, otherwise a structured StorageError.
pub fn validate_create_table_request(req: &CreateTableRequest) -> Result<(), StorageError> {
    req.validate_key_schema()
}
