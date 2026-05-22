//! High-level request validation helpers shared across backends.
use storage_types::{CreateTableRequest, StorageError};

/// Validate a CreateTableRequest with extended hooks (future: GSI limits,
/// stream spec checks).
pub fn validate_create_table(req: &CreateTableRequest) -> Result<(), StorageError> {
    req.validate_storage_common()
}
