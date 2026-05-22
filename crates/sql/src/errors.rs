//! Centralized error helper functions for sqlite storage provider.
//!
//! These helpers standardize user-facing validation messages so tests and
//! higher layers can rely on consistent wording.

use storage_types::{IndexName, StorageError, StoredTableInfo};

pub(crate) fn missing_index_error(table: &StoredTableInfo, index: &IndexName) -> StorageError {
    StorageError::validation(format!(
        "The table '{}' does not have the specified index: {index}",
        table.table_name
    ))
}
