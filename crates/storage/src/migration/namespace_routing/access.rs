use storage_types::{
    StorageEnum, StorageError, StorageResult, TableName, context::WrappedError as _,
};

use crate::tables::Tables;

#[must_use]
pub fn is_shared_table_enabled_namespace_route(table_name: &TableName) -> bool {
    Tables::parse_namespace_table_name(table_name).is_some()
}

pub fn reject_direct_shared_table_access(table_name: &TableName) -> StorageResult<()> {
    if Tables::parse_shared_table_location(table_name).is_some() {
        return Err(StorageError::validation(
            "shared table routing failed closed: direct shared table access is not allowed",
        ));
    }
    Ok(())
}

pub(crate) fn is_missing_sys_namespaces_table_error(error: &StorageError) -> bool {
    matches!(
        error.to_enum(),
        StorageEnum::TableNotFound { name, .. } if name == Tables::sys_namespaces().as_ref()
    ) || matches!(
        error.to_enum(),
        StorageEnum::ResourceNotFound {
            resource_type,
            resource_id
        } if resource_type == &"table" && resource_id == Tables::sys_namespaces().as_ref()
    )
}

pub(crate) fn is_retryable_cutover_watcher_error(error: &StorageError) -> bool {
    is_missing_sys_namespaces_table_error(error)
        || matches!(
            error.to_enum(),
            StorageEnum::TransactionConflict { .. } | StorageEnum::TransactionInProgress { .. }
        )
}

pub fn is_retryable_pause_error(error: &StorageError) -> bool {
    matches!(error.to_enum(), StorageEnum::Throttled { .. })
}
