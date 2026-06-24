use foundationdb::FdbError;
use storage_types::StorageError;

pub(super) fn map_fdb_error(scope: &str, err: FdbError) -> StorageError {
    StorageError::internal(&format!("{scope}: {err}"))
}
