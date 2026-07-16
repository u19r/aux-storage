use storage_provider::AtomicItemReadModifyWriteRequest;
use storage_types::{StorageError, StorageResult};

use crate::database_manager::DatabaseManager;

impl DatabaseManager {
    pub async fn atomic_item_read_modify_write(
        &self,
        request: AtomicItemReadModifyWriteRequest,
    ) -> StorageResult<Vec<u8>> {
        let operation = self
            .resolve_storage_operation(request.table_name.clone())
            .await?;
        if operation.route.is_some() {
            return Err(StorageError::unsupported(
                "atomic item read-modify-write does not support namespace routing",
            ));
        }
        self.storage.atomic_item_read_modify_write(request).await
    }
}
