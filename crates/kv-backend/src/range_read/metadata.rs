use storage_types::{BoundTableInfo, StorageError, StorageResult, maintenance::TargetIncarnation};

use crate::{
    KvAttempt,
    catalog::{KvCatalogAttempt, table_id_from_history},
    gsi::index_incarnation,
};

impl<A: KvAttempt> KvCatalogAttempt<'_, A> {
    pub(super) fn projection_incarnation(
        &self,
        table: &BoundTableInfo,
        index: &storage_types::IndexName,
    ) -> StorageResult<TargetIncarnation> {
        let index = table
            .binding
            .gsi(index)
            .ok_or_else(|| StorageError::internal("KV projection index is absent"))?;
        Ok(index_incarnation(table_id_from_history(table.history_id)?, index.index_id).into())
    }
}
