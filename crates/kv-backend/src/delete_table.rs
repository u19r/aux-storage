//! KV table deletion under the catalog operation's native attempt.

use std::future::Future;

use storage_provider::{DeleteTableAttempt, TableReclaimPhase};
use storage_types::{BoundTableInfo, StorageResult};

use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> DeleteTableAttempt for KvCatalogAttempt<'_, A> {
    async fn load_external_cleanup_requirement(
        &mut self,
        table: &BoundTableInfo,
    ) -> StorageResult<bool> {
        Ok(self
            .identity_for(&table.info.table_name)?
            .external_cleanup_required)
    }

    fn delete_table_layout(
        &mut self,
        table: &BoundTableInfo,
        phase: TableReclaimPhase,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        self.tombstone_table(table, phase)
    }
}
