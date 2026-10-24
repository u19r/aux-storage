//! KV table deletion under the catalog operation's native attempt.

use std::future::Future;

use storage_provider::DeleteTableAttempt;
use storage_types::{BoundTableInfo, StorageResult, TimestampMillis};

use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> DeleteTableAttempt for KvCatalogAttempt<'_, A> {
    fn delete_table_layout(
        &mut self,
        table: &BoundTableInfo,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        self.tombstone_table(table, captured_at)
    }
}
