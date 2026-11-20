//! Cache one protected GSI table and TTL observation within a native attempt.

use std::sync::Arc;

use storage_common::TtlConfigRecord;
use storage_types::{BoundTableInfo, StorageError, StorageResult, TableName};

use super::{GsiPort, GsiTargetAdmission, transaction::GsiAttempt};

impl<P: GsiPort, S: GsiTargetAdmission<P>> GsiAttempt<'_, P, S> {
    pub(super) async fn load_table(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Arc<BoundTableInfo>> {
        if let Some(table) = &self.table {
            if table.info.table_name != *name {
                return Err(StorageError::internal("GSI attempt targets another table"));
            }
            return Ok(Arc::clone(table));
        }
        let stamp = self
            .port
            .read_catalog_stamp(name)
            .await?
            .ok_or_else(|| StorageError::internal("GSI target table is missing"))?;
        let table = self.port.read_table_at_stamp(name, stamp).await?;
        self.table = Some(Arc::clone(&table));
        Ok(table)
    }

    pub(super) async fn load_ttl(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        let table = self.load_table(name).await?;
        if let Some(ttl) = &self.ttl {
            return Ok(ttl.clone());
        }
        let ttl = self.port.read_ttl_config(&table).await?;
        self.ttl = Some(ttl.clone());
        Ok(ttl)
    }
}
