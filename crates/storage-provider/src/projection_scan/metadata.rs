use serde::{Deserialize, Serialize};
use storage_types::{
    BoundTableInfo, IndexName, StorageError, StorageResult, TableHistoryId,
    maintenance::TargetIncarnation,
};

/// Opaque native address qualified by both table and index incarnation.
/// Its key bytes use the provider encoding and are not a canonical source key.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeProjectionAddress {
    history: TableHistoryId,
    index: IndexName,
    incarnation: TargetIncarnation,
    key: Vec<u8>,
}

impl NativeProjectionAddress {
    pub fn try_new(
        table: &BoundTableInfo,
        index: &IndexName,
        incarnation: TargetIncarnation,
        key: Vec<u8>,
    ) -> StorageResult<Self> {
        if key.is_empty() || index.is_ttl_index() {
            return Err(StorageError::internal("invalid native projection address"));
        }
        Ok(Self {
            history: table.history_id,
            index: index.clone(),
            incarnation,
            key,
        })
    }
    pub fn require_scope(
        &self,
        table: &BoundTableInfo,
        index: &IndexName,
        incarnation: &TargetIncarnation,
    ) -> StorageResult<()> {
        if self.history != table.history_id
            || &self.index != index
            || &self.incarnation != incarnation
            || self.key.is_empty()
        {
            return Err(StorageError::internal(
                "native projection address has another incarnation",
            ));
        }
        Ok(())
    }
    pub fn native_key(&self) -> &[u8] {
        &self.key
    }
}
