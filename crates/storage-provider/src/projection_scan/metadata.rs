use std::{num::NonZeroU32, sync::Arc};

use serde::{Deserialize, Serialize};
use storage_types::{
    BoundTableInfo, IndexName, ItemRevision, StorageError, StorageResult, TableHistoryId,
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

#[derive(Clone, Serialize, Deserialize)]
pub struct ProjectionMetadataCursor {
    address: NativeProjectionAddress,
    position: Vec<u8>,
}

impl ProjectionMetadataCursor {
    pub fn try_new(address: NativeProjectionAddress, position: Vec<u8>) -> StorageResult<Self> {
        if position.is_empty() {
            return Err(StorageError::internal("empty projection metadata cursor"));
        }
        Ok(Self { address, position })
    }
    pub fn require_scope(
        &self,
        table: &BoundTableInfo,
        index: &IndexName,
        incarnation: &TargetIncarnation,
    ) -> StorageResult<()> {
        self.address.require_scope(table, index, incarnation)
    }
    pub fn native_position(&self) -> &[u8] {
        &self.position
    }
    pub fn native_address(&self) -> &NativeProjectionAddress {
        &self.address
    }
}

pub struct GsiTombstoneScan {
    pub table: Arc<BoundTableInfo>,
    pub index: IndexName,
    pub after: Option<ProjectionMetadataCursor>,
    pub limit: NonZeroU32,
}

impl GsiTombstoneScan {
    pub fn try_new(
        table: Arc<BoundTableInfo>,
        index: IndexName,
        after: Option<ProjectionMetadataCursor>,
        limit: NonZeroU32,
    ) -> StorageResult<Self> {
        if index.is_ttl_index()
            || limit.get() > storage_common::MAX_SCAN_LIMIT
            || !table
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|definition| definition.index_name == index)
        {
            return Err(StorageError::internal("invalid projection metadata scan"));
        }
        Ok(Self {
            table,
            index,
            after,
            limit,
        })
    }
}

pub struct GsiTombstoneRow {
    pub address: NativeProjectionAddress,
    pub revision: ItemRevision,
}

pub struct GsiTombstonePage {
    pub rows: Vec<GsiTombstoneRow>,
    pub continuation: Option<ProjectionMetadataCursor>,
}

impl GsiTombstonePage {
    pub fn finish(self, scan: &GsiTombstoneScan) -> StorageResult<Self> {
        if self.rows.len() > scan.limit.get() as usize
            || self.rows.iter().any(|row| {
                row.address.history != scan.table.history_id
                    || row.address.index != scan.index
                    || row.address.key.is_empty()
            })
        {
            return Err(StorageError::internal(
                "invalid projection metadata page scope or count",
            ));
        }
        if let Some(cursor) = &self.continuation
            && (self
                .rows
                .last()
                .is_none_or(|row| row.address != cursor.address)
                || scan.after.as_ref().is_some_and(|after| {
                    after.position == cursor.position || after.address == cursor.address
                }))
        {
            return Err(StorageError::internal(
                "projection metadata cursor cannot advance",
            ));
        }
        Ok(self)
    }
}
