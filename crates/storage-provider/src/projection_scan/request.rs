use std::sync::Arc;

use storage_common::{DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT};
use storage_types::{BoundTableInfo, IndexName, ScanTableRequest, StorageResult};

use crate::{BoundRangeReadPage, BoundRangeReadPlan, RangeReadPlan, RangeReadiness};

/// A bounded native scan of one current GSI incarnation. The adapter must
/// check native readiness in the same attempt before executing its page.
pub struct GsiProjectionScan {
    table: Arc<BoundTableInfo>,
    index: IndexName,
    plan: RangeReadPlan,
}

impl GsiProjectionScan {
    pub fn try_new(
        table: Arc<BoundTableInfo>,
        index: IndexName,
        after: Option<String>,
        limit: Option<u32>,
    ) -> StorageResult<Self> {
        let plan = RangeReadPlan::try_from_scan(ScanTableRequest {
            table_name: table.table_name.clone(),
            index_name: Some(index.clone()),
            limit,
            exclusive_start_key: after,
            consistent_read: false,
        })?;
        plan.bind(Arc::clone(&table.info), RangeReadiness::GsiPending)?;
        Ok(Self { table, index, plan })
    }
    pub fn table(&self) -> &BoundTableInfo {
        &self.table
    }
    pub fn index(&self) -> &IndexName {
        &self.index
    }
    pub fn bind(&self) -> StorageResult<BoundRangeReadPlan<'_>> {
        self.plan
            .bind(Arc::clone(&self.table.info), RangeReadiness::GsiPending)
    }
    pub fn page<'a>(
        &self,
        bound: &'a BoundRangeReadPlan<'a>,
    ) -> StorageResult<BoundRangeReadPage<'a>> {
        bound.page(DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)
    }
}

/// Physical expiration-index query facts, without maintenance authority.
pub struct TtlIndexRead {
    pub table_name: storage_types::TableName,
    pub cutoff_seconds: i64,
    pub cursor: Option<storage_common::TtlMaintenanceCursor>,
    pub limit: std::num::NonZeroUsize,
}

impl From<&crate::maintenance::ExpirationScanRead> for TtlIndexRead {
    fn from(read: &crate::maintenance::ExpirationScanRead) -> Self {
        Self {
            table_name: read.table_name.clone(),
            cutoff_seconds: read.cutoff_seconds,
            cursor: read.cursor.clone(),
            limit: read.limit,
        }
    }
}

pub struct TtlProjectionScan {
    table: Arc<BoundTableInfo>,
    read: TtlIndexRead,
}

impl TtlProjectionScan {
    pub fn try_new(
        table: Arc<BoundTableInfo>,
        after: Option<storage_common::TtlMaintenanceCursor>,
        limit: std::num::NonZeroUsize,
    ) -> StorageResult<Self> {
        u32::try_from(
            limit
                .get()
                .checked_add(1)
                .ok_or_else(|| storage_types::StorageError::internal("TTL scan limit overflow"))?,
        )
        .map_err(|_| storage_types::StorageError::internal("TTL scan limit overflow"))?;
        if let Some(cursor) = &after {
            crate::maintenance::ExpirationCandidateCursor::try_from(cursor)?;
        }
        let read = TtlIndexRead {
            table_name: table.table_name.clone(),
            cutoff_seconds: i64::MAX,
            cursor: after,
            limit,
        };
        Ok(Self { table, read })
    }
    pub fn table(&self) -> &BoundTableInfo {
        &self.table
    }
    pub fn read(&self) -> &TtlIndexRead {
        &self.read
    }
}
