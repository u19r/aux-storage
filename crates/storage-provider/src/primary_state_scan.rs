//! Bounded primary-state reads for higher-level capture owners. This is a
//! native page observation, not a frozen snapshot or permission to serve it.

use std::{future::Future, sync::Arc};

use storage_common::{DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT};
use storage_types::{BoundTableInfo, ScanTableRequest, StorageResult};

use crate::{
    BoundRangeReadPage, BoundRangeReadPlan, PrimaryStateRow, RangeReadPage, RangeReadPlan,
    RangeReadiness,
};

/// Private request fields ensure this scan can only select the primary table.
/// The catalog carries the incarnation; callers cannot supply a GSI or filter.
pub struct PrimaryStateScan {
    table: Arc<BoundTableInfo>,
    plan: RangeReadPlan,
}

impl PrimaryStateScan {
    pub fn try_new(
        table: Arc<BoundTableInfo>,
        after: Option<String>,
        limit: Option<u32>,
    ) -> StorageResult<Self> {
        let plan = RangeReadPlan::try_from_scan(ScanTableRequest {
            table_name: table.table_name.clone(),
            index_name: None,
            limit,
            exclusive_start_key: after,
            consistent_read: true,
        })?;
        Ok(Self { table, plan })
    }

    pub fn table(&self) -> &BoundTableInfo {
        &self.table
    }

    pub fn bind(&self) -> StorageResult<BoundRangeReadPlan<'_>> {
        self.plan
            .bind(self.table.info.clone(), RangeReadiness::BaseTable)
    }

    pub fn page<'a>(
        &self,
        bound: &'a BoundRangeReadPlan<'a>,
    ) -> StorageResult<BoundRangeReadPage<'a>> {
        bound.page(DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)
    }
}

pub trait PrimaryStateScanAttempt: Send {
    /// Return complete live primary observations, including indexer
    /// declarations, revision, provenance and TTL epoch. Deleted authority is
    /// stored separately and is not included in this live-row scan.
    fn scan_primary_state(
        &mut self,
        scan: &PrimaryStateScan,
    ) -> impl Future<Output = StorageResult<RangeReadPage<PrimaryStateRow>>> + Send;
}
