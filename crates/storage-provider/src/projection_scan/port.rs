use std::future::Future;

use storage_types::StorageResult;

use super::{GsiProjectionRow, GsiProjectionScan};
use crate::RangeReadPage;

pub trait GsiProjectionScanAttempt: Send {
    fn scan_gsi_projections(
        &mut self,
        scan: &GsiProjectionScan,
    ) -> impl Future<Output = StorageResult<RangeReadPage<GsiProjectionRow>>> + Send;
}

pub trait TtlProjectionScanAttempt: Send {
    fn scan_ttl_projections(
        &mut self,
        scan: &super::TtlProjectionScan,
    ) -> impl Future<Output = StorageResult<super::TtlProjectionPage>> + Send;
}

pub trait GsiProjectionMetadataAttempt: Send {
    fn encode_gsi_projection_addresses(
        &mut self,
        table: &storage_types::BoundTableInfo,
        keys: &[storage_backfill::GsiProjectionKey],
    ) -> impl Future<Output = StorageResult<Vec<super::NativeProjectionAddress>>> + Send;
    fn scan_gsi_tombstones(
        &mut self,
        scan: &super::GsiTombstoneScan,
    ) -> impl Future<Output = StorageResult<super::GsiTombstonePage>> + Send;
}
