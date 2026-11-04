//! Physical GSI pages and conditional writes inside the caller's attempt.

use std::future::Future;

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildTarget, GsiPageLimit, GsiProjection, GsiProjectionKey,
    GsiProjectionStateReadBatch, GsiScanSourcePage,
};
use storage_types::{BoundTableInfo, StorageResult, SystemStreamCheckpoint};

use super::GsiTransition;
use crate::{CatalogAttempt, DescribeTimeToLiveAttempt, maintenance::MaintenancePort};

pub struct GsiScanSource<'a> {
    pub table: &'a BoundTableInfo,
    pub target: &'a GsiBuildTarget,
    pub after: Option<&'a Vec<u8>>,
    pub limit: GsiPageLimit,
}

pub trait GsiPort:
    CatalogAttempt
    + crate::TableUpdateTransaction
    + DescribeTimeToLiveAttempt
    + MaintenancePort
    + stream_provider::SystemHistoryReadAttempt
    + stream_provider::ItemHistoryReadAttempt
{
    fn scan_gsi_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> impl Future<Output = StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]>> + Send;
    fn load_gsi_candidate(
        &mut self,
        build: &GsiBuild<Vec<u8>>,
    ) -> impl Future<Output = StorageResult<Option<GsiBuild<Vec<u8>>>>> + Send;
    fn load_gsi_scan_source_page(
        &mut self,
        source: GsiScanSource<'_>,
    ) -> impl Future<Output = StorageResult<GsiScanSourcePage<Vec<u8>>>> + Send;
    fn load_gsi_projection_states(
        &mut self,
        table: &BoundTableInfo,
        batch: GsiProjectionStateReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>>> + Send;
    fn load_gsi_frontier(
        &mut self,
        current: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
    ) -> impl Future<Output = StorageResult<()>> + Send;
    fn load_gsi_activation_state(
        &mut self,
        table: &BoundTableInfo,
        target: &GsiBuildTarget,
    ) -> impl Future<Output = StorageResult<GsiActivationState>> + Send;
    fn store_gsi_transition(
        &mut self,
        table: &BoundTableInfo,
        transition: GsiTransition<'_, Vec<u8>>,
        fence: &Self::ProgressFence,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
