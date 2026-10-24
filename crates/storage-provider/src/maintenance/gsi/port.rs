//! Physical GSI pages and conditional writes inside the caller's attempt.

use std::future::Future;

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildTarget, GsiPageLimit, GsiProjection, GsiProjectionKey,
    GsiProjectionStateReadBatch, GsiReferencedHistoryItem, GsiReferencedHistoryRead,
    GsiScanSourcePage, GsiStreamSourcePage,
};
use storage_types::{
    BoundTableInfo, StorageResult, SystemStreamCheckpoint, SystemStreamPosition, SystemStreamScope,
};

use super::GsiTransition;
use crate::{CatalogAttempt, DescribeTimeToLiveAttempt, maintenance::MaintenancePort};

pub struct GsiScanSource<'a> {
    pub table: &'a BoundTableInfo,
    pub target: &'a GsiBuildTarget,
    pub after: Option<&'a Vec<u8>>,
    pub limit: GsiPageLimit,
}

pub struct GsiStreamSource<'a> {
    pub table: &'a BoundTableInfo,
    pub target: &'a GsiBuildTarget,
    pub after: &'a SystemStreamCheckpoint,
    pub through: &'a SystemStreamCheckpoint,
    pub limit: GsiPageLimit,
}

pub struct GsiCandidateObservation {
    pub build: Option<GsiBuild<Vec<u8>>>,
    pub holds: Vec<(SystemStreamScope, SystemStreamPosition)>,
}

pub trait GsiPort:
    CatalogAttempt
    + DescribeTimeToLiveAttempt
    + MaintenancePort
    + infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn scan_gsi_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> impl Future<Output = StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]>> + Send;
    fn load_gsi_candidate_holds(
        &mut self,
        build: &GsiBuild<Vec<u8>>,
    ) -> impl Future<Output = StorageResult<GsiCandidateObservation>> + Send;
    fn load_gsi_scan_source_page(
        &mut self,
        source: GsiScanSource<'_>,
    ) -> impl Future<Output = StorageResult<GsiScanSourcePage<Vec<u8>>>> + Send;
    fn load_gsi_stream_source_page(
        &mut self,
        source: GsiStreamSource<'_>,
    ) -> impl Future<Output = StorageResult<GsiStreamSourcePage>> + Send;
    fn load_gsi_stream_tail(
        &mut self,
    ) -> impl Future<Output = StorageResult<SystemStreamCheckpoint>> + Send;
    fn load_gsi_projection_states(
        &mut self,
        table: &BoundTableInfo,
        batch: GsiProjectionStateReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>>> + Send;
    fn load_gsi_referenced_histories(
        &mut self,
        reads: &[GsiReferencedHistoryRead<'_>],
    ) -> impl Future<Output = StorageResult<Vec<GsiReferencedHistoryItem>>> + Send;
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
