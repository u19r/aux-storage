//! Physical GSI pages and conditional writes inside the caller's attempt.

use std::future::Future;

use storage_backfill::{GsiBuild, GsiBuildTarget, GsiPageLimit, GsiScanSourcePage};
use storage_types::{BoundTableInfo, StorageResult};

use super::GsiTransition;
use crate::{CatalogAttempt, TtlPolicyAttempt, maintenance::MaintenancePort};

pub struct GsiScanSource<'a> {
    pub table: &'a BoundTableInfo,
    pub target: &'a GsiBuildTarget,
    pub after: Option<&'a Vec<u8>>,
    pub limit: GsiPageLimit,
}

pub trait GsiPort:
    CatalogAttempt + crate::TableUpdateTransaction + TtlPolicyAttempt + MaintenancePort
{
    const NATIVE_INDEX: bool = false;
    /// Queue a native schema effect. The store executes it only after commit.
    fn stage_native_index(&mut self, _work: super::NativeIndexWork) -> StorageResult<()> {
        Err(storage_types::StorageError::internal(
            "Native index DDL is unsupported",
        ))
    }
    fn load_native_gsi_page(
        &mut self,
        _table: &BoundTableInfo,
        _target: &GsiBuildTarget,
        _after: Option<&Vec<u8>>,
        _limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<super::NativeGsiPage>> + Send {
        async {
            Err(storage_types::StorageError::internal(
                "Native index fill is unsupported",
            ))
        }
    }
    fn store_native_gsi_fills(
        &mut self,
        _table: &BoundTableInfo,
        _target: &GsiBuildTarget,
        _fills: Vec<super::NativeGsiFill>,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send {
        async {
            Err(storage_types::StorageError::internal(
                "Native index fill is unsupported",
            ))
        }
    }
    fn load_native_index_state(
        &mut self,
        _table: &BoundTableInfo,
        _index: &storage_types::BoundGsi,
    ) -> impl Future<Output = StorageResult<super::NativeIndexState>> + Send {
        async {
            Err(storage_types::StorageError::internal(
                "Native index state is unsupported",
            ))
        }
    }
    fn store_native_gsi_transition(
        &mut self,
        _table: &BoundTableInfo,
        _transition: super::NativeGsiTransition,
        _progress: crate::maintenance::ProgressWrite,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async {
            Err(storage_types::StorageError::internal(
                "Native index transition is unsupported",
            ))
        }
    }
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

    /// `progress` is the checked progress write of this transition; the port
    /// stores it in the same native write wave as the transition.
    /// `transition.expected_revision` and `transition.next_progress` are
    /// already encoded in `progress`; ports ignore them.
    fn store_gsi_transition(
        &mut self,
        table: &BoundTableInfo,
        transition: GsiTransition<'_, Vec<u8>>,
        progress: crate::maintenance::ProgressWrite,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
