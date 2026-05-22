//! Internal GSI backfill coordination for aux-storage backends.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod catchup_state;
mod constants;
mod gsi_catchup;
mod logical;
mod policy;
mod session_protection;
mod state;
mod traits;
mod worker;

pub use catchup_state::{
    CatchupApplyAdapter, CatchupSessionState, CatchupState, CatchupStateError, CleanupState,
    CompletionState, ProtectedBoundaryState, ProtectedStreamBoundary, ScanState,
    StreamDrainCheckpoint, StreamDrainState,
};
pub use constants::{
    BACKFILL_BATCH_SIZE, BACKFILL_BATCH_SLEEP_MS, BACKFILL_LOCK_TTL_MS,
    MAX_CONCURRENT_GSI_BACKFILLS,
};
pub use gsi_catchup::{GsiCatchupApplyCase, GsiCatchupOutcome, plan_gsi_catchup_apply};
pub use logical::{
    LogicalBackfillActivationGate, LogicalBackfillCaller, LogicalBackfillCheckpoint,
    LogicalBackfillChecksum, LogicalBackfillChunk, LogicalBackfillChunkId,
    LogicalBackfillChunkSummary, LogicalBackfillCommand, LogicalBackfillConflictPolicy,
    LogicalBackfillDomain, LogicalBackfillError, LogicalBackfillExport, LogicalBackfillId,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalBackfillPolicy, LogicalBackfillRecord,
    LogicalBackfillResult, LogicalBackfillTombstone, LogicalBackfillTombstoneCleanup,
    LogicalBootstrapPreflightCase, LogicalBootstrapPreflightDecision, LogicalExportPage,
    LogicalExportRequest, LogicalImportApplyCase, LogicalImportApplyDecision,
    LogicalImportRecordKind, MultiRegionBootstrapPolicy, SyncLearnerCatchupPolicy,
    plan_logical_bootstrap_preflight, plan_logical_import_apply,
    validate_logical_chunk_for_manifest,
};
pub use policy::{
    BackfillControl, BackfillPolicy, GsiBackfillPolicy, GsiKeyMapping, GsiProjection,
    GsiScanObservation, GsiStreamRecord, GsiTombstoneEvidence,
};
pub use session_protection::{
    ActiveBackfillSession, ActiveBackfillSessionError, is_active_backfill_session_key,
    merge_protected_backfill_cursor, parse_active_backfill_session,
};
pub use state::{
    BackfillBatchOutcome, BackfillError, BackfillLock, BackfillResult, BackfillResultType,
    BackfillState, BackfillStatus, GsiBackfillDescriptor, WorkerContext,
};
pub use traits::BackfillDriver;
pub use worker::{BackfillConfig, BackfillCoordinator};

#[cfg(test)]
mod catchup_state_tests;
#[cfg(test)]
mod gsi_catchup_tests;
#[cfg(test)]
mod logical_tests;
#[cfg(test)]
mod policy_tests;
#[cfg(test)]
mod quint_gsi_backfill_catchup_tests;
#[cfg(test)]
mod quint_logical_backfill_tests;
#[cfg(test)]
mod session_protection_tests;
#[cfg(test)]
mod worker_tests;
