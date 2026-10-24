//! Internal GSI backfill coordination for aux-storage backends.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod constants;
mod errors;
mod gsi;
mod scan_tail;
mod session_protection;

pub use gsi::{
    GsiActivationObservation, GsiActivationState, GsiActive, GsiBlocked, GsiBuild,
    GsiBuildCandidates, GsiBuildDiscovery, GsiBuildPhase, GsiBuildProjectionObservation,
    GsiBuildStatus, GsiBuildTarget, GsiChange, GsiFailureReason, GsiKeyPart, GsiLifecycleError,
    GsiObservation, GsiPageLimit, GsiProjectedRow, GsiProjection, GsiProjectionKey,
    GsiProjectionLifecycle, GsiProjectionStateRead, GsiProjectionStateReadBatch,
    GsiProjectionWrite, GsiProjectionWriteBatch, GsiReadRequest, GsiRecord, GsiReferencedHistory,
    GsiReferencedHistoryItem, GsiReferencedHistoryRead, GsiScanObservation, GsiScanPageEnd,
    GsiScanSourcePage, GsiScannedSourceItem, GsiSourceProjection, GsiState, GsiStateRecord,
    GsiStreamCandidate, GsiStreamObservation, GsiStreamPageAssembly, GsiStreamPageEnd,
    GsiStreamSourcePage, GsiWriteAction, GsiWriteRequest, PreparedGsiPut,
};
pub use session_protection::merge_protected_backfill_cursor;

#[cfg(test)]
mod gsi_history_tests;
#[cfg(test)]
mod gsi_lifecycle_tests;
#[cfg(test)]
mod session_protection_tests;

pub use scan_tail::{
    ScanPageEnd, ScanProgress, ScanTailError, ScanTransition, TailPageEnd, TailProgress,
};
