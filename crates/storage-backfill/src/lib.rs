//! Internal GSI backfill coordination for aux-storage backends.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod constants;
mod gsi;
mod scan_tail;

pub use gsi::{
    GsiActivationObservation, GsiActivationState, GsiActive, GsiBlocked, GsiBuild,
    GsiBuildCandidates, GsiBuildDiscovery, GsiBuildPhase, GsiBuildProjectionObservation,
    GsiBuildStatus, GsiBuildTarget, GsiChange, GsiFailureReason, GsiKeyPart, GsiLifecycleError,
    GsiObservation, GsiPageLimit, GsiProjectedRow, GsiProjection, GsiProjectionKey,
    GsiProjectionLifecycle, GsiProjectionStateRead, GsiProjectionStateReadBatch,
    GsiProjectionWrite, GsiProjectionWriteBatch, GsiReadRequest, GsiRecord, GsiScanObservation,
    GsiScanPageEnd, GsiScanSourcePage, GsiScannedSourceItem, GsiSourceProjection, GsiState,
    GsiStateRecord, GsiStreamCandidate, GsiStreamObservation, GsiStreamPageAssembly,
    GsiStreamPageEnd, GsiStreamSourcePage, GsiWriteAction, GsiWriteRequest, PreparedGsiPut,
};

#[cfg(test)]
mod gsi_history_tests;
#[cfg(test)]
mod gsi_lifecycle_tests;

pub use scan_tail::{
    ScanPageEnd, ScanProgress, ScanTailError, ScanTransition, TailPageEnd, TailProgress,
};
