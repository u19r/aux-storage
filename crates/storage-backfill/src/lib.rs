//! Internal GSI backfill coordination for aux-storage backends.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod gsi;
mod scan_tail;

pub use gsi::{
    GsiActivationState, GsiActive, GsiBuild, GsiBuildCandidates, GsiBuildDiscovery, GsiBuildPhase,
    GsiBuildStatus, GsiBuildTarget, GsiChange, GsiIndexWriteActions, GsiKeyPart, GsiKeyParts,
    GsiLifecycleError, GsiNativeBuilding, GsiPageLimit, GsiProjectedRow, GsiProjectionKey,
    GsiProjectionLifecycle, GsiProjectionWrite, GsiProjectionWriteBatch, GsiRecord, GsiRecordError,
    GsiScanObservation, GsiScanPageEnd, GsiScanSourcePage, GsiScannedSourceItem, GsiScanning,
    GsiState, GsiStateRecord, GsiWriteAction, GsiWriteRequest, PreparedGsiPut,
};

#[cfg(test)]
mod gsi_lifecycle_tests;

pub use scan_tail::{
    ScanPageEnd, ScanProgress, ScanTailError, ScanTransition, TailPageEnd, TailProgress,
};
