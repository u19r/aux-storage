mod build;
mod errors;
mod lifecycle;
mod models;
mod prepared_put;
mod projection;
mod referenced_history;
mod status;
mod stream;

pub use errors::{GsiLifecycleError, GsiRecordError};
pub use models::{
    GsiActivationObservation, GsiActivationState, GsiActive, GsiBlocked, GsiBuild,
    GsiBuildCandidates, GsiBuildDiscovery, GsiBuildProjectionObservation, GsiBuildTarget,
    GsiCatchingUp, GsiChange, GsiFailureReason, GsiIndexWriteActions, GsiKeyPart, GsiKeyParts,
    GsiObservation, GsiPageLimit, GsiProjectedRow, GsiProjection, GsiProjectionChange,
    GsiProjectionKey, GsiProjectionLifecycle, GsiProjectionStateRead, GsiProjectionStateReadBatch,
    GsiProjectionWrite, GsiProjectionWriteBatch, GsiReadRequest, GsiRecord,
    GsiReferencedHistoryItem, GsiScanObservation, GsiScanPageEnd, GsiScanSourcePage,
    GsiScannedSourceItem, GsiScanning, GsiSourceProjection, GsiState, GsiStateRecord,
    GsiStreamCandidate, GsiStreamObservation, GsiStreamPageEnd, GsiStreamSourcePage,
    GsiWriteAction, GsiWriteRequest,
};
pub use prepared_put::PreparedGsiPut;
pub use referenced_history::{GsiReferencedHistory, GsiReferencedHistoryRead};
pub use status::{GsiBuildPhase, GsiBuildStatus};
pub use stream::GsiStreamPageAssembly;

#[cfg(test)]
mod status_tests;

#[cfg(test)]
mod stream_tests;
