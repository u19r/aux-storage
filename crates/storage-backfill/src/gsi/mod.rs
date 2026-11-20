mod build;
mod errors;
mod lifecycle;
mod models;
mod prepared_put;
mod projection;
mod status;

pub use errors::{GsiLifecycleError, GsiRecordError};
pub use models::{
    GsiActivationState, GsiActive, GsiBuild, GsiBuildCandidates, GsiBuildDiscovery, GsiBuildTarget,
    GsiChange, GsiIndexWriteActions, GsiKeyPart, GsiKeyParts, GsiNativeBuilding, GsiPageLimit,
    GsiProjectedRow, GsiProjectionKey, GsiProjectionLifecycle, GsiProjectionWrite,
    GsiProjectionWriteBatch, GsiRecord, GsiScanObservation, GsiScanPageEnd, GsiScanSourcePage,
    GsiScannedSourceItem, GsiScanning, GsiState, GsiStateRecord, GsiWriteAction, GsiWriteRequest,
};
pub use prepared_put::PreparedGsiPut;
pub use status::{GsiBuildPhase, GsiBuildStatus};

#[cfg(test)]
mod status_tests;
