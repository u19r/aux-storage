//! Protected infrastructure failures within the shared storage error model.

use thiserror::Error;

/// The failed infrastructure concern, shared across providers and their
/// callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfrastructureContext {
    RecordAddress,
    RecordEncoding,
    Revision,
    Bounds,
    NativeCapacity,
    RecordBatch,
    RecordPage,
    PhysicalLayout,
    NativeTarget,
    WriterAuthority,
    AttemptSettlement,
    PublicationConfiguration,
    ResourcePolicyBinding,
    CommittedApplication,
    PublicationBinding,
    PublicationHistory,
    Observation,
    RegionalSchema,
    RegionalSource,
}

#[derive(Debug, Error, Clone)]
pub enum InfrastructureResultKind {
    #[error("Required protected state is unavailable ({context:?}).")]
    Unavailable { context: InfrastructureContext },
    #[error("Invalid protected record encoding ({context:?}).")]
    Encoding { context: InfrastructureContext },
    #[error("Unsupported protected format version ({context:?}).")]
    UnsupportedVersion { context: InfrastructureContext },
    #[error("Invalid physical bound ({context:?}).")]
    InvalidBound { context: InfrastructureContext },
}

/// Why a native attempt was known to abort without a verdict from the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeUnavailable {
    Deadline(AttemptPhase),
    ConnectionLost,
    WriterQueueFull,
    /// A WAL sync failed. The store refuses attempts until the process
    /// restarts, because a later sync does not prove the earlier data.
    DurabilityFailed,
    /// A native call of this attempt failed without a classified error or
    /// was abandoned (its future dropped before submission). A known abort.
    Abandoned,
}

/// The phase of a native attempt that a deadline interrupted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPhase {
    Acquire,
    Queued,
    Begin,
    Statement,
    Rollback,
    Settle,
}
