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
    #[error("Required protected state is unavailable.")]
    Unavailable { context: InfrastructureContext },
    #[error("Invalid protected record encoding.")]
    Encoding { context: InfrastructureContext },
    #[error("Unsupported protected format version.")]
    UnsupportedVersion { context: InfrastructureContext },
    #[error("Invalid physical bound.")]
    InvalidBound { context: InfrastructureContext },
    /// The native owner proved rejection before commit. This is not an
    /// ambiguous outcome or permission to retry the same oversized attempt.
    #[error("Native attempt exceeded capacity before commit.")]
    NativeCapacityRejected,
    #[error("Protected revision conflict.")]
    ProtectedConflict { context: InfrastructureContext },
}
