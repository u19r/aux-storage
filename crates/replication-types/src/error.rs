use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReplicationProtocolError {
    #[error("canonical output allocation rejected")]
    CapacityRejected,
    #[error("invalid identity: {context}")]
    InvalidIdentity { context: &'static str },
    #[error("invalid canonical encoding: {context}")]
    InvalidEncoding { context: &'static str },
    #[error("invalid bounded range: {context}")]
    InvalidRange { context: &'static str },
    #[error("counter overflow: {context}")]
    CounterOverflow { context: &'static str },
    #[error("canonical identity or content mismatch: {context}")]
    IdentityContentMismatch { context: &'static str },
    #[error("invalid origin: {context}")]
    InvalidOrigin { context: &'static str },
    #[error("table lineage mismatch: {context}")]
    LineageMismatch { context: &'static str },
    #[error("source coverage gap: {context}")]
    SourceGap { context: &'static str },
    #[error("incompatible protocol or semantics: {context}")]
    ProtocolIncompatible { context: &'static str },
    #[error("{context}: payload is {actual} bytes, limit is {limit} bytes")]
    PayloadTooLarge {
        context: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("invalid transfer scope: {context}")]
    InvalidScope { context: &'static str },
    #[error("protected revision mismatch: {context}")]
    RevisionMismatch { context: &'static str },
    #[error("certificate does not cover required participants: {context}")]
    IncompleteCertificate { context: &'static str },
    #[error("invalid transition: {context}")]
    InvalidTransition { context: &'static str },
    #[error("invalid clock evidence: {context}")]
    InvalidClock { context: &'static str },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum PeerError {
    #[error("invalid origin: {context}")]
    InvalidOrigin { context: &'static str },
    #[error("lineage mismatch: {context}")]
    LineageMismatch { context: &'static str },
    #[error("identity/content mismatch: {context}")]
    IdentityContentMismatch { context: &'static str },
    #[error("source gap: {context}")]
    SourceGap { context: &'static str },
    #[error("schema unavailable: {context}")]
    SchemaUnavailable { context: &'static str },
    #[error("history unavailable: {context}")]
    HistoryUnavailable { context: &'static str },
    #[error("history retired: {context}")]
    RetiredHistory { context: &'static str },
    #[error("protocol incompatible: {context}")]
    ProtocolIncompatible { context: &'static str },
    #[error("stored state corrupt: {context}")]
    StoredStateCorrupt { context: &'static str },
    #[error("capacity unavailable: {context}")]
    CapacityUnavailable { context: &'static str },
    #[error("commit outcome unknown: {context}")]
    UnknownCommit { context: &'static str },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ControlError {
    #[error("revision mismatch: {context}")]
    RevisionMismatch { context: &'static str },
    #[error("conflicting operation: {context}")]
    Conflict { context: &'static str },
    #[error("required participant unavailable: {context}")]
    ParticipantUnavailable { context: &'static str },
    #[error("invalid transition: {context}")]
    InvalidTransition { context: &'static str },
    #[error("invalid certificate: {context}")]
    InvalidCertificate { context: &'static str },
    #[error("commit outcome unknown: {context}")]
    UnknownCommit { context: &'static str },
}
