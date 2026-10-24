use thiserror::Error;

#[derive(Debug, Error)]
pub enum ActiveBackfillSessionError {
    #[error("active backfill session {session_key} payload is invalid JSON: {reason}")]
    InvalidPayloadJson { session_key: String, reason: String },
    #[error("active backfill session {session_key} is missing a protected stream cursor")]
    MissingProtectedCursor { session_key: String },
    #[error(
        "active backfill session {session_key} has an invalid protected stream cursor: {reason}"
    )]
    InvalidProtectedCursor { session_key: String, reason: String },
}
