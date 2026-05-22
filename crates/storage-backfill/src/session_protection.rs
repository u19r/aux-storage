use storage_types::StreamItemId;
use thiserror::Error;

const PEER_SESSION_PREFIX: &str = "peer#";
const BOOTSTRAP_SESSION_PREFIX: &str = "bootstrap#";
const CATCHUP_SESSION_PREFIX: &str = "catchup#";
const LAST_SYSTEM_STREAM_CURSOR: &str = "last_system_stream_cursor";
const PROTECTED_STREAM_CURSOR: &str = "protected_stream_cursor";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveBackfillSession {
    pub session_key: String,
    pub protected_system_stream_cursor: StreamItemId,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
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

#[must_use]
pub fn is_active_backfill_session_key(key: &str) -> bool {
    key.starts_with(PEER_SESSION_PREFIX)
        || key.starts_with(BOOTSTRAP_SESSION_PREFIX)
        || key.starts_with(CATCHUP_SESSION_PREFIX)
}

pub fn parse_active_backfill_session(
    session_key: &str,
    payload_json: &str,
) -> Result<Option<ActiveBackfillSession>, ActiveBackfillSessionError> {
    if !is_active_backfill_session_key(session_key) {
        return Ok(None);
    }

    let payload: serde_json::Value = serde_json::from_str(payload_json).map_err(|source| {
        ActiveBackfillSessionError::InvalidPayloadJson {
            session_key: session_key.to_string(),
            reason: source.to_string(),
        }
    })?;
    let cursor_value = payload
        .get(LAST_SYSTEM_STREAM_CURSOR)
        .or_else(|| payload.get(PROTECTED_STREAM_CURSOR))
        .ok_or_else(|| ActiveBackfillSessionError::MissingProtectedCursor {
            session_key: session_key.to_string(),
        })?;
    let protected_system_stream_cursor =
        serde_json::from_value(cursor_value.clone()).map_err(|source| {
            ActiveBackfillSessionError::InvalidProtectedCursor {
                session_key: session_key.to_string(),
                reason: source.to_string(),
            }
        })?;

    Ok(Some(ActiveBackfillSession {
        session_key: session_key.to_string(),
        protected_system_stream_cursor,
    }))
}

pub fn merge_protected_backfill_cursor(
    current_floor: Option<StreamItemId>,
    session_key: &str,
    payload_json: &str,
) -> Result<Option<StreamItemId>, ActiveBackfillSessionError> {
    let Some(session) = parse_active_backfill_session(session_key, payload_json)? else {
        return Ok(current_floor);
    };

    Ok(Some(match current_floor {
        Some(existing) => existing.min(session.protected_system_stream_cursor),
        None => session.protected_system_stream_cursor,
    }))
}
