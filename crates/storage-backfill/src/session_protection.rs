use serde::Deserialize;
use serde_json::value::RawValue;
use storage_types::StreamItemId;

use crate::{
    constants::{BOOTSTRAP_SESSION_PREFIX, CATCHUP_SESSION_PREFIX, PEER_SESSION_PREFIX},
    errors::ActiveBackfillSessionError,
};

#[must_use]
fn is_active_backfill_session_key(key: &str) -> bool {
    key.starts_with(PEER_SESSION_PREFIX)
        || key.starts_with(BOOTSTRAP_SESSION_PREFIX)
        || key.starts_with(CATCHUP_SESSION_PREFIX)
}

pub fn merge_protected_backfill_cursor(
    current_floor: Option<StreamItemId>,
    session_key: &str,
    payload_json: &str,
) -> Result<Option<StreamItemId>, ActiveBackfillSessionError> {
    if !is_active_backfill_session_key(session_key) {
        return Ok(current_floor);
    }
    let protected_system_stream_cursor = parse_protected_cursor(session_key, payload_json)?;

    Ok(Some(match current_floor {
        Some(existing) => existing.min(protected_system_stream_cursor),
        None => protected_system_stream_cursor,
    }))
}

#[derive(Deserialize)]
struct ActiveBackfillSessionPayload<'a> {
    #[serde(borrow, rename = "last_system_stream_cursor")]
    last_system_stream_cursor: Option<&'a RawValue>,
    #[serde(borrow, rename = "protected_stream_cursor")]
    protected_stream_cursor: Option<&'a RawValue>,
}

fn parse_protected_cursor(
    session_key: &str,
    payload_json: &str,
) -> Result<StreamItemId, ActiveBackfillSessionError> {
    let payload: ActiveBackfillSessionPayload<'_> =
        serde_json::from_str(payload_json).map_err(|source| {
            ActiveBackfillSessionError::InvalidPayloadJson {
                session_key: session_key.to_owned(),
                reason: source.to_string(),
            }
        })?;
    let cursor = payload
        .last_system_stream_cursor
        .or(payload.protected_stream_cursor)
        .ok_or_else(|| ActiveBackfillSessionError::MissingProtectedCursor {
            session_key: session_key.to_owned(),
        })?;
    serde_json::from_str(cursor.get()).map_err(|source| {
        ActiveBackfillSessionError::InvalidProtectedCursor {
            session_key: session_key.to_owned(),
            reason: source.to_string(),
        }
    })
}
