use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{Duration, Utc};
use http_error::HttpApiError;
use serde::{Deserialize, Serialize};
use storage_types::{
    ExclusiveStartKey, ReadSequenceConsistency, ReadSequenceNodeOperation, ReadSequencePlan,
    ReadSequenceRequest, ReadSequenceValidationError, StorageError,
};
use uuid::Uuid;

pub(super) const READ_SEQUENCE_TOKEN_VERSION: u8 = 2;
const READ_SEQUENCE_TOKEN_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReadSequenceToken {
    pub(super) version: u8,
    pub(super) request_digest: String,
    pub(super) metadata_digest: String,
    pub(super) consistency: ReadSequenceConsistency,
    pub(super) next_node_ordinal: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) invocation_ordinal: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) query_cursor: Option<ExclusiveStartKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) query_continuations: Option<Vec<ReadSequenceQueryContinuation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) provider_continuation: Option<String>,
    pub(super) completed_nodes: Vec<usize>,
    pub(super) issued_at_epoch_seconds: i64,
    pub(super) expires_at_epoch_seconds: i64,
    pub(super) integrity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReadSequenceQueryContinuation {
    pub(super) node_ordinal: usize,
    pub(super) invocation_ordinal: u32,
    pub(super) query_cursor: ExclusiveStartKey,
}

impl ReadSequenceToken {
    pub(super) fn new(
        request_digest: &str,
        metadata_digest: &str,
        consistency: ReadSequenceConsistency,
    ) -> Self {
        Self {
            version: READ_SEQUENCE_TOKEN_VERSION,
            request_digest: request_digest.to_owned(),
            metadata_digest: metadata_digest.to_owned(),
            consistency,
            next_node_ordinal: 0,
            invocation_ordinal: None,
            query_cursor: None,
            query_continuations: None,
            provider_continuation: None,
            completed_nodes: Vec::new(),
            issued_at_epoch_seconds: read_sequence_token_timestamp(),
            expires_at_epoch_seconds: read_sequence_token_expiration(),
            integrity: String::new(),
        }
    }
}

pub(super) fn prepare_resume_token(
    request: &mut ReadSequenceRequest,
    metadata_digest: &str,
) -> Result<Option<ReadSequenceToken>, HttpApiError> {
    let Some(raw_token) = request.next_sequence_token.take() else {
        return Ok(None);
    };
    let token = decode_read_sequence_token(&raw_token)?;
    if token.version != READ_SEQUENCE_TOKEN_VERSION
        || token.expires_at_epoch_seconds <= Utc::now().timestamp()
        || token.consistency != request.read_consistency
    {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    let request_digest = read_sequence_request_digest(request)?;
    if token.request_digest != request_digest || token.metadata_digest != metadata_digest {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    Ok(Some(token))
}

pub(super) fn validate_resume_token_shape(
    token: &ReadSequenceToken,
    plan: &ReadSequencePlan,
) -> Result<(), HttpApiError> {
    if !has_valid_resume_frontier(token, plan)
        || !has_valid_query_continuations(token, plan)
        || !has_valid_resume_continuation_state(token, plan)
        || token.integrity != token_integrity(token)
    {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    Ok(())
}

fn has_valid_resume_frontier(token: &ReadSequenceToken, plan: &ReadSequencePlan) -> bool {
    token.next_node_ordinal < plan.nodes.len()
        && has_valid_completed_nodes(token, plan)
        && !token.completed_nodes.contains(&token.next_node_ordinal)
        && token.query_cursor.is_some() == token.invocation_ordinal.is_some()
}

fn has_valid_completed_nodes(token: &ReadSequenceToken, plan: &ReadSequencePlan) -> bool {
    token
        .completed_nodes
        .iter()
        .all(|node| *node < plan.nodes.len())
        && token
            .completed_nodes
            .windows(2)
            .all(|nodes| nodes[0] < nodes[1])
}

fn has_valid_query_continuations(token: &ReadSequenceToken, plan: &ReadSequencePlan) -> bool {
    token
        .query_continuations
        .as_ref()
        .is_none_or(|continuations| {
            !continuations.is_empty()
                && continuations
                    .windows(2)
                    .all(|pair| pair[0].node_ordinal < pair[1].node_ordinal)
                && continuations.iter().all(|continuation| {
                    continuation.node_ordinal < plan.nodes.len()
                        && matches!(
                            plan.nodes
                                .get(continuation.node_ordinal)
                                .map(|node| &node.operation),
                            Some(ReadSequenceNodeOperation::Query(_))
                        )
                })
        })
}

fn has_valid_resume_continuation_state(token: &ReadSequenceToken, plan: &ReadSequencePlan) -> bool {
    let has_provider_continuation = token.provider_continuation.is_some();
    let has_query_cursor = token.query_cursor.is_some();
    let has_invocation = token.invocation_ordinal.is_some();
    let has_query_continuations = token.query_continuations.is_some();

    (!has_query_continuations || !has_query_cursor && !has_invocation)
        && (!has_provider_continuation
            || !has_query_cursor
                && !has_invocation
                && !has_query_continuations
                && token.completed_nodes.is_empty())
        && (!has_query_cursor
            || has_query_continuations
            || has_provider_continuation
            || matches!(
                plan.nodes
                    .get(token.next_node_ordinal)
                    .map(|node| &node.operation),
                Some(ReadSequenceNodeOperation::Query(_))
            ))
}

pub(super) fn encode_read_sequence_token(
    token: &ReadSequenceToken,
) -> Result<String, HttpApiError> {
    let mut token = token.clone();
    // Completion is collected from concurrent waves.  Canonicalize every
    // repeated field before computing the checksum so an equivalent frontier
    // has one token byte representation regardless of future completion order.
    token.completed_nodes.sort_unstable();
    if let Some(continuations) = token.query_continuations.as_mut() {
        continuations.sort_by_key(|continuation| {
            (continuation.node_ordinal, continuation.invocation_ordinal)
        });
    }
    token.integrity.clear();
    token.integrity = token_integrity(&token);
    let payload = serde_json::to_vec(&token).map_err(|error| {
        HttpApiError::from(StorageError::internal(&format!(
            "serialize ReadSequence continuation: {error}"
        )))
    })?;
    if payload.len() > READ_SEQUENCE_TOKEN_MAX_BYTES {
        return Err(HttpApiError::from(StorageError::internal(
            "ReadSequence continuation exceeds the token size limit",
        )));
    }
    Ok(encode_hex(&payload))
}

pub(super) fn read_sequence_request_digest(
    request: &ReadSequenceRequest,
) -> Result<String, HttpApiError> {
    let mut digest_request = request.clone();
    digest_request.next_sequence_token = None;
    let payload = storage_types::canonical_json::to_vec(&digest_request).map_err(|error| {
        HttpApiError::from(StorageError::internal(&format!(
            "serialize ReadSequence request digest: {error}"
        )))
    })?;
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string())
}

fn read_sequence_token_expiration() -> i64 {
    (Utc::now() + Duration::minutes(15)).timestamp()
}

fn read_sequence_token_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs() as i64)
}

pub(super) fn decode_read_sequence_token(raw: &str) -> Result<ReadSequenceToken, HttpApiError> {
    if raw.len() > READ_SEQUENCE_TOKEN_MAX_BYTES * 2 {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    let payload = decode_hex(raw)?;
    let token: ReadSequenceToken = serde_json::from_slice(&payload)
        .map_err(|_| read_sequence_error(ReadSequenceValidationError::StaleToken))?;
    if token.integrity != token_integrity(&token) {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    Ok(token)
}

fn token_integrity(token: &ReadSequenceToken) -> String {
    // UUID-v5 gives us a deterministic canonical checksum for corruption and
    // stale-frontier detection.  It is intentionally not described as a
    // signature: without a server-held secret it cannot authenticate a token.
    let mut unsigned = token.clone();
    unsigned.integrity.clear();
    let payload = storage_types::canonical_json::to_vec(&unsigned).unwrap_or_default();
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string()
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn decode_hex(value: &str) -> Result<Vec<u8>, HttpApiError> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    (0..bytes.len())
        .step_by(2)
        .map(|index| {
            let high = hex_digit(bytes[index])?;
            let low = hex_digit(bytes[index + 1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8, HttpApiError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(read_sequence_error(ReadSequenceValidationError::StaleToken)),
    }
}

fn read_sequence_error(error: ReadSequenceValidationError) -> HttpApiError {
    HttpApiError::from(StorageError::from(error))
}
