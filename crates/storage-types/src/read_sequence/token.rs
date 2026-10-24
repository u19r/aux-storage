use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ExclusiveStartKey, ReadSequenceConsistency, ReadSequenceNodeOperation, ReadSequencePlan,
    ReadSequenceRequest, ReadSequenceValidationError, StorageError,
    constants::{READ_SEQUENCE_TOKEN_MAX_BYTES, READ_SEQUENCE_TOKEN_VERSION},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSequenceToken {
    pub version: u8,
    pub request_digest: String,
    pub metadata_digest: String,
    pub consistency: ReadSequenceConsistency,
    pub next_node_ordinal: usize,
    /// A Query may resume at a later invocation without a key cursor when
    /// the prior invocation finished exactly at the page budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_ordinal: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_cursor: Option<ExclusiveStartKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_continuations: Option<Vec<ReadSequenceQueryContinuation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_pages: Option<Vec<ReadSequenceReplayPage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_continuation: Option<String>,
    pub completed_nodes: Vec<usize>,
    pub issued_at_epoch_seconds: i64,
    pub expires_at_epoch_seconds: i64,
    pub integrity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSequenceQueryContinuation {
    pub node_ordinal: usize,
    pub invocation_ordinal: u32,
    pub query_cursor: ExclusiveStartKey,
}

/// A pending ancestor Query page is replayed to rebuild descendant bindings.
/// Its next cursor is visited only after those descendants finish.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSequenceReplayPage {
    pub node_ordinal: usize,
    pub start_invocation_ordinal: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_cursor: Option<ExclusiveStartKey>,
    /// None means this was the ancestor's final page.
    pub next_invocation_ordinal: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<ExclusiveStartKey>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadSequenceTokenWithoutIntegrity<'token> {
    version: u8,
    request_digest: &'token str,
    metadata_digest: &'token str,
    consistency: &'token ReadSequenceConsistency,
    next_node_ordinal: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    invocation_ordinal: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_cursor: Option<&'token ExclusiveStartKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_continuations: Option<&'token [ReadSequenceQueryContinuation]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replay_pages: Option<&'token [ReadSequenceReplayPage]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_continuation: Option<&'token str>,
    completed_nodes: &'token [usize],
    issued_at_epoch_seconds: i64,
    expires_at_epoch_seconds: i64,
    integrity: &'static str,
}

impl ReadSequenceToken {
    pub fn new(
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
            replay_pages: None,
            provider_continuation: None,
            completed_nodes: Vec::new(),
            issued_at_epoch_seconds: read_sequence_token_timestamp(),
            expires_at_epoch_seconds: read_sequence_token_expiration(),
            integrity: String::new(),
        }
    }
}

pub fn prepare_resume_token(
    request: &mut ReadSequenceRequest,
    metadata_digest: &str,
) -> Result<(Option<ReadSequenceToken>, Option<String>), StorageError> {
    let Some(raw_token) = request.next_sequence_token.take() else {
        return Ok((None, None));
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
    Ok((Some(token), Some(request_digest)))
}

pub fn validate_resume_token_shape(
    token: &ReadSequenceToken,
    plan: &ReadSequencePlan,
) -> Result<(), StorageError> {
    if !has_valid_resume_frontier(token, plan)
        || !has_valid_query_continuations(token, plan)
        || !has_valid_replay_pages(token, plan)
        || !has_valid_resume_continuation_state(token, plan)
        || token.integrity != token_integrity(token)
    {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    Ok(())
}

pub fn encode_read_sequence_token(token: &ReadSequenceToken) -> Result<String, StorageError> {
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
    token.integrity = token_integrity(&token);
    let payload = serde_json::to_vec(&token).map_err(|error| {
        StorageError::internal(&format!("serialize ReadSequence continuation: {error}"))
    })?;
    if payload.len() > READ_SEQUENCE_TOKEN_MAX_BYTES {
        return Err(StorageError::internal(
            "ReadSequence continuation exceeds the token size limit",
        ));
    }
    Ok(encode_hex(&payload))
}

pub fn read_sequence_request_digest(request: &ReadSequenceRequest) -> Result<String, StorageError> {
    let mut digest_request = request.clone();
    digest_request.next_sequence_token = None;
    let payload = serde_json::to_vec(&digest_request).map_err(|error| {
        StorageError::internal(&format!("serialize ReadSequence request digest: {error}"))
    })?;
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string())
}

pub fn decode_read_sequence_token(raw: &str) -> Result<ReadSequenceToken, StorageError> {
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

#[doc(hidden)]
pub fn token_integrity(token: &ReadSequenceToken) -> String {
    // UUID-v5 gives us a deterministic canonical checksum for corruption and
    // stale-frontier detection. It is a protocol-integrity check, not a
    // caller identity check.
    let payload = serde_json::to_vec(&token_without_integrity(token)).unwrap_or_default();
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string()
}

#[doc(hidden)]
pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes.iter().copied() {
        encoded.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn token_without_integrity(token: &ReadSequenceToken) -> ReadSequenceTokenWithoutIntegrity<'_> {
    ReadSequenceTokenWithoutIntegrity {
        version: token.version,
        request_digest: &token.request_digest,
        metadata_digest: &token.metadata_digest,
        consistency: &token.consistency,
        next_node_ordinal: token.next_node_ordinal,
        invocation_ordinal: token.invocation_ordinal,
        query_cursor: token.query_cursor.as_ref(),
        query_continuations: token.query_continuations.as_deref(),
        replay_pages: token.replay_pages.as_deref(),
        provider_continuation: token.provider_continuation.as_deref(),
        completed_nodes: &token.completed_nodes,
        issued_at_epoch_seconds: token.issued_at_epoch_seconds,
        expires_at_epoch_seconds: token.expires_at_epoch_seconds,
        integrity: "",
    }
}

fn has_valid_resume_frontier(token: &ReadSequenceToken, plan: &ReadSequencePlan) -> bool {
    let Some(dependencies) = plan.graph.dependencies.get(token.next_node_ordinal) else {
        return false;
    };
    token.next_node_ordinal < plan.nodes.len()
        && has_valid_completed_nodes(token, plan)
        && dependencies.iter().all(|dependency| {
            token
                .completed_nodes
                .binary_search(&dependency.index())
                .is_ok()
        })
        && !token.completed_nodes.contains(&token.next_node_ordinal)
        && (token.query_cursor.is_none() || token.invocation_ordinal.is_some())
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
        && token.completed_nodes.iter().all(|node| {
            plan.graph.dependencies[*node].iter().all(|dependency| {
                token
                    .completed_nodes
                    .binary_search(&dependency.index())
                    .is_ok()
            })
        })
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
                && continuations[0].node_ordinal == token.next_node_ordinal
                && continuations.iter().all(|continuation| {
                    continuation.node_ordinal < plan.nodes.len()
                        && !token.completed_nodes.contains(&continuation.node_ordinal)
                        && matches!(
                            plan.nodes
                                .get(continuation.node_ordinal)
                                .map(|node| &node.operation),
                            Some(ReadSequenceNodeOperation::Query(_))
                        )
                        && plan.graph.dependencies[continuation.node_ordinal]
                            .iter()
                            .all(|dependency| {
                                token
                                    .completed_nodes
                                    .binary_search(&dependency.index())
                                    .is_ok()
                            })
                })
        })
}

fn has_valid_replay_pages(token: &ReadSequenceToken, plan: &ReadSequencePlan) -> bool {
    token.replay_pages.as_ref().is_none_or(|pages| {
        !pages.is_empty()
            && token.provider_continuation.is_none()
            && token.query_continuations.is_none()
            && pages.iter().all(|page| {
                token
                    .completed_nodes
                    .binary_search(&page.node_ordinal)
                    .is_ok()
                    && matches!(
                        plan.nodes
                            .get(page.node_ordinal)
                            .map(|node| &node.operation),
                        Some(ReadSequenceNodeOperation::Query(_))
                    )
                    && (page.next_invocation_ordinal.is_some() || page.next_cursor.is_none())
                    && page
                        .next_invocation_ordinal
                        .is_none_or(|next| next >= page.start_invocation_ordinal)
            })
            && pages
                .windows(2)
                .all(|pair| depends_on_node(plan, pair[1].node_ordinal, pair[0].node_ordinal))
            && pages.last().is_some_and(|page| {
                depends_on_node(plan, token.next_node_ordinal, page.node_ordinal)
            })
    })
}

fn depends_on_node(plan: &ReadSequencePlan, node: usize, ancestor: usize) -> bool {
    plan.graph.dependencies[node].iter().any(|dependency| {
        dependency.index() == ancestor || depends_on_node(plan, dependency.index(), ancestor)
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
        && (!has_invocation
            || has_query_continuations
            || has_provider_continuation
            || matches!(
                plan.nodes
                    .get(token.next_node_ordinal)
                    .map(|node| &node.operation),
                Some(ReadSequenceNodeOperation::Query(_))
            ))
}

fn read_sequence_token_expiration() -> i64 {
    (Utc::now() + Duration::minutes(15)).timestamp()
}

fn read_sequence_token_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs() as i64)
}

fn decode_hex(value: &str) -> Result<Vec<u8>, StorageError> {
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

fn hex_digit(value: u8) -> Result<u8, StorageError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(read_sequence_error(ReadSequenceValidationError::StaleToken)),
    }
}

fn read_sequence_error(error: ReadSequenceValidationError) -> StorageError {
    StorageError::from(error)
}
