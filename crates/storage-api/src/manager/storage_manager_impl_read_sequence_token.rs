use chrono::{Duration, Utc};
use http_error::HttpApiError;
use serde::{Deserialize, Serialize};
use storage_types::{
    ExclusiveStartKey, KeyAttributes, ReadSequenceRequest, ReadSequenceStep,
    ReadSequenceValidationError, StorageError,
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReadSequenceToken {
    pub version: u8,
    pub request_digest: String,
    pub metadata_digest: String,
    pub step_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_index: Option<usize>,
    pub expires_at_epoch_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_key: Option<KeyAttributes>,
}

pub(super) fn prepare_resume_token(
    request: &mut ReadSequenceRequest,
) -> Result<Option<ReadSequenceToken>, HttpApiError> {
    let Some(raw_token) = request.next_sequence_token.take() else {
        return Ok(None);
    };
    let token = decode_read_sequence_token(&raw_token)?;
    if token.version != 1 {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    if token.expires_at_epoch_seconds <= Utc::now().timestamp() {
        return Err(read_sequence_error(
            ReadSequenceValidationError::SnapshotExpired,
        ));
    }
    let request_digest = read_sequence_request_digest(request)?;
    if token.request_digest != request_digest {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    let Some(step) = request.sequence.get_mut(token.step_index) else {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    };
    if token.metadata_digest != read_sequence_step_metadata_digest(step)? {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    if token.parent_index.is_none() {
        let Some(query) = step.query.as_mut() else {
            return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
        };
        let Some(exclusive_start_key) = token.exclusive_start_key.clone() else {
            return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
        };
        query.exclusive_start_key = Some(ExclusiveStartKey::from(exclusive_start_key));
    }
    Ok(Some(token))
}

pub(super) fn encode_read_sequence_token(
    token: &ReadSequenceToken,
) -> Result<String, HttpApiError> {
    serde_json::to_string(token).map_err(|error| {
        HttpApiError::from(StorageError::internal(&format!(
            "serialize ReadSequence token: {error}"
        )))
    })
}

pub(super) fn read_sequence_request_digest(
    request: &ReadSequenceRequest,
) -> Result<String, HttpApiError> {
    let mut digest_request = request.clone();
    digest_request.next_sequence_token = None;
    let payload = serde_json::to_vec(&digest_request).map_err(|error| {
        HttpApiError::from(StorageError::internal(&format!(
            "serialize ReadSequence request digest: {error}"
        )))
    })?;
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string())
}

pub(super) fn read_sequence_step_metadata_digest(
    step: &ReadSequenceStep,
) -> Result<String, HttpApiError> {
    let payload = serde_json::to_vec(step).map_err(|error| {
        HttpApiError::from(StorageError::internal(&format!(
            "serialize ReadSequence token step metadata digest: {error}"
        )))
    })?;
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_OID, &payload).to_string())
}

pub(super) fn read_sequence_token_expiration() -> i64 {
    (Utc::now() + Duration::minutes(15)).timestamp()
}

fn decode_read_sequence_token(raw: &str) -> Result<ReadSequenceToken, HttpApiError> {
    serde_json::from_str(raw)
        .map_err(|_| read_sequence_error(ReadSequenceValidationError::StaleToken))
}

fn read_sequence_error(error: ReadSequenceValidationError) -> HttpApiError {
    HttpApiError::from(StorageError::from(error))
}
