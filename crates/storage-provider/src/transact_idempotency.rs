//! Transact-write token encoding, validation, and expiry decisions.

use storage_types::{
    StorageError, StorageResult, TimestampMillis, TransactWriteItemsRequest,
    TransactWriteItemsResponse,
};

use crate::{
    DynamoTransactWriteAttempt, SemanticDecision,
    transact_request_identity::TransactRequestIdentity,
};

/// Idempotency window for completed transact-write token receipts.
pub const TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS: i64 = 10 * 60 * 1000;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactReceipt<I> {
    request_identity: I,
    response: Vec<u8>,
    completed_at: TimestampMillis,
    #[serde(default, skip_serializing_if = "ReceiptRetention::is_request_window")]
    retention: ReceiptRetention,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ReceiptRetention {
    #[default]
    RequestWindow,
    JobExecution,
}

impl ReceiptRetention {
    fn is_request_window(&self) -> bool {
        matches!(self, Self::RequestWindow)
    }
}

impl<I> TransactReceipt<I> {
    fn expired(&self, at: TimestampMillis) -> bool {
        self.retention.is_request_window() && transact_write_token_expired(self.completed_at, at)
    }
}

pub(crate) async fn find_transact_response<A: DynamoTransactWriteAttempt>(
    attempt: &mut A,
    request: Option<&TransactTokenRequest>,
    captured_at: TimestampMillis,
) -> StorageResult<SemanticDecision<Option<TransactWriteItemsResponse>>> {
    let Some(request) = request else {
        return Ok(SemanticDecision::Accepted(None));
    };
    let Some(bytes) = attempt.read_transact_receipt(request.token()).await? else {
        return Ok(SemanticDecision::Accepted(None));
    };
    let mut receipt: TransactReceipt<Vec<u8>> = storage_types::storage_serde::from_bytes(&bytes)?;
    if !request.matches_retention(&receipt.retention) {
        return Err(StorageError::internal(
            "transaction receipt retention differs from its execution",
        ));
    }
    if receipt.expired(captured_at) {
        return Ok(SemanticDecision::Accepted(None));
    }
    if TransactRequestIdentity::from_stored(&receipt.request_identity)? != request.identity {
        return Ok(SemanticDecision::Rejected(
            StorageError::idempotent_parameter_mismatch(),
        ));
    }
    let response = decode_token_response(&receipt.response)?;
    let identity = request.identity.stored_bytes();
    if !receipt.retention.is_request_window() && receipt.request_identity != identity.as_slice() {
        receipt.request_identity = identity.as_slice().to_vec();
        attempt
            .write_transact_receipt(
                request.token(),
                storage_types::storage_serde::to_bytes(&receipt)?,
            )
            .await?;
    }
    Ok(SemanticDecision::Accepted(Some(response)))
}

pub(crate) async fn find_job_effect_response<A: DynamoTransactWriteAttempt>(
    attempt: &mut A,
    token: &str,
    authority: &crate::job_write_authority::JobWriteAuthority,
) -> StorageResult<Option<TransactWriteItemsResponse>> {
    let Some(bytes) = attempt
        .read_transact_receipt(&authority.receipt_key(token))
        .await?
    else {
        return Ok(None);
    };
    let receipt: TransactReceipt<Vec<u8>> = storage_types::storage_serde::from_bytes(&bytes)?;
    let identity = TransactRequestIdentity::from_stored(&receipt.request_identity)?;
    if !matches!(receipt.retention, ReceiptRetention::JobExecution)
        || identity.execution() != Some(authority.execution_identity())
    {
        return Err(StorageError::internal(
            "invalid scheduled-job effect receipt",
        ));
    }
    decode_token_response(&receipt.response).map(Some)
}

pub(crate) fn encode_transact_receipt(
    request: &TransactTokenRequest,
    completed_at: TimestampMillis,
) -> StorageResult<Vec<u8>> {
    let response = serde_json::to_vec(&TransactWriteItemsResponse {
        consumed_capacity: None,
        item_collection_metrics: None,
    })?;
    let bytes = storage_types::storage_serde::to_bytes(&TransactReceipt {
        request_identity: request.identity.stored_bytes().as_slice(),
        response,
        completed_at,
        retention: request.retention(),
    })?;
    if bytes.len() > TRANSACT_RECEIPT_WRITE_BYTES {
        return Err(StorageError::internal(
            "transaction receipt exceeds fixed encoding bound",
        ));
    }
    Ok(bytes)
}

/// Maximum encoded new receipt: fixed envelope, byte arrays and signed
/// timestamp. This stays below the raw JSON threshold, so encoding never adds
/// compression.
pub const TRANSACT_RECEIPT_WRITE_BYTES: usize = storage_types::STORAGE_SERDE_ENCODED_PREFIX_LEN
    + b"{\"request_identity\":[],\"response\":[],\"completed_at\":}".len()
    + (crate::transact_request_identity::REQUEST_IDENTITY_BYTES * 4 - 1)
    + (2 * 4 - 1)
    + 20
    + b",\"retention\":{\"type\":\"job_execution\"}".len();

const _: () = assert!(
    TRANSACT_RECEIPT_WRITE_BYTES - storage_types::STORAGE_SERDE_ENCODED_PREFIX_LEN
        <= storage_types::STORAGE_SERDE_RAW_JSON_LIMIT_BYTES
);

#[derive(Clone)]
pub(crate) struct TransactTokenRequest {
    token: String,
    identity: TransactRequestIdentity,
}

impl TransactTokenRequest {
    pub(crate) fn try_from_request(
        request: &TransactWriteItemsRequest,
    ) -> StorageResult<Option<Self>> {
        request
            .client_request_token
            .as_ref()
            .map(|token| {
                Ok(Self {
                    token: token.clone(),
                    identity: TransactRequestIdentity::try_from_request(request)?,
                })
            })
            .transpose()
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    fn retention(&self) -> ReceiptRetention {
        if self.identity.execution().is_some() {
            ReceiptRetention::JobExecution
        } else {
            ReceiptRetention::RequestWindow
        }
    }

    fn matches_retention(&self, retention: &ReceiptRetention) -> bool {
        self.retention() == *retention
    }

    pub(crate) fn authorize_effect(
        &mut self,
        authority: &crate::job_write_authority::JobWriteAuthority,
    ) {
        self.token = authority.receipt_key(&self.token);
        let execution = authority.execution_identity();
        self.identity.bind_execution(execution);
    }
}

/// Validate stored token response bytes against the direct serde form.
pub fn decode_token_response(response_bytes: &[u8]) -> StorageResult<TransactWriteItemsResponse> {
    let response: TransactWriteItemsResponse = serde_json::from_slice(response_bytes)
        .map_err(|_| StorageError::internal("invalid transact-write token response"))?;
    if serde_json::to_vec(&response)? != response_bytes {
        return Err(StorageError::internal(
            "non-canonical transact-write token response",
        ));
    }
    Ok(response)
}

/// Whether a completed receipt is older than the idempotency window at the
/// capturing request's timestamp.
#[must_use]
pub fn transact_write_token_expired(
    completed_at: TimestampMillis,
    request_captured_at: TimestampMillis,
) -> bool {
    i128::from(request_captured_at.timestamp_millis())
        >= i128::from(completed_at.timestamp_millis())
            + i128::from(TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS)
}
