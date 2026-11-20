//! Transact-write token encoding, validation, and expiry decisions.

use scheduled_bg_jobs::store::StoreKey;
use storage_types::{
    StorageError, StorageResult, TimestampMillis, TransactWriteItemsRequest,
    TransactWriteItemsResponse,
};

use crate::{ReceiptSlot, SemanticDecision, transact_request_identity::TransactRequestIdentity};

/// Idempotency window for completed transact-write token receipts.
pub const TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS: i64 = 10 * 60 * 1000;

/// Delay after the window before a receipt may be trimmed. A retry captured
/// inside the window on one node can read the receipt only later: it still
/// spends its Transaction budget (15 s by default, configurable) and commit
/// grace (2 s), and node clocks differ. Five minutes covers that budget, the
/// grace and a clock allowance, so such a retry always finds its receipt.
pub const TRANSACT_RECEIPT_TRIM_DELAY_MILLIS: i64 = 5 * 60 * 1000;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactReceipt<I> {
    request_identity: I,
    response: Vec<u8>,
    completed_at: TimestampMillis,
}

pub(crate) fn accept_transact_response(
    request: Option<&TransactTokenRequest>,
    bytes: Option<Vec<u8>>,
    captured_at: TimestampMillis,
) -> StorageResult<SemanticDecision<Option<TransactWriteItemsResponse>>> {
    let (Some(request), Some(bytes)) = (request, bytes) else {
        return Ok(SemanticDecision::Accepted(None));
    };
    let receipt: TransactReceipt<Vec<u8>> = storage_types::storage_serde::from_bytes(&bytes)?;
    // A job-effect receipt lives exactly as long as its execution; only a
    // request receipt has a time window.
    if request.job_effect_key.is_none()
        && transact_write_token_expired(receipt.completed_at, captured_at)
    {
        return Ok(SemanticDecision::Accepted(None));
    }
    if TransactRequestIdentity::from_stored(&receipt.request_identity)? != request.identity {
        return Ok(SemanticDecision::Rejected(
            StorageError::idempotent_parameter_mismatch(),
        ));
    }
    decode_token_response(&receipt.response)
        .map(|response| SemanticDecision::Accepted(Some(response)))
}

/// A request receipt is trimmable once its window and the trim delay have
/// passed. Job-effect receipts are not in this table.
pub(crate) fn transact_receipt_is_trimmable(
    bytes: &[u8],
    at: TimestampMillis,
) -> StorageResult<bool> {
    let receipt: TransactReceipt<Vec<u8>> = storage_types::storage_serde::from_bytes(bytes)?;
    Ok(i128::from(at.timestamp_millis())
        >= i128::from(receipt.completed_at.timestamp_millis())
            + i128::from(TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS)
            + i128::from(TRANSACT_RECEIPT_TRIM_DELAY_MILLIS))
}

pub(crate) async fn find_job_effect_response<A: crate::DynamoTransactWriteAttempt>(
    attempt: &mut A,
    token: &str,
    authority: &crate::job_write_authority::JobWriteAuthority,
) -> StorageResult<Option<TransactWriteItemsResponse>> {
    let key = authority.receipt_key(token)?;
    let Some(bytes) = attempt
        .read_transact_receipt(ReceiptSlot::JobEffect(&key))
        .await?
    else {
        return Ok(None);
    };
    let receipt: TransactReceipt<Vec<u8>> = storage_types::storage_serde::from_bytes(&bytes)?;
    let identity = TransactRequestIdentity::from_stored(&receipt.request_identity)?;
    if identity.execution() != Some(authority.execution_identity()) {
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
    + 20;

const _: () = assert!(
    TRANSACT_RECEIPT_WRITE_BYTES - storage_types::STORAGE_SERDE_ENCODED_PREFIX_LEN
        <= storage_types::STORAGE_SERDE_RAW_JSON_LIMIT_BYTES
);

#[derive(Clone)]
pub(crate) struct TransactTokenRequest {
    token: String,
    identity: TransactRequestIdentity,
    job_effect_key: Option<StoreKey>,
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
                    job_effect_key: None,
                })
            })
            .transpose()
    }

    pub(crate) fn slot(&self) -> ReceiptSlot<'_> {
        match &self.job_effect_key {
            Some(key) => ReceiptSlot::JobEffect(key),
            None => ReceiptSlot::Request(&self.token),
        }
    }

    pub(crate) fn authorize_effect(
        &mut self,
        authority: &crate::job_write_authority::JobWriteAuthority,
    ) -> StorageResult<()> {
        self.job_effect_key = Some(authority.receipt_key(&self.token)?);
        self.identity.bind_execution(authority.execution_identity());
        Ok(())
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
