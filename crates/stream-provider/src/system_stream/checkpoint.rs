use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use storage_types::{
    StorageError, StorageResult, SystemStreamCheckpoint, SystemStreamCheckpointToken,
    SystemStreamScope, TimestampMillis,
};
use uuid::Uuid;

/// Checkpoint issuance values prepared once before native conflict retries.
#[derive(Clone, Serialize, Deserialize)]
pub struct SystemStreamCheckpointIssue {
    pub input_token: String,
    pub input_state: Vec<u8>,
    pub result_state: Vec<u8>,
    pub operation_key: String,
    pub limit: u32,
    pub issued_token: SystemStreamCheckpointToken,
    pub issued_at: TimestampMillis,
}

impl SystemStreamCheckpointIssue {
    pub fn capture(
        input_token: Option<&SystemStreamCheckpointToken>,
        input_checkpoint: &SystemStreamCheckpoint,
        limit: u32,
        result_checkpoint: &SystemStreamCheckpoint,
    ) -> StorageResult<Self> {
        Self::try_new(
            input_token,
            input_checkpoint,
            limit,
            result_checkpoint,
            new_checkpoint_token()?,
            TimestampMillis::now(),
        )
    }

    pub fn try_new(
        input_token: Option<&SystemStreamCheckpointToken>,
        input_checkpoint: &SystemStreamCheckpoint,
        limit: u32,
        result_checkpoint: &SystemStreamCheckpoint,
        issued_token: SystemStreamCheckpointToken,
        issued_at: TimestampMillis,
    ) -> StorageResult<Self> {
        let input_state = input_checkpoint
            .encode_state()
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let result_state = result_checkpoint
            .encode_state()
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let input_token =
            input_token.map_or_else(|| "initial".to_string(), |token| token.as_str().to_string());
        let operation_key = format!(
            "v1:{}:{}:{}:{}:{}:{limit}:{}",
            result_checkpoint.scope().version(),
            result_checkpoint.scope().scope_id().as_str(),
            result_checkpoint.scope().generation(),
            input_token,
            encode_hex(&input_state),
            encode_hex(&result_state),
        );
        Ok(Self {
            input_token,
            input_state,
            result_state,
            operation_key,
            limit,
            issued_token,
            issued_at,
        })
    }

    #[must_use]
    pub fn receipt(&self, checkpoint: &SystemStreamCheckpoint) -> SystemStreamCheckpointReceipt {
        SystemStreamCheckpointReceipt {
            token: self.issued_token.clone(),
            scope: checkpoint.scope().clone(),
            operation_key: self.operation_key.clone(),
            input_state: self.input_state.clone(),
            limit: self.limit,
            result_state: self.result_state.clone(),
        }
    }

    #[must_use]
    pub fn matches_receipt(&self, receipt: &SystemStreamCheckpointReceipt) -> bool {
        receipt.operation_key == self.operation_key
            && receipt.input_state == self.input_state
            && receipt.limit == self.limit
            && receipt.result_state == self.result_state
    }
}

/// Canonical values decoded from a durable provider checkpoint receipt.
#[derive(Clone, Deserialize, Serialize)]
pub struct SystemStreamCheckpointReceipt {
    pub token: SystemStreamCheckpointToken,
    pub scope: SystemStreamScope,
    pub operation_key: String,
    pub input_state: Vec<u8>,
    pub limit: u32,
    pub result_state: Vec<u8>,
}

/// The two indexed addresses of one durable issuance receipt.
pub enum SystemStreamCheckpointLookup<'a> {
    Token(&'a SystemStreamCheckpointToken),
    Operation(&'a str),
}

fn new_checkpoint_token() -> StorageResult<SystemStreamCheckpointToken> {
    let mut encoded = String::with_capacity(35);
    encoded.push_str("v1.");
    for byte in Uuid::new_v4().into_bytes() {
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| StorageError::internal("SystemStream token encoding failed"))?;
    }
    SystemStreamCheckpointToken::try_from(encoded)
        .map_err(|error| StorageError::validation(error.to_string()))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
