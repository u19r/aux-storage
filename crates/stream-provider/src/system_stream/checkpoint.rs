//! Durable checkpoint receipts bind source progress and the initial visible
//! window.

use serde::{Deserialize, Serialize};
use storage_types::{
    StorageError, StorageResult, SystemStreamCheckpoint, SystemStreamCheckpointToken,
    SystemStreamScope, TimestampMillis,
};

/// Issuance identity and time are captured once before native conflict retries.
pub struct SystemStreamCheckpointIssueInput<'a> {
    pub input_token: Option<&'a SystemStreamCheckpointToken>,
    pub input_checkpoint: &'a SystemStreamCheckpoint,
    pub result_checkpoint: &'a SystemStreamCheckpoint,
    pub limit: u32,
    pub issued_token: SystemStreamCheckpointToken,
    pub issued_at: TimestampMillis,
    pub initial_visibility_cutoff: TimestampMillis,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SystemStreamCheckpointIssue {
    pub input_token: String,
    pub input_state: Vec<u8>,
    pub result_state: Vec<u8>,
    pub operation_key: String,
    pub limit: u32,
    pub issued_token: SystemStreamCheckpointToken,
    pub issued_at: TimestampMillis,
    pub initial_visibility_cutoff: TimestampMillis,
}

impl TryFrom<SystemStreamCheckpointIssueInput<'_>> for SystemStreamCheckpointIssue {
    type Error = StorageError;

    fn try_from(input: SystemStreamCheckpointIssueInput<'_>) -> StorageResult<Self> {
        let invalid = |error: storage_types::SystemStreamContractError| {
            StorageError::validation(error.to_string())
        };
        let input_state = input.input_checkpoint.encode_state().map_err(invalid)?;
        let result_state = input.result_checkpoint.encode_state().map_err(invalid)?;
        let input_token = input
            .input_token
            .map_or_else(|| "initial".to_string(), |token| token.as_str().to_string());
        let scope = input.result_checkpoint.scope();
        let operation_key = format!(
            "v2:{}:{}:{}:{}:{}:{}:{}:{}",
            scope.version(),
            scope.scope_id().as_str(),
            scope.generation(),
            input_token,
            hex::encode(&input_state),
            hex::encode(&result_state),
            input.limit,
            input.initial_visibility_cutoff.timestamp_millis()
        );
        Ok(Self {
            input_token,
            input_state,
            result_state,
            operation_key,
            limit: input.limit,
            issued_token: input.issued_token,
            issued_at: input.issued_at,
            initial_visibility_cutoff: input.initial_visibility_cutoff,
        })
    }
}

impl SystemStreamCheckpointIssue {
    pub fn receipt(&self, checkpoint: &SystemStreamCheckpoint) -> SystemStreamCheckpointReceipt {
        SystemStreamCheckpointReceipt {
            token: self.issued_token.clone(),
            scope: checkpoint.scope().clone(),
            operation_key: self.operation_key.clone(),
            input_state: self.input_state.clone(),
            limit: self.limit,
            result_state: self.result_state.clone(),
            initial_visibility_cutoff: self.initial_visibility_cutoff,
        }
    }

    pub fn matches_receipt(&self, receipt: &SystemStreamCheckpointReceipt) -> bool {
        receipt.operation_key == self.operation_key
            && receipt.input_state == self.input_state
            && receipt.limit == self.limit
            && receipt.result_state == self.result_state
            && receipt.initial_visibility_cutoff == self.initial_visibility_cutoff
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SystemStreamCheckpointReceipt {
    pub token: SystemStreamCheckpointToken,
    pub scope: SystemStreamScope,
    pub operation_key: String,
    pub input_state: Vec<u8>,
    pub limit: u32,
    pub result_state: Vec<u8>,
    pub initial_visibility_cutoff: TimestampMillis,
}

pub enum SystemStreamCheckpointLookup<'a> {
    Token(&'a SystemStreamCheckpointToken),
    Operation(&'a str),
}
