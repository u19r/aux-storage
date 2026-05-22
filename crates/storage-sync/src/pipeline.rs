use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::SyncWriteRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncProposalPipelineLimits {
    pub max_batch_operations: usize,
    pub max_batch_bytes: usize,
    pub max_queue_depth: usize,
    pub max_proposal_latency_ms: u64,
}

impl Default for SyncProposalPipelineLimits {
    fn default() -> Self {
        Self {
            max_batch_operations: 100,
            max_batch_bytes: 1024 * 1024,
            max_queue_depth: 1024,
            max_proposal_latency_ms: 250,
        }
    }
}

impl SyncProposalPipelineLimits {
    pub fn validate_request(&self, request: &SyncWriteRequest) -> StorageResult<SyncProposalShape> {
        let shape = SyncProposalShape {
            operation_count: request.operation_count(),
            byte_count: serde_json::to_vec(request)?.len(),
        };
        if shape.operation_count > self.max_batch_operations {
            return Err(storage_types::StorageError::validation(format!(
                "sync proposal operation count {} exceeds limit {}",
                shape.operation_count, self.max_batch_operations
            )));
        }
        if shape.byte_count > self.max_batch_bytes {
            return Err(storage_types::StorageError::validation(format!(
                "sync proposal byte count {} exceeds limit {}",
                shape.byte_count, self.max_batch_bytes
            )));
        }
        Ok(shape)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncProposalShape {
    pub operation_count: usize,
    pub byte_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncProposalPipelineQueueFull {
    pub max_queue_depth: usize,
}

impl Display for SyncProposalPipelineQueueFull {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "sync proposal queue depth exceeds limit {}",
            self.max_queue_depth
        )
    }
}

impl std::error::Error for SyncProposalPipelineQueueFull {}

impl SyncWriteRequest {
    #[must_use]
    pub fn operation_count(&self) -> usize {
        match self {
            Self::BatchWriteItem(request) => {
                request.request_items.values().map(std::vec::Vec::len).sum()
            }
            Self::TransactWriteItems(request) => request.transact_items.len(),
            Self::PutItem(_)
            | Self::UpdateItem(_)
            | Self::DeleteItem(_)
            | Self::CreateTable(_)
            | Self::UpdateTable(_)
            | Self::DeleteTable(_)
            | Self::UpdateTimeToLive(_) => 1,
        }
    }
}
