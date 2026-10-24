//! Test-only injection at logical operations and native commit boundaries.

use futures::future::BoxFuture;
use storage_types::{StorageEnum, StorageError, StorageResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvCommitBoundary {
    BeforeCommit,
    AfterDurableAcknowledgement,
}

impl KvCommitBoundary {
    pub(crate) async fn observe(self, probe: Option<&dyn KvFaultProbe>) -> StorageResult<()> {
        let Some(probe) = probe else {
            return Ok(());
        };
        probe
            .at_commit_boundary(self)
            .await
            .map_err(|_| match self {
                Self::BeforeCommit => StorageEnum::TransactionConflict {
                    message: "KV commit was rejected before native submission".to_owned(),
                }
                .into(),
                Self::AfterDurableAcknowledgement => StorageError::sync_submission_ambiguous(
                    None,
                    "KV durable commit response was suppressed at its native boundary",
                ),
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvFaultOperation {
    GetBatch,
    Scan,
    ScanBatch,
    Stage,
    StageVersionstamped,
}

#[derive(Clone, Copy)]
pub struct KvFaultCall {
    pub operation: KvFaultOperation,
}

pub trait KvFaultProbe: Send + Sync {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError>;

    fn at_commit_boundary(&self, _boundary: KvCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn alter_get_batch(
        &self,
        _call: KvFaultCall,
        values: Vec<Option<Vec<u8>>>,
    ) -> Vec<Option<Vec<u8>>> {
        values
    }
}
