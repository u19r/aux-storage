//! Durable response identity for a captured table append.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_types::{
    StorageError, StorageResult, StreamItemId, TableHistoryId, TableHistoryTarget, TableName,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "uuid::Uuid", into = "uuid::Uuid")]
pub struct StreamAppendOperationId(uuid::Uuid);

impl StreamAppendOperationId {
    pub fn fresh() -> Self {
        Self(uuid::Uuid::new_v4())
    }
    pub fn bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
    pub fn token(self) -> String {
        self.0.to_string()
    }
    pub fn parse(token: &str) -> StorageResult<Self> {
        uuid::Uuid::parse_str(token)
            .map_err(|_| StorageError::validation("Invalid append operation ID"))?
            .try_into()
    }
}

impl TryFrom<uuid::Uuid> for StreamAppendOperationId {
    type Error = StorageError;
    fn try_from(value: uuid::Uuid) -> StorageResult<Self> {
        if value.is_nil() {
            return Err(StorageError::validation(
                "Append operation ID cannot be zero",
            ));
        }
        Ok(Self(value))
    }
}

impl From<StreamAppendOperationId> for uuid::Uuid {
    fn from(value: StreamAppendOperationId) -> Self {
        value.0
    }
}

#[derive(Clone)]
pub struct TableAppendRequest {
    operation: StreamAppendOperationId,
    target: TableHistoryTarget,
    digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableAppendReceiptKey {
    pub history_id: TableHistoryId,
    pub operation: StreamAppendOperationId,
}

impl From<&TableAppendRequest> for TableAppendReceiptKey {
    fn from(request: &TableAppendRequest) -> Self {
        Self {
            history_id: request.target.history_id,
            operation: request.operation,
        }
    }
}

impl TableAppendRequest {
    pub fn operation(&self) -> StreamAppendOperationId {
        self.operation
    }
    pub fn table(&self) -> &TableName {
        &self.target.table_name
    }
    pub fn target(&self) -> &TableHistoryTarget {
        &self.target
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn new(
        operation: StreamAppendOperationId,
        target: TableHistoryTarget,
        data: &[u8],
    ) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"table-append/2");
        digest.update(target.history_id.as_ref());
        digest.update((target.table_name.as_ref().len() as u64).to_be_bytes());
        digest.update(target.table_name.as_ref().as_bytes());
        digest.update(data);
        Self {
            operation,
            target,
            digest: digest.finalize().into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum StreamAppendAllocation {
    AttemptAllocated(StreamItemId),
    Versionstamped([u8; 2]),
}

impl StreamAppendAllocation {
    pub fn resolve(self, stamp: Option<[u8; 10]>) -> StorageResult<StreamItemId> {
        match self {
            Self::AttemptAllocated(id) => Ok(id),
            Self::Versionstamped(user_bytes) => {
                let stamp = stamp.ok_or_else(|| {
                    StorageError::sync_submission_ambiguous(
                        None,
                        "stream commit had no versionstamp",
                    )
                })?;
                let mut id = [0u8; 12];
                id[..10].copy_from_slice(&stamp);
                id[10..].copy_from_slice(&user_bytes);
                Ok(StreamItemId::from(id))
            }
        }
    }
}

/// The proof is serialized by the capture owner and never interpreted by a
/// driver.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamAppendReceipt {
    version: u16,
    pub history_id: TableHistoryId,
    pub operation: StreamAppendOperationId,
    pub request_digest: [u8; 32],
    pub proof: Vec<u8>,
    pub response: StreamAppendAllocation,
}

impl StreamAppendReceipt {
    pub fn new(
        request: &TableAppendRequest,
        proof: Vec<u8>,
        response: StreamAppendAllocation,
    ) -> Self {
        Self {
            version: crate::constants::STREAM_APPEND_RECEIPT_VERSION,
            history_id: request.target.history_id,
            operation: request.operation,
            request_digest: request.digest,
            proof,
            response,
        }
    }

    pub fn require_identity(&self, key: TableAppendReceiptKey) -> StorageResult<()> {
        if self.version != crate::constants::STREAM_APPEND_RECEIPT_VERSION
            || self.operation != key.operation
            || self.history_id != key.history_id
            || self.history_id.as_ref() == &[0; 16]
            || self.proof.is_empty()
        {
            return Err(invalid_receipt());
        }
        Ok(())
    }

    pub fn require_request(&self, request: &TableAppendRequest) -> StorageResult<()> {
        self.require_identity(request.into())?;
        if self.request_digest != request.digest {
            return Err(invalid_receipt());
        }
        Ok(())
    }

    pub fn with_commit_stamp(mut self, stamp: [u8; 12]) -> StorageResult<Self> {
        let StreamAppendAllocation::Versionstamped(user_bytes) = self.response else {
            return Err(invalid_receipt());
        };
        if stamp[..10] == [0xff; 10] || stamp[10..] != user_bytes {
            return Err(invalid_receipt());
        }
        self.response = StreamAppendAllocation::AttemptAllocated(StreamItemId::from(stamp));
        Ok(self)
    }
}

impl From<&StreamAppendReceipt> for TableAppendReceiptKey {
    fn from(receipt: &StreamAppendReceipt) -> Self {
        Self {
            history_id: receipt.history_id,
            operation: receipt.operation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableAppendReconciliation {
    Durable(StreamItemId),
    Unresolved,
}

fn invalid_receipt() -> StorageError {
    StorageError::internal("Invalid table append receipt")
}
