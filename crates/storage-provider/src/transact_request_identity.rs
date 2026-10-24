//! Fixed canonical request fingerprints for transaction token receipts.

use std::io::{self, Write};

use sha2::{Digest, Sha256};
use storage_types::{
    ReturnConsumedCapacity, StorageError, StorageResult, TransactWriteItemsRequest,
};

const REQUEST_IDENTITY_VERSION: u8 = 2;
const JOB_REQUEST_IDENTITY_VERSION: u8 = 3;
const PUBLIC_REQUEST_IDENTITY_BYTES: usize = 33;
pub(crate) const REQUEST_IDENTITY_BYTES: usize = 65;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct TransactRequestIdentity {
    digest: [u8; 32],
    execution: Option<[u8; 32]>,
}

pub(crate) enum StoredRequestIdentity {
    Request([u8; PUBLIC_REQUEST_IDENTITY_BYTES]),
    Job([u8; REQUEST_IDENTITY_BYTES]),
}

impl StoredRequestIdentity {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Request(bytes) => bytes,
            Self::Job(bytes) => bytes,
        }
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "PascalCase")]
struct IdentityRequest<'a> {
    transact_items: &'a [storage_types::TransactWriteItem],
    #[serde(skip_serializing_if = "Option::is_none")]
    return_consumed_capacity: Option<ReturnConsumedCapacity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    return_item_collection_metrics: Option<&'a str>,
}

/// The compact JSON job receipts written by the prior main implementation.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredJobDigests {
    execution_digest: [u8; 32],
    request_digest: [u8; 32],
}

impl TransactRequestIdentity {
    pub(crate) fn try_from_request(request: &TransactWriteItemsRequest) -> StorageResult<Self> {
        let identity = IdentityRequest {
            transact_items: &request.transact_items,
            return_consumed_capacity: request.return_consumed_capacity,
            return_item_collection_metrics: request.return_item_collection_metrics.as_deref(),
        };
        Self::from_value(serde_json::to_value(identity)?)
    }

    pub(crate) fn from_stored(bytes: &[u8]) -> StorageResult<Self> {
        match bytes.first() {
            Some(&REQUEST_IDENTITY_VERSION) if bytes.len() == PUBLIC_REQUEST_IDENTITY_BYTES => {
                Ok(Self {
                    digest: bytes[1..].try_into().map_err(|_| invalid_fingerprint())?,
                    execution: None,
                })
            }
            Some(&JOB_REQUEST_IDENTITY_VERSION) if bytes.len() == REQUEST_IDENTITY_BYTES => {
                Ok(Self {
                    digest: bytes[1..PUBLIC_REQUEST_IDENTITY_BYTES]
                        .try_into()
                        .map_err(|_| invalid_fingerprint())?,
                    execution: Some(
                        bytes[PUBLIC_REQUEST_IDENTITY_BYTES..]
                            .try_into()
                            .map_err(|_| invalid_fingerprint())?,
                    ),
                })
            }
            Some(b'{') | Some(b'[') => Self::parse_prior_identity(bytes),
            _ => Err(invalid_fingerprint()),
        }
    }

    pub(crate) fn stored_bytes(&self) -> StoredRequestIdentity {
        if let Some(execution) = self.execution {
            let mut bytes = [0; REQUEST_IDENTITY_BYTES];
            bytes[0] = JOB_REQUEST_IDENTITY_VERSION;
            bytes[1..PUBLIC_REQUEST_IDENTITY_BYTES].copy_from_slice(&self.digest);
            bytes[PUBLIC_REQUEST_IDENTITY_BYTES..].copy_from_slice(&execution);
            StoredRequestIdentity::Job(bytes)
        } else {
            let mut bytes = [0; PUBLIC_REQUEST_IDENTITY_BYTES];
            bytes[0] = REQUEST_IDENTITY_VERSION;
            bytes[1..].copy_from_slice(&self.digest);
            StoredRequestIdentity::Request(bytes)
        }
    }

    pub(crate) fn execution(&self) -> Option<[u8; 32]> {
        self.execution
    }

    pub(crate) fn bind_execution(&mut self, execution: [u8; 32]) {
        let mut digest = Sha256::new();
        digest.update(b"aux-storage/job-effect-request/v1");
        digest.update(execution);
        digest.update(self.digest);
        self.digest = digest.finalize().into();
        self.execution = Some(execution);
    }

    // Public JSON receipts expire after ten minutes. Both prior job formats
    // remain replayable indefinitely and converge on the fixed fingerprint.
    fn parse_prior_identity(bytes: &[u8]) -> StorageResult<Self> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.is_array() {
            let (execution, request): (Vec<u8>, serde_json::Value) = serde_json::from_value(value)?;
            let mut identity = Self::from_value(request)?;
            identity.bind_execution(Sha256::digest(execution).into());
            return Ok(identity);
        }
        if value.get("execution_digest").is_some() {
            let stored: StoredJobDigests = serde_json::from_value(value)?;
            let mut identity = Self {
                digest: stored.request_digest,
                execution: None,
            };
            identity.bind_execution(stored.execution_digest);
            return Ok(identity);
        }
        Self::from_value(value)
    }

    fn from_value(mut value: serde_json::Value) -> StorageResult<Self> {
        value.sort_all_objects();
        let mut writer = IdentityHasher(Sha256::new());
        serde_json::to_writer(&mut writer, &value)?;
        Ok(Self {
            digest: writer.0.finalize().into(),
            execution: None,
        })
    }
}

struct IdentityHasher(Sha256);

impl Write for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn invalid_fingerprint() -> StorageError {
    StorageError::internal("invalid transaction request fingerprint")
}
