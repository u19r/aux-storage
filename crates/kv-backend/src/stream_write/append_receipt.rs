//! KV append receipts preserve the native commit-stamped response.

use storage_types::{ReadProtection, StorageError, StorageResult, storage_serde};
use stream_provider::{StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt};

use crate::{KvAttempt, KvMutation, constants::STREAM_APPEND_RECEIPT_PREFIX};

pub(crate) async fn load_append_receipt<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    operation: StreamAppendOperationId,
) -> StorageResult<Option<StreamAppendReceipt>> {
    let Some(bytes) = attempt.get(&receipt_key(operation), protection).await? else {
        return Ok(None);
    };
    let receipt: StreamAppendReceipt = if bytes.starts_with(b"\0vs/") {
        let stamp = bytes
            .get(4..16)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(invalid_receipt)?;
        let receipt: StreamAppendReceipt = storage_serde::from_bytes(&bytes[16..])?;
        receipt.with_commit_stamp(stamp)?
    } else {
        storage_serde::from_bytes(&bytes)?
    };
    receipt.require_identity(operation)?;
    receipt.response.resolve(None)?;
    Ok(Some(receipt))
}

pub(crate) async fn store_append_receipt<A: KvAttempt>(
    attempt: &mut A,
    receipt: &StreamAppendReceipt,
) -> StorageResult<()> {
    receipt.require_identity(receipt.operation)?;
    if attempt
        .get(&receipt_key(receipt.operation), ReadProtection::Conflict)
        .await?
        .is_some()
    {
        return Err(invalid_receipt());
    }
    let key = receipt_key(receipt.operation);
    let encoded = storage_serde::to_bytes(receipt)?;
    match receipt.response {
        StreamAppendAllocation::AttemptAllocated(_) => attempt.stage(vec![KvMutation::Put {
            key,
            value: encoded,
        }]),
        StreamAppendAllocation::Versionstamped(user_bytes) => {
            let mut value = b"\0vs/".to_vec();
            value.extend_from_slice(&[0xff; 10]);
            value.extend_from_slice(&user_bytes);
            value.extend_from_slice(&encoded);
            attempt.stage_versionstamped_value(key, value, 4)
        }
    }
}

fn receipt_key(operation: StreamAppendOperationId) -> Vec<u8> {
    let mut key = STREAM_APPEND_RECEIPT_PREFIX.to_vec();
    key.extend_from_slice(&operation.bytes());
    key
}

fn invalid_receipt() -> StorageError {
    StorageError::internal("Invalid KV append receipt")
}
