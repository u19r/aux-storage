//! KV append receipts preserve the native commit-stamped response.

use storage_types::{ReadProtection, StorageError, StorageResult, storage_serde};
use stream_provider::{
    STREAM_APPEND_RECEIPT_ENCODED_BYTES, StreamAppendAllocation, StreamAppendReceipt,
    TableAppendReceiptKey,
};

use crate::{
    KvAttempt, KvMutation, KvValueAllowance,
    table_key_layout::{TableStreamFamily, table_stream_prefix},
};

/// `\0vs/`, the 10-byte stamp and the 2 user bytes before a versionstamped
/// receipt's encoding.
const VERSIONSTAMPED_RECEIPT_HEAD_BYTES: usize = b"\0vs/".len() + 10 + 2;

/// Largest logical receipt value.
pub(crate) const APPEND_RECEIPT_VALUE_BYTES: usize =
    VERSIONSTAMPED_RECEIPT_HEAD_BYTES + STREAM_APPEND_RECEIPT_ENCODED_BYTES;

// Receipts are never chunked: the value, its inline tag and the native
// versionstamp offset trailer fit one FoundationDB value.
// 100_000 is the FoundationDB value limit (`FOUNDATIONDB_NATIVE_VALUE_BYTES`,
// which is compiled only with the `foundationdb` feature or in tests).
const _: () = assert!(APPEND_RECEIPT_VALUE_BYTES + 1 + std::mem::size_of::<u32>() <= 100_000);

fn receipt_allowance() -> KvValueAllowance {
    KvValueAllowance::new(APPEND_RECEIPT_VALUE_BYTES, APPEND_RECEIPT_VALUE_BYTES)
}

pub(crate) async fn load_append_receipt<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    key: TableAppendReceiptKey,
) -> StorageResult<Option<StreamAppendReceipt>> {
    let Some(bytes) = attempt
        .get_admitted(&receipt_key(key)?, protection, receipt_allowance())
        .await?
    else {
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
    receipt.require_identity(key)?;
    receipt.response.resolve(None)?;
    Ok(Some(receipt))
}

pub(crate) async fn store_append_receipt<A: KvAttempt>(
    attempt: &mut A,
    receipt: &StreamAppendReceipt,
) -> StorageResult<()> {
    receipt.require_identity(receipt.into())?;
    let key = receipt_key(receipt.into())?;
    if attempt
        .get_admitted(&key, ReadProtection::Conflict, receipt_allowance())
        .await?
        .is_some()
    {
        return Err(invalid_receipt());
    }
    let encoded = storage_serde::to_bytes(receipt)?;
    if encoded.len() > STREAM_APPEND_RECEIPT_ENCODED_BYTES {
        return Err(StorageError::internal(
            "KV append receipt exceeds its encoded bound",
        ));
    }
    match receipt.response {
        StreamAppendAllocation::AttemptAllocated(_) => {
            attempt.stage(vec![KvMutation::PutUnchunked {
                key,
                value: encoded,
            }])
        }
        StreamAppendAllocation::Versionstamped(user_bytes) => {
            let mut value = Vec::new();
            value
                .try_reserve_exact(VERSIONSTAMPED_RECEIPT_HEAD_BYTES + encoded.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            value.extend_from_slice(b"\0vs/");
            value.extend_from_slice(&[0xff; 10]);
            value.extend_from_slice(&user_bytes);
            value.extend_from_slice(&encoded);
            attempt.stage_versionstamped_unchunked_value(key, value, 4)
        }
    }
}

pub(crate) fn receipt_prefix(history: storage_types::TableHistoryId) -> StorageResult<Vec<u8>> {
    Ok(table_stream_prefix(
        crate::catalog::table_id_from_history(history)?,
        TableStreamFamily::AppendReceipt,
    )
    .to_vec())
}

fn receipt_key(lookup: TableAppendReceiptKey) -> StorageResult<Vec<u8>> {
    let mut key = receipt_prefix(lookup.history_id)?;
    key.extend_from_slice(&lookup.operation.bytes());
    Ok(key)
}

fn invalid_receipt() -> StorageError {
    StorageError::internal("Invalid KV append receipt")
}
