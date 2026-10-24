use scheduled_bg_jobs::store::{Check, Mutation, StoreTransaction};
use storage_types::StorageError;

use crate::RawOrderedJobStoreError;

const CONTENT_VERSION: u8 = 1;

/// Encodes the exact ordered checks and mutations bound to one operation ID.
pub fn encode_transaction(
    transaction: &StoreTransaction,
) -> Result<Vec<u8>, RawOrderedJobStoreError> {
    let capacity = encoded_transaction_bytes(transaction)?;
    let mut content = Vec::new();
    content.try_reserve_exact(capacity).map_err(|_| {
        RawOrderedJobStoreError::Backend(
            storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
        )
    })?;
    content.push(CONTENT_VERSION);
    encode_count(&mut content, transaction.checks().len())?;
    for check in transaction.checks() {
        match check {
            Check::Absent(key) => {
                content.push(0);
                encode_bytes(&mut content, key.as_bytes())?;
            }
            Check::Value(key, value) => {
                content.push(1);
                encode_bytes(&mut content, key.as_bytes())?;
                encode_bytes(&mut content, value)?;
            }
        }
    }
    encode_count(&mut content, transaction.mutations().len())?;
    for mutation in transaction.mutations() {
        match mutation {
            Mutation::Put(key, value) => {
                content.push(0);
                encode_bytes(&mut content, key.as_bytes())?;
                encode_bytes(&mut content, value)?;
            }
            Mutation::Delete(key) => {
                content.push(1);
                encode_bytes(&mut content, key.as_bytes())?;
            }
        }
    }
    Ok(content)
}

fn encode_count(content: &mut Vec<u8>, count: usize) -> Result<(), RawOrderedJobStoreError> {
    content.extend_from_slice(&checked_u32(count)?.to_be_bytes());
    Ok(())
}

fn encode_bytes(content: &mut Vec<u8>, value: &[u8]) -> Result<(), RawOrderedJobStoreError> {
    content.extend_from_slice(&checked_u32(value.len())?.to_be_bytes());
    content.extend_from_slice(value);
    Ok(())
}

fn checked_u32(value: usize) -> Result<u32, RawOrderedJobStoreError> {
    u32::try_from(value).map_err(|_| encoding_too_large())
}

fn encoding_too_large() -> RawOrderedJobStoreError {
    RawOrderedJobStoreError::Backend(StorageError::validation(
        "scheduled-job transaction encoding exceeds 32-bit bounds",
    ))
}

fn encoded_transaction_bytes(
    transaction: &StoreTransaction,
) -> Result<usize, RawOrderedJobStoreError> {
    checked_u32(transaction.checks().len())?;
    checked_u32(transaction.mutations().len())?;
    let mut bytes = 1usize + 2 * std::mem::size_of::<u32>();
    for check in transaction.checks() {
        add_entry_bytes(
            &mut bytes,
            check.key().as_bytes(),
            match check {
                Check::Absent(_) => None,
                Check::Value(_, value) => Some(value.as_ref()),
            },
        )?;
    }
    for mutation in transaction.mutations() {
        add_entry_bytes(
            &mut bytes,
            mutation.key().as_bytes(),
            match mutation {
                Mutation::Delete(_) => None,
                Mutation::Put(_, value) => Some(value.as_ref()),
            },
        )?;
    }
    Ok(bytes)
}

fn add_entry_bytes(
    bytes: &mut usize,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<(), RawOrderedJobStoreError> {
    checked_u32(key.len())?;
    let mut entry_bytes = 1usize
        .checked_add(std::mem::size_of::<u32>())
        .and_then(|bytes| bytes.checked_add(key.len()))
        .ok_or_else(encoding_too_large)?;
    if let Some(value) = value {
        checked_u32(value.len())?;
        entry_bytes = entry_bytes
            .checked_add(std::mem::size_of::<u32>())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(encoding_too_large)?;
    }
    *bytes = bytes
        .checked_add(entry_bytes)
        .ok_or_else(encoding_too_large)?;
    Ok(())
}
