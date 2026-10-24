//! KV sequence authority for Dynamo writes and individual user streams.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};

use super::key_layout::append_segment;
use crate::{
    KvAttempt, KvMutation, ReadProtection,
    constants::{USER_STREAM_SEQUENCE_BYTES, USER_STREAM_SEQUENCE_PREFIX},
};

pub(crate) async fn allocate_stream_item_id<A: KvAttempt>(
    attempt: &mut A,
    stream_name: &StreamName,
) -> StorageResult<StreamItemId> {
    let key = user_stream_sequence_key(stream_name)?;
    let bytes = attempt
        .get_admitted(
            &key,
            ReadProtection::Conflict,
            crate::KvValueAllowance::new(USER_STREAM_SEQUENCE_BYTES, USER_STREAM_SEQUENCE_BYTES),
        )
        .await?;
    let counter =
        reserve_stream_counter(attempt, key, 1, "KV user stream sequence is missing", bytes)
            .await?;
    Ok(stream_provider::sequenced_stream_item_id(counter))
}

async fn reserve_stream_counter<A: KvAttempt>(
    attempt: &mut A,
    key: Vec<u8>,
    count: u64,
    missing_error: &'static str,
    bytes: Option<Vec<u8>>,
) -> StorageResult<u64> {
    let first = match bytes {
        Some(bytes) => <[u8; USER_STREAM_SEQUENCE_BYTES]>::try_from(bytes.as_slice())
            .map(u64::from_be_bytes)
            .map_err(|_| StorageError::internal("KV stream sequence is invalid"))?,
        None => return Err(StorageError::internal(missing_error)),
    };
    if first == 0 {
        return Err(StorageError::internal("KV stream sequence starts at zero"));
    }
    let next = first
        .checked_add(count)
        .ok_or_else(|| StorageError::internal("KV stream sequence overflowed"))?;
    attempt.stage(vec![KvMutation::Put {
        key,
        value: next.to_be_bytes().to_vec(),
    }])?;
    Ok(first)
}

pub(crate) fn user_stream_sequence_key(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    let mut key = USER_STREAM_SEQUENCE_PREFIX.to_vec();
    append_segment(&mut key, stream_name.as_ref())?;
    Ok(key)
}

/// Catalog creation already initializes this physical sequence. Explicit
/// initialization also serves callers that do not use that catalog writer.
pub(crate) async fn initialize_user_sequence<A: KvAttempt>(
    attempt: &mut A,
    stream: &StreamName,
) -> StorageResult<()> {
    let key = user_stream_sequence_key(stream)?;
    if attempt
        .get_admitted(
            &key,
            ReadProtection::Conflict,
            crate::KvValueAllowance::new(USER_STREAM_SEQUENCE_BYTES, USER_STREAM_SEQUENCE_BYTES),
        )
        .await?
        .is_none()
    {
        attempt.stage(vec![KvMutation::Put {
            key,
            value: 1u64.to_be_bytes().to_vec(),
        }])?;
    }
    Ok(())
}

pub(crate) fn delete_user_sequence<A: KvAttempt>(
    attempt: &mut A,
    stream: &StreamName,
) -> StorageResult<()> {
    attempt.stage(vec![KvMutation::Delete {
        key: user_stream_sequence_key(stream)?,
    }])
}
