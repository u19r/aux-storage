//! Checkpoint lookups join the customer's native read-write attempt.

use storage_types::{
    ReadProtection, StorageError, StorageResult, SystemStreamPosition, SystemStreamScope,
    storage_serde,
};
use stream_provider::{SystemStreamCheckpointLookup, SystemStreamCheckpointReceipt};

use super::key_layout;
use crate::{KvAttempt, KvRangePage};

pub(crate) async fn read_receipt<A: KvAttempt>(
    attempt: &mut A,
    lookup: SystemStreamCheckpointLookup<'_>,
) -> StorageResult<Option<SystemStreamCheckpointReceipt>> {
    let key = match lookup {
        SystemStreamCheckpointLookup::Token(token) => key_layout::token(token),
        SystemStreamCheckpointLookup::Operation(key) => key_layout::operation(key),
    };
    attempt
        .get(&key, ReadProtection::Conflict)
        .await?
        .map(|bytes| storage_serde::from_bytes(&bytes))
        .transpose()
}

pub(crate) fn decode_protected_frontiers(
    page: KvRangePage,
    scope: &SystemStreamScope,
) -> StorageResult<Vec<SystemStreamPosition>> {
    if page.more || page.entries.len() > storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        return Err(StorageError::internal(
            "KV SystemStream frontier exceeds its bound",
        ));
    }
    page.entries
        .into_iter()
        .map(|entry| {
            let position: SystemStreamPosition = storage_serde::from_bytes(&entry.value)?;
            if key_layout::frontier(scope, &position)? != entry.key {
                return Err(StorageError::internal(
                    "KV SystemStream frontier key differs from value",
                ));
            }
            Ok(position)
        })
        .collect()
}
