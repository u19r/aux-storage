//! Checkpoint lookups join the customer's native read-write attempt.

use storage_types::{ReadProtection, StorageResult, storage_serde};
use stream_provider::{SystemStreamCheckpointLookup, SystemStreamCheckpointReceipt};

use super::key_layout;
use crate::KvAttempt;

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
