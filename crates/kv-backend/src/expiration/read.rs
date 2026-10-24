//! Protected KV TTL index scan with a byte bound and stable continuation.

use std::num::NonZeroU32;

use storage_provider::{
    TtlIndexRead,
    maintenance::{EXPIRATION_SCAN_BYTES, ExpirationCandidateCursor, ExpirationIndexLocator},
};
use storage_types::{StorageError, StorageResult};

use super::key_layout::{decode_index_key, scan_begin, scan_end};
use crate::{KvAttempt, KvRange, ReadProtection};

pub(super) async fn read_expiration_index<A: KvAttempt>(
    attempt: &mut A,
    table_id: u32,
    read: &TtlIndexRead,
) -> StorageResult<(Vec<ExpirationIndexLocator>, bool)> {
    let cursor = read
        .cursor
        .as_ref()
        .map(ExpirationCandidateCursor::try_from)
        .transpose()?;
    let limit = NonZeroU32::new(read.limit.get() as u32 + 1)
        .ok_or_else(|| StorageError::internal("expiration page limit overflow"))?;
    let page = attempt
        .scan(
            KvRange {
                entry_bounds: None,
                begin: scan_begin(table_id, cursor)?,
                end: scan_end(table_id, read.cutoff_seconds)?,
                reverse: false,
                limit,
                max_bytes: EXPIRATION_SCAN_BYTES,
            },
            ReadProtection::Conflict,
        )
        .await?;
    let more = page.more;
    let mut rows = Vec::new();
    rows.try_reserve_exact(page.entries.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for entry in page.entries {
        if !entry.value.is_empty() {
            return Err(StorageError::internal(
                "KV expiration index has invalid marker bytes",
            ));
        }
        rows.push(decode_index_key(table_id, &entry.key)?);
    }
    Ok((rows, more))
}
