//! Physical stream rows staged on the caller's native KV write attempt.

use storage_types::{StorageError, StorageResult, StreamItemId};
use stream_provider::{StreamItem, StreamRowValue};

use super::key_layout::{item_row_key, record_key};
use crate::{KvAttempt, KvMutation};

pub(crate) async fn append_attempt_allocated_user_item<A: KvAttempt>(
    attempt: &mut A,
    row: StreamRowValue,
) -> StorageResult<StreamItemId> {
    let stream = row
        .stream_name()
        .ok_or_else(|| StorageError::internal("sequenced user stream row has no stream name"))?;
    let id = super::counter::allocate_stream_item_id(attempt, stream).await?;
    store_user_item(attempt, row.into_item(id))?;
    Ok(id)
}

pub(crate) fn store_user_item<A: KvAttempt>(attempt: &mut A, row: StreamItem) -> StorageResult<()> {
    let key = item_row_key(&row)?;
    attempt.stage(vec![KvMutation::Put {
        key,
        value: StreamRowValue::from(row).to_bytes()?,
    }])
}

pub(crate) fn append_versionstamped_user_item<A: KvAttempt>(
    attempt: &mut A,
    row: StreamRowValue,
) -> StorageResult<[u8; 2]> {
    let stream_name = row.stream_name().ok_or_else(|| {
        StorageError::internal("versionstamped user stream row has no stream name")
    })?;
    let prefix = super::key_layout::item_row_prefix(stream_name)?;
    let encoded = row.to_bytes()?;
    let user_bytes = [0, 0];
    attempt.stage_versionstamped_puts(vec![(prefix, user_bytes, encoded)])?;
    Ok(user_bytes)
}

pub(crate) fn dynamo_stream_mutations(
    (history_id, position): (
        storage_types::TableHistoryId,
        &storage_types::SystemStreamPosition,
    ),
    created_at: storage_types::TimestampMillis,
    value: Vec<u8>,
) -> StorageResult<Vec<KvMutation>> {
    let reference = super::record::FeedReference::encode(history_id, created_at, &value)?;
    Ok(vec![
        KvMutation::Put {
            key: super::key_layout::entry_key(history_id, position)?,
            value,
        },
        KvMutation::Put {
            key: record_key(position)?,
            value: reference,
        },
    ])
}

pub(crate) fn stage_commit_ordered_dynamo<A: KvAttempt>(
    attempt: &mut A,
    (history_id, position): (
        storage_types::TableHistoryId,
        &storage_types::SystemStreamPosition,
    ),
    created_at: storage_types::TimestampMillis,
    value: Vec<u8>,
) -> StorageResult<()> {
    let stamp = position
        .offset()
        .versionstamp()
        .ok_or_else(|| StorageError::internal("Missing pending native position"))?;
    let ordinal = [stamp[10], stamp[11]];
    let reference = super::record::FeedReference::encode(history_id, created_at, &value)?;
    attempt.stage_versionstamped_puts(vec![
        (
            super::key_layout::entry_prefix(history_id, position)?,
            ordinal,
            value,
        ),
        (
            super::key_layout::record_prefix(position),
            ordinal,
            reference,
        ),
    ])
}
