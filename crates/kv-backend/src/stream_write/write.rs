//! Physical stream rows staged on the caller's native KV write attempt.

use storage_types::{StorageError, StorageResult, StreamItemId, storage_serde};
use stream_provider::{DynamoStreamWritePlan, StreamItem, StreamRowValue};

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
    plan: DynamoStreamWritePlan,
) -> StorageResult<Vec<KvMutation>> {
    let mut changes = Vec::with_capacity(4);
    for row in plan.rows {
        changes.push(KvMutation::Put {
            key: item_row_key(row.value())?,
            value: StreamRowValue::from_planned(row)?.to_bytes()?,
        });
    }
    let history = plan
        .record
        .table_history_id()
        .ok_or_else(|| StorageError::internal("New system history has no target"))?;
    changes.push(KvMutation::Put {
        key: super::key_layout::table_record_key(history, plan.record.position())?,
        value: Vec::new(),
    });
    changes.push(KvMutation::Put {
        key: record_key(plan.record.position())?,
        value: storage_serde::to_bytes(&plan.record)?,
    });
    Ok(changes)
}

pub(crate) fn stage_commit_ordered_dynamo<A: KvAttempt>(
    attempt: &mut A,
    plan: DynamoStreamWritePlan,
) -> StorageResult<()> {
    let position = plan.record.position();
    let stamp = position
        .offset()
        .versionstamp()
        .ok_or_else(|| StorageError::internal("Missing pending native position"))?;
    let ordinal = [stamp[10], stamp[11]];
    let high_key = crate::stream_history::segmented_key(
        crate::constants::SYSTEM_STREAM_SEQUENCE_PREFIX,
        position.shard_id().as_str(),
    )?;
    attempt.stage_versionstamped_value(high_key, stamp.to_vec(), 0)?;
    for (index, row) in plan.rows.into_iter().enumerate() {
        let key = item_row_key(row.value())?;
        let row = StreamRowValue::from_planned(row)?;
        let offset = row.payload_offset()? + storage_types::ItemHistoryRecord::VERSIONSTAMP_OFFSET;
        let value = row.to_bytes()?;
        if index == 0 {
            attempt.stage_versionstamped_value(key, value, offset)?;
        } else {
            attempt.stage_versionstamped_puts(vec![(
                key[..key.len() - 12].to_vec(),
                ordinal,
                value,
            )])?;
        }
    }
    let history = plan
        .record
        .table_history_id()
        .ok_or_else(|| StorageError::internal("New system history has no target"))?;
    let index = super::key_layout::table_record_key(history, position)?;
    attempt.stage_versionstamped_puts(vec![
        (
            super::key_layout::record_prefix(position),
            ordinal,
            storage_serde::to_bytes(&plan.record)?,
        ),
        (index[..index.len() - 12].to_vec(), ordinal, Vec::new()),
    ])
}
