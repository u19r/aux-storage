//! Route one keyed append and stage its row and point-read index atomically.

use storage_common::partition::PartitionInfo;
use storage_types::{StorageError, StorageResult, StreamItemId};
use stream_provider::StreamRowValue;

use super::key_layout::{encode_route, index_prefix, row_prefix};
use crate::{KvAttempt, KvMutation, stream_write::counter::allocate_stream_item_id};

pub(crate) async fn append_partitioned_user_item<A: KvAttempt>(
    attempt: &mut A,
    partition: &PartitionInfo,
    row: StreamRowValue,
) -> StorageResult<[u8; 2]> {
    let (row_prefix, index_prefix, route) = partition_prefixes(partition, &row)?;
    let encoded = row.to_bytes()?;
    let user_bytes = [0, 0];
    attempt.stage_versionstamped_puts(vec![
        (row_prefix, user_bytes, encoded),
        (index_prefix, user_bytes, route.to_vec()),
    ])?;
    Ok(user_bytes)
}

pub(crate) async fn append_attempt_allocated_partitioned_user_item<A: KvAttempt>(
    attempt: &mut A,
    partition: &PartitionInfo,
    row: StreamRowValue,
) -> StorageResult<StreamItemId> {
    let stream_name = row
        .stream_name()
        .ok_or_else(|| StorageError::internal("partitioned stream row has no stream name"))?;
    let (mut row_key, mut index_key, route) = partition_prefixes(partition, &row)?;
    let id = allocate_stream_item_id(attempt, stream_name).await?;
    row_key.extend_from_slice(id.as_bytes());
    index_key.extend_from_slice(id.as_bytes());
    attempt.stage(vec![
        KvMutation::Put {
            key: row_key,
            value: row.to_bytes()?,
        },
        KvMutation::Put {
            key: index_key,
            value: route.to_vec(),
        },
    ])?;
    Ok(id)
}

fn partition_prefixes(
    partition: &PartitionInfo,
    row: &StreamRowValue,
) -> StorageResult<(Vec<u8>, Vec<u8>, [u8; 4])> {
    let stream_name = row
        .stream_name()
        .ok_or_else(|| StorageError::internal("partitioned stream row has no stream name"))?;
    let row_prefix = row_prefix(
        stream_name,
        partition.placement_slot,
        partition.partition_id,
    )?;
    let index_prefix = index_prefix(stream_name)?;
    Ok((
        row_prefix,
        index_prefix,
        encode_route(partition.placement_slot, partition.partition_id),
    ))
}
