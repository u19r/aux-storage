//! Physical stream rows staged on the caller's native KV write attempt.

use storage_types::{StorageError, StorageResult, StreamItemId, storage_serde};
use stream_provider::{DynamoStreamWritePlan, StreamItem, StreamRowValue};

use super::{
    key_layout::{effect_key, item_row_key, pointer_owner_key, pointer_reference_key, record_key},
    record::KvSystemStreamRecord,
};
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
    let DynamoStreamWritePlan {
        rows,
        pointer_references,
        record,
        receipt,
    } = plan;
    if record.position != receipt.position {
        return Err(storage_types::StorageError::internal(
            "KV SystemStream record differs from its pointer position",
        ));
    }
    let mut changes = Vec::with_capacity(9);
    for row in rows {
        let key = item_row_key(row.value())?;
        changes.push(KvMutation::Put {
            key,
            value: StreamRowValue::from_planned(row)?.to_bytes()?,
        });
    }
    for reference in pointer_references {
        let reverse_key = pointer_reference_key(&reference)?;
        changes.push(KvMutation::Put {
            key: pointer_owner_key(&reference.owner_stream, reference.owner_item_id)?,
            value: reverse_key.clone(),
        });
        changes.push(KvMutation::Put {
            key: reverse_key,
            value: Vec::new(),
        });
    }
    changes.push(KvMutation::Put {
        key: record_key(&receipt.position)?,
        value: KvSystemStreamRecord {
            pointer_item_id: receipt.pointer_item_id,
            effect_id: receipt.effect_id.as_str().to_owned(),
            created_at: receipt.created_at,
            record,
        }
        .encode()?,
    });
    changes.push(KvMutation::Put {
        key: effect_key(receipt.effect_id.as_str()),
        value: storage_serde::to_bytes(&receipt)?,
    });
    Ok(changes)
}

/// Commit-addressed rows use the existing ID-free payload codec. Only the
/// receipt needs a stamped value because its lookup key is an effect digest.
pub(crate) fn stage_commit_ordered_dynamo<A: KvAttempt>(
    attempt: &mut A,
    plan: DynamoStreamWritePlan,
) -> StorageResult<()> {
    let stamp = plan
        .record
        .position
        .offset()
        .versionstamp()
        .ok_or_else(|| StorageError::internal("Missing pending native position"))?;
    let ordinal = [stamp[10], stamp[11]];
    let high_key = crate::stream_history::segmented_key(
        crate::constants::SYSTEM_STREAM_SEQUENCE_PREFIX,
        plan.record.position.shard_id().as_str(),
    )?;
    attempt.stage_versionstamped_value(high_key, stamp.to_vec(), 0)?;
    let mut changes = Vec::new();
    for (index, row) in plan.rows.into_iter().enumerate() {
        let key = item_row_key(row.value())?;
        let value = StreamRowValue::from_planned(row)?.to_bytes()?;
        if index == 0 {
            changes.push(KvMutation::Put { key, value });
        } else {
            attempt.stage_versionstamped_puts(vec![(
                key[..key.len() - 12].to_vec(),
                ordinal,
                value,
            )])?;
        }
    }
    for reference in plan.pointer_references {
        stage_commit_ordered_reference(attempt, &reference, ordinal)?;
    }
    let key = super::key_layout::record_prefix(&plan.record.position);
    let record = KvSystemStreamRecord {
        pointer_item_id: plan.receipt.pointer_item_id,
        effect_id: plan.receipt.effect_id.as_str().to_owned(),
        created_at: plan.receipt.created_at,
        record: plan.record,
    };
    attempt.stage_versionstamped_puts(vec![(key, ordinal, record.encode()?)])?;
    let key = effect_key(plan.receipt.effect_id.as_str());
    let mut value = b"\0vs/".to_vec();
    value.extend_from_slice(&stamp);
    value.extend_from_slice(&storage_serde::to_bytes(&plan.receipt)?);
    attempt.stage_versionstamped_value(key, value, 4)?;
    attempt.stage(changes)
}

fn stage_commit_ordered_reference<A: KvAttempt>(
    attempt: &mut A,
    reference: &stream_provider::StreamPointerReference,
    ordinal: [u8; 2],
) -> StorageResult<()> {
    let reverse_key = pointer_reference_key(reference)?;
    let owner_key = pointer_owner_key(&reference.owner_stream, reference.owner_item_id)?;
    attempt.stage_versionstamped_puts(vec![(
        owner_key[..owner_key.len() - 12].to_vec(),
        ordinal,
        reverse_key.clone(),
    )])?;
    attempt.stage_versionstamped_puts(vec![(
        reverse_key[..reverse_key.len() - 12].to_vec(),
        ordinal,
        Vec::new(),
    )])
}
