//! Protected pointer ownership reads and releases in the caller's KV attempt.

use std::num::NonZeroU32;

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::PointerOwner;

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::RETENTION_READ_BYTES,
    range::prefix_upper_bound,
    stream_write::key_layout::{
        decode_pointer_reference_key, pointer_owner_key, pointer_reference_prefix,
    },
    value_layout::logical_value_len,
};

pub(crate) fn pointer_reference_range(item_stream: &StreamName) -> StorageResult<KvRange> {
    let prefix = pointer_reference_prefix(item_stream)?;
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV pointer reference prefix has no upper bound"))?;
    // The zero ID encodes pointer ownership without an item-history hold.
    let mut unreferenced = prefix;
    unreferenced.extend_from_slice(StreamItemId::default().as_bytes());
    let begin = prefix_upper_bound(&unreferenced)
        .ok_or_else(|| StorageError::internal("KV pointer ownership prefix has no upper bound"))?;
    Ok(KvRange {
        entry_bounds: None,
        begin,
        end,
        reverse: false,
        limit: NonZeroU32::MIN,
        max_bytes: RETENTION_READ_BYTES,
    })
}

pub(crate) fn decode_first_pointer_reference(
    item_stream: &StreamName,
    page: crate::KvRangePage,
) -> StorageResult<Option<StreamItemId>> {
    let mut entries = page.entries.into_iter();
    let first = entries.next();
    if entries.next().is_some() || page.more && first.is_none() {
        return Err(StorageError::internal(
            "KV pointer reference read exceeded one row",
        ));
    }
    first
        .map(|entry| {
            let reference = decode_pointer_reference_key(&entry.key)?;
            if reference.item_stream != *item_stream || logical_value_len(&entry.value)? != 0 {
                return Err(StorageError::internal(
                    "KV pointer reference row differs from its key",
                ));
            }
            reference
                .item_item_id
                .ok_or_else(|| StorageError::internal("KV image hold has no item ID"))
        })
        .transpose()
}

pub(crate) async fn release_pointer_owners<A: KvAttempt>(
    attempt: &mut A,
    owner: PointerOwner,
    owners: &[(StreamName, StreamItemId)],
) -> StorageResult<Vec<KvMutation>> {
    let owner_keys = owners
        .iter()
        .map(|(stream, id)| pointer_owner_key(stream, *id))
        .collect::<StorageResult<Vec<_>>>()?;
    let reverse_keys = attempt
        .get_batch(owner_keys.clone(), ReadProtection::Conflict)
        .await?;
    if reverse_keys.len() != owner_keys.len() {
        return Err(StorageError::internal(
            "KV pointer owner batch count differs",
        ));
    }
    let reverse_keys = reverse_keys
        .into_iter()
        .zip(owners)
        .map(|(key, (_, id))| {
            let key =
                key.ok_or_else(|| StorageError::internal("KV stream pointer owner is absent"))?;
            crate::stream_write::key_layout::resolve_pointer_owner(key, *id)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let reverse_values = attempt
        .get_batch_admitted(
            reverse_keys.clone(),
            ReadProtection::Conflict,
            crate::KvValueAllowance::new(0, 0),
        )
        .await?;
    if reverse_values.len() != reverse_keys.len() {
        return Err(StorageError::internal(
            "KV pointer reference batch count differs",
        ));
    }
    let mut changes = Vec::with_capacity(owner_keys.len() * 2);
    for (((owner_key, (owner_stream, owner_item_id)), reverse_key), reverse_value) in owner_keys
        .into_iter()
        .zip(owners)
        .zip(reverse_keys)
        .zip(reverse_values)
    {
        let reference = decode_pointer_reference_key(&reverse_key)?;
        if reference.owner != owner
            || reference.owner_stream != *owner_stream
            || reference.owner_item_id != *owner_item_id
            || reverse_value.as_deref() != Some(&[])
        {
            return Err(StorageError::internal(
                "KV stream pointer owner differs from its reverse row",
            ));
        }
        changes.push(KvMutation::Delete { key: owner_key });
        changes.push(KvMutation::Delete { key: reverse_key });
    }
    Ok(changes)
}
