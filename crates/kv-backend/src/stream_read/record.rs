//! Batched point reads reuse the original receipt and versionstamp codecs.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, SystemStreamPosition};
use stream_provider::{StreamItem, StreamPointerReference};

use super::row;
use crate::{KvAttempt, KvEntry, ReadProtection, stream_write::key_layout};

pub(crate) async fn load_stream_effects<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    effects: &[storage_types::SystemStreamEffectDigest],
) -> StorageResult<Vec<Option<stream_provider::DynamoStreamEffectRow>>> {
    let keys = effects
        .iter()
        .map(|effect| key_layout::effect_key(effect.as_str()))
        .collect();
    load_point_rows(attempt, keys, protection, |_, bytes| {
        crate::stream_write::record::decode_effect(&bytes)
    })
    .await
}

pub(crate) async fn load_stream_records<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    positions: &[SystemStreamPosition],
) -> StorageResult<
    Vec<
        Option<stream_provider::retained_image::verified_system_stream::VerifiedSystemStreamRecord>,
    >,
> {
    let keys = positions
        .iter()
        .map(key_layout::record_key)
        .collect::<StorageResult<Vec<_>>>()?;
    let records = load_point_rows(attempt, keys, protection, |index, value| {
        let entry = KvEntry {
            key: key_layout::record_key(&positions[index])?,
            value,
        };
        row::decode_history_row(entry).map(|(record, _)| record)
    })
    .await?;
    let mut catalog = crate::catalog::KvCatalogAttempt::new(attempt, protection);
    stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::hydrate_optional(
        &mut catalog,
        records,
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await
}

pub(crate) async fn load_stream_items<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    addresses: &[(&StreamName, StreamItemId)],
) -> StorageResult<Vec<Option<StreamItem>>> {
    let keys = addresses
        .iter()
        .map(|(stream, id)| key_layout::item_key(stream, *id))
        .collect::<StorageResult<Vec<_>>>()?;
    let rows = load_point_rows(attempt, keys, protection, |index, bytes| {
        let (stream, id) = &addresses[index];
        let row = stream_provider::retained_image::stream_row::StoredStreamRow::decode(
            *id,
            &bytes,
            crate::constants::STREAM_READ_BYTES.get(),
        )?;
        row.require_address(stream, *id)?;
        Ok(row)
    })
    .await?;
    let mut catalog = crate::catalog::KvCatalogAttempt::new(attempt, protection);
    stream_provider::retained_image::stream_row::StoredStreamRow::hydrate_optional(
        &mut catalog,
        rows,
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await
}

pub(crate) async fn load_stream_references<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    addresses: &[(&StreamName, StreamItemId)],
) -> StorageResult<Vec<Option<StreamPointerReference>>> {
    let keys = addresses
        .iter()
        .map(|(stream, id)| key_layout::pointer_owner_key(stream, *id))
        .collect::<StorageResult<Vec<_>>>()?;
    load_point_rows(attempt, keys, protection, |index, bytes| {
        let reverse = key_layout::resolve_pointer_owner(bytes, addresses[index].1)?;
        key_layout::decode_pointer_reference_key(&reverse)
    })
    .await
}

async fn load_point_rows<A: KvAttempt, T>(
    attempt: &mut A,
    keys: Vec<Vec<u8>>,
    protection: ReadProtection,
    mut decode: impl FnMut(usize, Vec<u8>) -> StorageResult<T> + Send,
) -> StorageResult<Vec<Option<T>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let count = keys.len();
    let rows = attempt.get_batch(keys, protection).await?;
    if rows.len() != count {
        return Err(StorageError::internal(
            "KV stream point batch count differs",
        ));
    }
    rows.into_iter()
        .enumerate()
        .map(|(index, value)| value.map(|bytes| decode(index, bytes)).transpose())
        .collect()
}
