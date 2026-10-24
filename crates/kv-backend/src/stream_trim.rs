//! Bounded ordered SystemStream record scans on the caller's KV attempt.

use std::{future::Future, num::NonZeroU32};

use storage_types::{
    ReadProtection, StorageError, StorageResult, StreamName, SystemStreamEffectDigest,
    SystemStreamPosition, storage_serde,
};
use stream_provider::{
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SystemStreamTrimAttempt, SystemStreamTrimOperationId,
    SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest, SystemStreamTrimRecord,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    catalog::KvCatalogAttempt,
    constants::{
        STREAM_READ_BYTES, SYSTEM_STREAM_RECORD_PREFIX, SYSTEM_STREAM_TRIM_RECEIPT_PREFIX,
    },
    range::prefix_upper_bound,
    stream_read::row::decode_stored_stream_row,
    stream_write::{
        key_layout::{
            decode_pointer_reference_key, effect_key, item_key, pointer_owner_key, record_key,
        },
        record::KvSystemStreamRecord,
    },
};

impl<A: KvAttempt> SystemStreamTrimAttempt for KvCatalogAttempt<'_, A> {
    fn read_trim_receipts(
        &mut self,
        request: &SystemStreamTrimPageRequest,
    ) -> impl Future<
        Output = StorageResult<(
            Option<SystemStreamTrimPageReceipt>,
            Option<SystemStreamTrimPageReceipt>,
        )>,
    > + Send {
        read_receipts(self.attempt, request)
    }

    fn load_trim_boundaries(
        &mut self,
        request: &SystemStreamTrimPageRequest,
    ) -> impl Future<Output = StorageResult<Vec<SystemStreamPosition>>> + Send {
        crate::stream_history::load_trim_boundaries(self.attempt, request)
    }

    fn scan_trim_records(
        &mut self,
        after: Option<&SystemStreamPosition>,
    ) -> impl Future<Output = StorageResult<(Vec<SystemStreamTrimRecord>, bool)>> + Send {
        scan_records(self.attempt, after)
    }

    fn apply_trim_page(
        &mut self,
        selected: &[SystemStreamTrimRecord],
        receipt: &SystemStreamTrimPageReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        apply_page(self.attempt, selected, receipt)
    }
}

async fn apply_page<A: KvAttempt>(
    attempt: &mut A,
    selected: &[SystemStreamTrimRecord],
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    if selected.is_empty() {
        return write_receipt(attempt, receipt);
    }
    let system_stream = StreamName::system_table_stream();
    let item_prefix = crate::stream_write::key_layout::item_row_prefix(&system_stream)?;
    let mut keys = Vec::with_capacity(selected.len() * 4);
    for record in selected {
        keys.push(effect_key(record.effect_id.as_str()));
        keys.push(record_key(&record.position)?);
        keys.push(item_key(&system_stream, record.pointer_item_id)?);
        keys.push(pointer_owner_key(&system_stream, record.pointer_item_id)?);
    }
    let values = attempt
        .get_batch(keys.clone(), ReadProtection::Conflict)
        .await?;
    if values.len() != keys.len() {
        return Err(StorageError::internal(
            "KV SystemStream trim observation count differs",
        ));
    }
    let mut values = values.into_iter();
    let mut descriptors = Vec::with_capacity(selected.len());
    let mut pointers = Vec::with_capacity(selected.len());
    let mut observations = Vec::with_capacity(selected.len());
    for (record, keys) in selected.iter().zip(keys.as_chunks::<4>().0.iter()) {
        let effect = values
            .next()
            .ok_or_else(invalid_record)?
            .ok_or_else(invalid_record)?;
        let stored = values
            .next()
            .ok_or_else(invalid_record)?
            .ok_or_else(invalid_record)?;
        let pointer = values
            .next()
            .ok_or_else(invalid_record)?
            .ok_or_else(invalid_record)?;
        let reverse_key = values
            .next()
            .ok_or_else(invalid_record)?
            .ok_or_else(invalid_record)?;
        let effect = crate::stream_write::record::decode_effect(&effect)?;
        let stored = KvSystemStreamRecord::decode(&keys[1], &stored)?;
        if stored.record.position != record.position
            || stored.pointer_item_id != record.pointer_item_id
            || stored.effect_id != record.effect_id.as_str()
            || stored.created_at != record.created_at
        {
            return Err(invalid_record());
        }
        let pointer =
            decode_stored_stream_row(&item_prefix, crate::KvEntry::new(keys[2].clone(), pointer))?;
        pointer.require_address(&system_stream, record.pointer_item_id)?;
        pointer.require_pointer_effect(&record.effect_id)?;
        let reverse_key = crate::stream_write::key_layout::resolve_pointer_owner(
            reverse_key,
            record.pointer_item_id,
        )?;
        let reference = decode_pointer_reference_key(&reverse_key)?;
        if stored.record.history() != effect.history_id {
            return Err(invalid_record());
        }
        descriptors.push(stored.record);
        pointers.push(pointer);
        observations.push((effect, reference, reverse_key));
    }
    let mut catalog = KvCatalogAttempt::new(attempt, storage_types::ReadProtection::Conflict);
    stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::admit_trim_work(
        &descriptors,
        &pointers,
        STREAM_READ_BYTES.get(),
    )?;
    let mut retiring =
        stream_provider::retained_image::stream_row::StoredStreamRow::retirement_receipts(
            &pointers,
        )?;
    retiring.extend(stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::retirement_receipts(&descriptors)?);
    let pointers = stream_provider::retained_image::stream_row::StoredStreamRow::hydrate(
        &mut catalog,
        pointers,
        STREAM_READ_BYTES.get(),
    )
    .await?;
    let retained =
        stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::hydrate(
            &mut catalog,
            descriptors,
            STREAM_READ_BYTES.get(),
        )
        .await?;
    if retained.len() != selected.len() || pointers.len() != selected.len() {
        return Err(invalid_record());
    }
    stream_provider::retained_image::RetainedOwnerRecord::release_stream_batch(
        &mut catalog,
        retiring,
    )
    .await?;
    let mut mutations = Vec::with_capacity(selected.len() * 5);
    let mut reverse_keys = Vec::with_capacity(selected.len());
    for ((((record, keys), (effect, reference, reverse_key)), logical), pointer) in selected
        .iter()
        .zip(keys.as_chunks::<4>().0.iter())
        .zip(observations)
        .zip(retained)
        .zip(pointers)
    {
        let trimmed = effect.accept_trim(record, &logical, &pointer, &reference)?;
        mutations.push(KvMutation::Put {
            key: keys[0].clone(),
            value: storage_serde::to_bytes(&trimmed)?,
        });
        if let Some(stamp) = record.position.offset().versionstamp() {
            let mut value = stamp.to_vec();
            value.push(0); // Existing inline physical value tag.
            attempt.native_stage_byte_max(
                crate::stream_write::key_layout::retired_key(&record.position),
                value,
            )?;
        }
        mutations.push(KvMutation::Delete {
            key: keys[1].clone(),
        });
        mutations.push(KvMutation::Delete {
            key: keys[2].clone(),
        });
        mutations.push(KvMutation::Delete {
            key: keys[3].clone(),
        });
        reverse_keys.push(reverse_key);
    }
    let reverse_values = attempt
        .get_batch_admitted(
            reverse_keys.clone(),
            ReadProtection::Conflict,
            crate::KvValueAllowance::new(0, 0),
        )
        .await?;
    if reverse_values.len() != reverse_keys.len() {
        return Err(invalid_record());
    }
    for (key, value) in reverse_keys.into_iter().zip(reverse_values) {
        if value.as_deref() != Some(&[]) {
            return Err(invalid_record());
        }
        mutations.push(KvMutation::Delete { key });
    }
    attempt.stage(mutations)?;
    write_receipt(attempt, receipt)
}

pub(crate) async fn read_receipts<A: KvAttempt>(
    attempt: &mut A,
    request: &SystemStreamTrimPageRequest,
) -> StorageResult<(
    Option<SystemStreamTrimPageReceipt>,
    Option<SystemStreamTrimPageReceipt>,
)> {
    let sequence = request.continuation().map_or(0, |cursor| cursor.sequence());
    let mut keys = vec![receipt_key(request.operation_id(), sequence)?];
    if let Some(previous) = sequence.checked_sub(1) {
        keys.push(receipt_key(request.operation_id(), previous)?);
    }
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != if sequence == 0 { 1 } else { 2 } {
        return Err(StorageError::internal(
            "KV SystemStream trim receipt count differs",
        ));
    }
    let mut values = values.into_iter();
    let current = decode_receipt(
        values.next().ok_or_else(invalid_receipt)?,
        request,
        sequence,
    )?;
    let predecessor = if sequence == 0 {
        None
    } else {
        decode_receipt(
            values.next().ok_or_else(invalid_receipt)?,
            request,
            sequence - 1,
        )?
    };
    Ok((current, predecessor))
}

pub(crate) fn write_receipt<A: KvAttempt>(
    attempt: &mut A,
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    attempt.stage(vec![KvMutation::Put {
        key: receipt_key(&receipt.operation_id, receipt.input_sequence)?,
        value: storage_serde::to_bytes(receipt)?,
    }])
}

fn receipt_key(operation: &SystemStreamTrimOperationId, sequence: u64) -> StorageResult<Vec<u8>> {
    let operation = storage_serde::to_bytes(operation)?;
    let length = u16::try_from(operation.len()).map_err(|_| invalid_receipt())?;
    let mut key = SYSTEM_STREAM_TRIM_RECEIPT_PREFIX.to_vec();
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(&operation);
    key.extend_from_slice(&sequence.to_be_bytes());
    Ok(key)
}

fn decode_receipt(
    bytes: Option<Vec<u8>>,
    request: &SystemStreamTrimPageRequest,
    sequence: u64,
) -> StorageResult<Option<SystemStreamTrimPageReceipt>> {
    bytes
        .map(|bytes| {
            let receipt: SystemStreamTrimPageReceipt = storage_serde::from_bytes(&bytes)?;
            if receipt.operation_id != *request.operation_id() || receipt.input_sequence != sequence
            {
                return Err(invalid_receipt());
            }
            Ok(receipt)
        })
        .transpose()
}

fn invalid_receipt() -> StorageError {
    StorageError::internal("KV SystemStream trim receipt is invalid")
}

fn invalid_record() -> StorageError {
    StorageError::internal("KV SystemStream trim record is invalid")
}

pub(crate) async fn scan_records<A: KvAttempt>(
    attempt: &mut A,
    after: Option<&SystemStreamPosition>,
) -> StorageResult<(Vec<SystemStreamTrimRecord>, bool)> {
    let prefix = SYSTEM_STREAM_RECORD_PREFIX;
    let begin = if let Some(position) = after {
        let mut key = record_key(position)?;
        key.push(0);
        key
    } else {
        prefix.to_vec()
    };
    let end = prefix_upper_bound(prefix).ok_or_else(|| {
        StorageError::internal("KV SystemStream record prefix has no upper bound")
    })?;
    let page = attempt
        .scan(
            KvRange {
                entry_bounds: None,
                begin,
                end,
                reverse: false,
                limit: NonZeroU32::new(
                    u32::try_from(MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS + 1).map_err(|_| {
                        StorageError::internal("SystemStream trim scan limit is invalid")
                    })?,
                )
                .ok_or_else(|| StorageError::internal("SystemStream trim scan limit is zero"))?,
                max_bytes: STREAM_READ_BYTES,
            },
            ReadProtection::Conflict,
        )
        .await?;
    let more = page.more || page.entries.len() > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS;
    let records = page
        .entries
        .into_iter()
        .take(MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS)
        .map(|entry| {
            let row = KvSystemStreamRecord::decode(&entry.key, &entry.value)?;
            if record_key(&row.record.position)? != entry.key {
                return Err(StorageError::internal(
                    "KV SystemStream record key differs from value",
                ));
            }
            let length = row.record.read_work_bytes(STREAM_READ_BYTES.get())?;
            Ok((
                length,
                SystemStreamTrimRecord {
                    position: row.record.position,
                    pointer_item_id: row.pointer_item_id,
                    effect_id: SystemStreamEffectDigest::try_new(row.effect_id).map_err(|_| {
                        StorageError::internal("KV SystemStream effect ID is invalid")
                    })?,
                    created_at: row.created_at,
                },
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let system_stream = StreamName::system_table_stream();
    let prefix = crate::stream_write::key_layout::item_row_prefix(&system_stream)?;
    let keys = records
        .iter()
        .map(|(_, record)| item_key(&system_stream, record.pointer_item_id))
        .collect::<StorageResult<Vec<_>>>()?;
    let values = attempt
        .get_batch(keys.clone(), ReadProtection::Conflict)
        .await?;
    if values.len() != keys.len() {
        return Err(invalid_record());
    }
    let pointers = keys
        .into_iter()
        .zip(values)
        .map(|(key, value)| {
            decode_stored_stream_row(
                &prefix,
                crate::KvEntry::new(key, value.ok_or_else(invalid_record)?),
            )
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let (selected, excluded) = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::select_trim_prefix(records, pointers, STREAM_READ_BYTES.get())?;
    Ok((selected, more || excluded))
}
