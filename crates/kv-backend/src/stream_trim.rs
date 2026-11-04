//! Bounded ordered SystemStream record scans on the caller's KV attempt.

use std::{future::Future, num::NonZeroU32};

use storage_types::{
    ReadProtection, StorageError, StorageResult, SystemStreamPosition, storage_serde,
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
    stream_write::key_layout::record_key,
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
    let mut frontiers = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut mutations = Vec::with_capacity(selected.len() * 3);
    for record in selected {
        let key = crate::stream_write::key_layout::retired_key(&record.position);
        let offset = match record.position.offset().versionstamp() {
            Some(stamp) => stamp.to_vec(),
            None => record
                .position
                .offset()
                .order()
                .map_err(|_| invalid_record())?
                .to_be_bytes()
                .to_vec(),
        };
        frontiers
            .entry(key)
            .and_modify(|current| {
                if *current < offset {
                    *current = offset.clone();
                }
            })
            .or_insert(offset);
        mutations.push(KvMutation::Delete {
            key: record_key(&record.position)?,
        });
        if let Some(history) = record.table_history_id {
            mutations.push(KvMutation::Delete {
                key: crate::stream_write::key_layout::table_record_key(history, &record.position)?,
            });
        }
    }
    store_retired_offsets(attempt, frontiers, &mut mutations).await?;
    attempt.stage(mutations)?;
    write_receipt(attempt, receipt)
}

async fn store_retired_offsets<A: KvAttempt>(
    attempt: &A,
    frontiers: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    mutations: &mut Vec<KvMutation>,
) -> StorageResult<()> {
    if frontiers.is_empty() {
        return Ok(());
    }
    let values = attempt
        .get_batch(
            frontiers.keys().cloned().collect(),
            ReadProtection::Conflict,
        )
        .await?;
    if values.len() != frontiers.len() {
        return Err(invalid_record());
    }
    for ((key, next), current) in frontiers.into_iter().zip(values) {
        if current
            .as_ref()
            .is_some_and(|current| current.len() != next.len())
        {
            return Err(invalid_record());
        }
        if current.as_ref().is_none_or(|current| *current < next) {
            mutations.push(KvMutation::Put { key, value: next });
        }
    }
    Ok(())
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
            let record = crate::stream_write::record::decode_record(&entry.key, &entry.value)?;
            Ok(SystemStreamTrimRecord {
                position: record.position().clone(),
                created_at: record.created_at(),
                table_history_id: record.table_history_id(),
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok((records, more))
}
