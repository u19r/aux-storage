//! Bounded ordered SystemStream record scans on the caller's KV attempt.

use std::num::NonZeroU32;

use storage_types::{
    ReadProtection, StorageError, StorageResult, SystemStreamPosition, storage_serde,
};
use stream_provider::{
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SystemStreamTrimOperationId, SystemStreamTrimPageReceipt,
    SystemStreamTrimRecord,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{STREAM_READ_BYTES, SYSTEM_STREAM_TRIM_RECEIPT_PREFIX},
    range::prefix_upper_bound,
    stream_write::key_layout::record_key,
};

pub(crate) async fn apply_page<A: KvAttempt>(
    attempt: &mut A,
    selected: &[SystemStreamTrimRecord],
    frontiers: &[SystemStreamPosition],
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    let mut mutations = Vec::with_capacity(selected.len() + frontiers.len() + 1);
    for record in selected {
        mutations.push(KvMutation::Delete {
            key: record_key(&record.position)?,
        });
    }
    // Selection starts strictly after the frontiers observed in this same
    // protected attempt, so each lane's maximum advances its marker.
    for position in frontiers {
        let offset = match position.offset().versionstamp() {
            Some(stamp) => stamp.to_vec(),
            None => position
                .offset()
                .order()
                .map_err(|_| invalid_record())?
                .to_be_bytes()
                .to_vec(),
        };
        mutations.push(KvMutation::Put {
            key: crate::stream_write::key_layout::retired_key(position),
            value: offset,
        });
    }
    attempt.stage(mutations)?;
    write_receipt(attempt, receipt)
}

pub(crate) async fn load_receipts<A: KvAttempt>(
    attempt: &mut A,
    operation: &SystemStreamTrimOperationId,
    sequences: &[u64],
) -> StorageResult<storage_types::Exact<Option<SystemStreamTrimPageReceipt>>> {
    let keys = sequences
        .iter()
        .map(|sequence| receipt_key(operation, *sequence))
        .collect::<StorageResult<Vec<_>>>()?;
    let values = attempt
        .get_batch_exact(keys, ReadProtection::Conflict)
        .await?;
    values.try_map(|bytes| {
        bytes
            .map(|bytes| storage_serde::from_bytes(&bytes))
            .transpose()
    })
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

fn invalid_receipt() -> StorageError {
    StorageError::internal("KV SystemStream trim receipt is invalid")
}

fn invalid_record() -> StorageError {
    StorageError::internal("KV SystemStream trim record is invalid")
}

/// A snapshot scan, so appends at the live tail do not conflict with the
/// page. Records are written once under a unique position and never rewritten,
/// so a delete needs no conflict read of its key. Selection takes only records
/// at or below the publication boundary of this attempt's own read version, and
/// every such record is visible to the same snapshot, so the retired frontier
/// never passes a record the page did not see. Concurrent trims conflict on the
/// retired frontier keys, which the attempt reads under conflict.
pub(crate) async fn scan_records<A: KvAttempt>(
    attempt: &mut A,
    starts: &[SystemStreamPosition],
) -> StorageResult<(Vec<SystemStreamTrimRecord>, bool)> {
    let mut records = Vec::new();
    for start in starts {
        let prefix = crate::stream_write::key_layout::record_prefix(start);
        let mut begin = record_key(start)?;
        begin.push(0);
        let end = prefix_upper_bound(&prefix).ok_or_else(invalid_record)?;
        let remaining = MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS + 1 - records.len();
        let page = attempt
            .scan(
                KvRange {
                    entry_bounds: None,
                    begin,
                    end,
                    reverse: false,
                    limit: NonZeroU32::new(u32::try_from(remaining).map_err(|_| invalid_record())?)
                        .ok_or_else(invalid_record)?,
                    max_bytes: STREAM_READ_BYTES,
                },
                ReadProtection::Snapshot,
            )
            .await?;
        let more = page.more;
        for entry in page.entries {
            let position = crate::stream_write::record::decode_position(start, &entry.key)?;
            let reference = crate::stream_write::record::FeedReference::decode(&entry.value)?;
            records.push(SystemStreamTrimRecord {
                position,
                created_at: reference.created_at,
            });
        }
        if more || records.len() > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS {
            records.truncate(MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS);
            return Ok((records, true));
        }
    }
    Ok((records, false))
}
