//! One ordered queue message layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use queue_provider::{
    MESSAGE_ID_VERSIONSTAMP_LEN, MessageId, QueueCleanupPage, QueueCleanupRequest, QueueDueRequest,
    QueueIncarnation, QueueMessageCounts, QueueMessageRecord, ReceiptHandle,
};
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{DUE_PREFIX, MESSAGE_PREFIX, MESSAGE_READ_BYTES, RECEIPT_PREFIX},
    range::prefix_upper_bound,
};

pub(crate) async fn read_due_messages<A: KvAttempt>(
    native: &mut A,
    request: &QueueDueRequest,
) -> StorageResult<Vec<QueueMessageRecord>> {
    let prefix = [DUE_PREFIX, request.incarnation.as_bytes()].concat();
    let end_prefix = [prefix.as_slice(), &ordered_timestamp(request.captured_at)].concat();
    let end = prefix_upper_bound(&end_prefix)
        .ok_or_else(|| StorageError::internal("KV queue due upper bound overflowed"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin: prefix.clone(),
                end,
                reverse: false,
                limit: request.limit,
                max_bytes: MESSAGE_READ_BYTES,
            },
            ReadProtection::Conflict,
        )
        .await?;
    let selected = page
        .entries
        .into_iter()
        .map(|entry| {
            let (due, id) = decode_due_key(&prefix, &entry.key)?;
            let (claimed, expires) = decode_due_value(&entry.value)?;
            Ok((due, id, claimed, expires))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let keys = selected
        .iter()
        .map(|(_, id, _, _)| message_key(request.incarnation, *id))
        .collect();
    let values = native.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != selected.len() {
        return Err(StorageError::internal(
            "KV queue due message batch count differs",
        ));
    }
    let mut result = Vec::with_capacity(selected.len());
    for ((due, id, claimed, expires), bytes) in selected.into_iter().zip(values) {
        let bytes =
            bytes.ok_or_else(|| StorageError::internal("KV queue due index has no message"))?;
        let record: QueueMessageRecord = storage_serde::from_bytes(&bytes)?;
        if record.incarnation != request.incarnation
            || record.message.message_id != id
            || record.visible_at() != due
            || record.expires_at != expires
            || record.message.receipt_handle.is_some() != claimed
        {
            return Err(StorageError::internal(
                "KV queue due index differs from message",
            ));
        }
        result.push(record);
    }
    Ok(result)
}

pub(crate) async fn read_messages_by_receipt<A: KvAttempt>(
    native: &mut A,
    incarnation: QueueIncarnation,
    receipts: Vec<ReceiptHandle>,
) -> StorageResult<Vec<Option<QueueMessageRecord>>> {
    let values = native
        .get_batch(
            receipts
                .iter()
                .map(|receipt| receipt_key(incarnation, receipt))
                .collect(),
            ReadProtection::Conflict,
        )
        .await?;
    if values.len() != receipts.len() {
        return Err(StorageError::internal(
            "KV queue receipt batch count differs",
        ));
    }
    let ids = values
        .into_iter()
        .map(|value| {
            value
                .map(|bytes| {
                    let id: [u8; MESSAGE_ID_VERSIONSTAMP_LEN] = bytes.try_into().map_err(|_| {
                        StorageError::internal("KV queue receipt index has invalid ID")
                    })?;
                    Ok(MessageId::from_bytes(id))
                })
                .transpose()
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let rows = native
        .get_batch(
            ids.iter()
                .flatten()
                .map(|id| message_key(incarnation, *id))
                .collect(),
            ReadProtection::Conflict,
        )
        .await?;
    let mut rows = rows.into_iter();
    ids.into_iter()
        .zip(receipts)
        .map(|(id, receipt)| {
            let Some(id) = id else {
                return Ok(None);
            };
            let bytes = rows
                .next()
                .flatten()
                .ok_or_else(|| StorageError::internal("KV queue receipt index has no message"))?;
            let record: QueueMessageRecord = storage_serde::from_bytes(&bytes)?;
            if record.incarnation != incarnation
                || record.message.message_id != id
                || record
                    .message
                    .receipt_handle
                    .as_ref()
                    .map(|handle| handle.0.as_str())
                    != Some(receipt.0.as_str())
            {
                return Err(StorageError::internal(
                    "KV queue receipt index differs from message",
                ));
            }
            Ok(Some(record))
        })
        .collect()
}

pub(crate) async fn read_messages_by_id<A: KvAttempt>(
    native: &mut A,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
) -> StorageResult<Vec<Option<QueueMessageRecord>>> {
    let keys = ids
        .into_iter()
        .map(|id| message_key(incarnation, id))
        .collect::<Vec<_>>();
    let values = native
        .get_batch(keys.clone(), ReadProtection::Conflict)
        .await?;
    if values.len() != keys.len() {
        return Err(StorageError::internal(
            "KV queue message batch count differs",
        ));
    }
    keys.into_iter()
        .zip(values)
        .map(|(key, value)| {
            value
                .map(|value| decode_message_entry(crate::KvEntry { key, value }))
                .transpose()
        })
        .collect()
}

pub(crate) async fn write_message_records<A: KvAttempt>(
    native: &mut A,
    records: Vec<(QueueMessageRecord, Option<QueueMessageRecord>)>,
) -> StorageResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let keys = records
        .iter()
        .map(|(record, _)| message_key(record.incarnation, record.message.message_id))
        .collect::<Vec<_>>();
    if keys
        .iter()
        .enumerate()
        .any(|(index, key)| keys[..index].contains(key))
    {
        return Err(StorageEnum::TransactionConflict {
            message: "KV queue write batch repeats a message key".to_owned(),
        }
        .into());
    }
    let current = native.get_batch(keys, ReadProtection::Conflict).await?;
    if current.len() != records.len() {
        return Err(StorageError::internal(
            "KV queue write batch read count differs",
        ));
    }
    let mut changes = Vec::new();
    for ((record, previous), current) in records.into_iter().zip(current) {
        if current != previous.as_ref().map(storage_serde::to_bytes).transpose()? {
            return Err(StorageEnum::TransactionConflict {
                message: "KV queue message changed during write".to_owned(),
            }
            .into());
        }
        changes.extend(write_message_mutations(record, previous)?);
    }
    native.stage(changes)
}

fn write_message_mutations(
    record: QueueMessageRecord,
    previous: Option<QueueMessageRecord>,
) -> StorageResult<Vec<KvMutation>> {
    let key = message_key(record.incarnation, record.message.message_id);
    let mut changes = Vec::with_capacity(5);
    if let Some(previous) = previous {
        changes.push(KvMutation::Delete {
            key: due_key(&previous),
        });
        if let Some(receipt) = &previous.message.receipt_handle {
            changes.push(KvMutation::Delete {
                key: receipt_key(previous.incarnation, receipt),
            });
        }
    }
    changes.push(KvMutation::Put {
        key,
        value: storage_serde::to_bytes(&record)?,
    });
    changes.push(KvMutation::Put {
        key: due_key(&record),
        value: due_value(&record),
    });
    if let Some(receipt) = &record.message.receipt_handle {
        changes.push(KvMutation::Put {
            key: receipt_key(record.incarnation, receipt),
            value: record.message.message_id.as_bytes().to_vec(),
        });
    }
    Ok(changes)
}

pub(crate) async fn delete_message_records<A: KvAttempt>(
    native: &mut A,
    records: Vec<QueueMessageRecord>,
) -> StorageResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let keys = records
        .iter()
        .map(|record| message_key(record.incarnation, record.message.message_id))
        .collect();
    let current = native.get_batch(keys, ReadProtection::Conflict).await?;
    if current.len() != records.len() {
        return Err(StorageError::internal(
            "KV queue cleanup read count differs",
        ));
    }
    let mut changes = Vec::new();
    for (record, current) in records.into_iter().zip(current) {
        if current != Some(storage_serde::to_bytes(&record)?) {
            return Err(StorageEnum::TransactionConflict {
                message: "KV queue message changed during delete".to_owned(),
            }
            .into());
        }
        changes.push(KvMutation::Delete {
            key: message_key(record.incarnation, record.message.message_id),
        });
        changes.push(KvMutation::Delete {
            key: due_key(&record),
        });
        if let Some(receipt) = &record.message.receipt_handle {
            changes.push(KvMutation::Delete {
                key: receipt_key(record.incarnation, receipt),
            });
        }
    }
    native.stage(changes)
}

pub(crate) async fn count_queue_messages<A: KvAttempt>(
    native: &mut A,
    incarnation: QueueIncarnation,
    captured_at: TimestampMillis,
) -> StorageResult<QueueMessageCounts> {
    let prefix = [DUE_PREFIX, incarnation.as_bytes()].concat();
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV queue count has no upper bound"))?;
    let mut begin = prefix.clone();
    let mut counts = QueueMessageCounts::default();
    loop {
        let page = native
            .scan(
                KvRange {
                    entry_bounds: None,
                    begin,
                    end: end.clone(),
                    reverse: false,
                    limit: NonZeroU32::MIN.saturating_add(512 - 1),
                    max_bytes: MESSAGE_READ_BYTES,
                },
                ReadProtection::Snapshot,
            )
            .await?;
        for entry in &page.entries {
            let (due, _) = decode_due_key(&prefix, &entry.key)?;
            let (claimed, expires) = decode_due_value(&entry.value)?;
            if expires <= captured_at {
                continue;
            }
            match (due <= captured_at, claimed) {
                (true, _) => counts.visible = counts.visible.saturating_add(1),
                (false, true) => counts.not_visible = counts.not_visible.saturating_add(1),
                (false, false) => counts.delayed = counts.delayed.saturating_add(1),
            }
        }
        if !page.more {
            return Ok(counts);
        }
        let last = page
            .entries
            .last()
            .ok_or_else(|| StorageError::internal("KV queue count page cannot advance"))?;
        begin = last.key.clone();
        begin.push(0);
    }
}

pub(crate) async fn read_message_cleanup_page<A: KvAttempt>(
    native: &mut A,
    request: &QueueCleanupRequest,
) -> StorageResult<QueueCleanupPage> {
    let mut begin = MESSAGE_PREFIX.to_vec();
    if let Some(cursor) = request.exclusive_start {
        begin = message_key(cursor.incarnation, cursor.message_id);
        begin.push(0);
    }
    let end = prefix_upper_bound(MESSAGE_PREFIX)
        .ok_or_else(|| StorageError::internal("KV queue cleanup has no upper bound"))?;
    let limit = NonZeroU32::new(request.limit.get() + 1)
        .ok_or_else(|| StorageError::internal("KV queue cleanup limit overflowed"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin,
                end,
                reverse: false,
                limit,
                max_bytes: MESSAGE_READ_BYTES,
            },
            ReadProtection::Conflict,
        )
        .await?;
    Ok(QueueCleanupPage {
        rows: page
            .entries
            .into_iter()
            .map(decode_message_entry)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

fn message_key(incarnation: QueueIncarnation, id: MessageId) -> Vec<u8> {
    [MESSAGE_PREFIX, incarnation.as_bytes(), id.as_bytes()].concat()
}

fn decode_message_entry(entry: crate::KvEntry) -> StorageResult<QueueMessageRecord> {
    let suffix = entry
        .key
        .strip_prefix(MESSAGE_PREFIX)
        .ok_or_else(|| StorageError::internal("KV queue message row has another prefix"))?;
    let (incarnation, id) = suffix
        .split_at_checked(16)
        .ok_or_else(|| StorageError::internal("KV queue message key is too short"))?;
    let incarnation = QueueIncarnation::try_from(incarnation)?;
    let id: [u8; MESSAGE_ID_VERSIONSTAMP_LEN] = id
        .try_into()
        .map_err(|_| StorageError::internal("KV queue message key has invalid ID"))?;
    let record: QueueMessageRecord = storage_serde::from_bytes(&entry.value)?;
    if record.incarnation != incarnation || record.message.message_id != MessageId::from_bytes(id) {
        return Err(StorageError::internal(
            "KV queue message key differs from row",
        ));
    }
    Ok(record)
}

fn due_key(record: &QueueMessageRecord) -> Vec<u8> {
    [
        DUE_PREFIX,
        record.incarnation.as_bytes(),
        &ordered_timestamp(record.visible_at()),
        record.message.message_id.as_bytes(),
    ]
    .concat()
}

fn due_value(record: &QueueMessageRecord) -> Vec<u8> {
    let mut value = Vec::with_capacity(9);
    value.push(u8::from(record.message.receipt_handle.is_some()));
    value.extend_from_slice(&record.expires_at.timestamp_millis().to_be_bytes());
    value
}

fn decode_due_value(value: &[u8]) -> StorageResult<(bool, TimestampMillis)> {
    let [claimed @ (0 | 1), expires @ ..] = value else {
        return Err(StorageError::internal("KV queue due state is invalid"));
    };
    let expires: [u8; 8] = expires
        .try_into()
        .map_err(|_| StorageError::internal("KV queue due expiration is invalid"))?;
    Ok((
        *claimed == 1,
        TimestampMillis::from_timestamp(i64::from_be_bytes(expires)),
    ))
}

fn receipt_key(incarnation: QueueIncarnation, receipt: &ReceiptHandle) -> Vec<u8> {
    [RECEIPT_PREFIX, incarnation.as_bytes(), receipt.0.as_bytes()].concat()
}

fn ordered_timestamp(value: TimestampMillis) -> [u8; 8] {
    ((value.timestamp_millis() as u64) ^ (1_u64 << 63)).to_be_bytes()
}

fn decode_due_key(prefix: &[u8], key: &[u8]) -> StorageResult<(TimestampMillis, MessageId)> {
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| StorageError::internal("KV queue due row has another prefix"))?;
    let (due, id) = suffix
        .split_at_checked(8)
        .ok_or_else(|| StorageError::internal("KV queue due row is too short"))?;
    let due: [u8; 8] = due
        .try_into()
        .map_err(|_| StorageError::internal("KV queue due timestamp is invalid"))?;
    let id = id
        .try_into()
        .map_err(|_| StorageError::internal("KV queue due message ID is invalid"))?;
    let due = (u64::from_be_bytes(due) ^ (1_u64 << 63)) as i64;
    Ok((
        TimestampMillis::from_timestamp(due),
        MessageId::from_bytes(id),
    ))
}
