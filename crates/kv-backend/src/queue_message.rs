//! One ordered queue message layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use queue_provider::{
    MESSAGE_ID_LEN, MessageId, QueueCatalogRecord, QueueCleanupPage, QueueCleanupRequest,
    QueueCountFacts, QueueDueBatch, QueueDueMark, QueueDueRequest, QueueIncarnation,
    QueueMessageBody, QueueMessageCounts, QueueMessageRecord, QueueMessageState, ReceiptHandle,
};
use storage_types::{
    Exact, MissExpectation, ReadProtection, StorageError, StorageResult, TimestampMillis,
    storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{
        DUE_PREFIX, MESSAGE_PREFIX, MESSAGE_READ_BYTES, QUEUE_STATE_PREFIX, RECEIPT_PREFIX,
    },
    queue_catalog,
    range::prefix_upper_bound,
};

/// A first message read that carried the check of a hinted catalog row
/// (W7-H). `Moved` holds the row read in the same native call when it
/// differs from the hint; none of the call's other values were decoded and
/// no dependent read was made.
pub(crate) enum Checked<T> {
    Read(T),
    Moved(Option<QueueCatalogRecord>),
}

/// One protected point read of `keys`; with `check`, the hinted row's key
/// rides in the same native call at `Snapshot`, last (W7-H, as
/// `pubsub_schedule::load_records`).
async fn get_conflict_checked<A: KvAttempt>(
    native: &A,
    mut keys: Vec<Vec<u8>>,
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<Exact<Option<Vec<u8>>>>> {
    let Some(expected) = check else {
        return native
            .get_batch_exact(keys, ReadProtection::Conflict)
            .await
            .map(Checked::Read);
    };
    let name = &expected.queue.queue_name;
    let mut protections = vec![ReadProtection::Conflict; keys.len()];
    keys.push(queue_catalog::name_key(name));
    protections.push(ReadProtection::Snapshot);
    let (values, catalog) = native
        .get_batch_mixed_exact(keys, protections)
        .await?
        .split_last_if(true);
    let current = catalog
        .flatten()
        .map(|bytes| queue_catalog::decode_named_value(&bytes, name))
        .transpose()?;
    if current.as_ref() != Some(expected) {
        return Ok(Checked::Moved(current));
    }
    Ok(Checked::Read(values))
}

/// One snapshot wave reads a window of due-index entries at or before the
/// claim time, the first entries after it, and the catalog row of a
/// dead-letter target when the request names one; the provider's rule selects
/// window positions; only the selected state and body keys are read with
/// conflict protection, in one call. A concurrent send adds index entries that
/// this attempt did not read with protection, so it does not abort the claim;
/// a concurrent claim of a selected message changes its state key and aborts
/// one of the two. With `check`, the hinted catalog row's range is one more
/// range of the snapshot wave and is compared before any due entry is decoded
/// (W7-H).
pub(crate) async fn read_due_messages<A: KvAttempt>(
    native: &A,
    request: &QueueDueRequest,
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<QueueDueBatch>> {
    let prefix = [DUE_PREFIX, request.incarnation.as_bytes()].concat();
    let split_prefix = [prefix.as_slice(), &ordered_timestamp(request.captured_at)].concat();
    let split = prefix_upper_bound(&split_prefix)
        .ok_or_else(|| StorageError::internal("KV queue due upper bound overflowed"))?;
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV queue due index has no upper bound"))?;
    let mut ranges = vec![
        KvRange {
            entry_bounds: None,
            begin: prefix.clone(),
            end: split.clone(),
            reverse: false,
            limit: request.window,
            max_bytes: MESSAGE_READ_BYTES,
        },
        KvRange {
            entry_bounds: None,
            begin: split,
            end,
            reverse: false,
            limit: request.head,
            max_bytes: MESSAGE_READ_BYTES,
        },
    ];
    if let Some(name) = &request.dead_letter_target {
        ranges.push(queue_catalog::queue_name_range(name));
    }
    if let Some(expected) = check {
        ranges.push(queue_catalog::queue_name_range(&expected.queue.queue_name));
    }
    // Ranges are window, head, then the optional dead-letter and hinted rows.
    let pages = native
        .scan_batch_exact(ranges, ReadProtection::Snapshot)
        .await?;
    let (pages, check_page) = pages.split_last_if(check.is_some());
    if let Some((expected, page)) = check.zip(check_page) {
        let current = queue_catalog::decode_named_page(page, &expected.queue.queue_name)?;
        if current.as_ref() != Some(expected) {
            return Ok(Checked::Moved(current));
        }
    }
    let (pages, dead_letter_page) = pages.split_last_if(request.dead_letter_target.is_some());
    let dead_letter_target = match request.dead_letter_target.as_ref().zip(dead_letter_page) {
        Some((name, page)) => queue_catalog::decode_named_page(page, name)?,
        None => None,
    };
    let [window, after] = pages.into_array::<2>()?;
    let window_full = window.more || window.entries.len() >= request.window.get() as usize;
    let entries = window
        .entries
        .iter()
        .map(|entry| decode_due_entry(&prefix, entry))
        .collect::<StorageResult<Vec<_>>>()?;
    let after = after
        .entries
        .iter()
        .map(|entry| decode_due_entry(&prefix, entry).map(|entry| entry.mark))
        .collect::<StorageResult<Vec<_>>>()?;
    let marks = entries.iter().map(|entry| entry.mark).collect::<Vec<_>>();
    let selection = request.select(&marks, window_full, &after);
    let selected = selection
        .positions
        .iter()
        .map(|position| {
            entries
                .get(*position)
                .copied()
                .ok_or_else(|| StorageError::internal("KV queue claim position is out of range"))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let ids = selected.iter().map(|entry| entry.id).collect::<Vec<_>>();
    let Checked::Read(parts) = read_parts(native, request.incarnation, &ids, None).await? else {
        return Err(StorageError::internal(
            "KV queue parts read moved without a check",
        ));
    };
    let mut records = Vec::with_capacity(selected.len());
    // Snapshot and protected reads of one attempt share its read version, so
    // the index and the state agree unless the store is corrupt.
    for (entry, part) in selected.into_iter().zip(parts) {
        let (state, body) =
            part.ok_or_else(|| StorageError::internal("KV queue due index has no message"))?;
        if state.visible_at() != entry.mark.due_at
            || state.expires_at != entry.expires_at
            || state.receipt_handle.is_some() != entry.mark.leased
        {
            return Err(StorageError::internal(
                "KV queue due index differs from message",
            ));
        }
        records.push(QueueMessageRecord::from_parts(state, body));
    }
    Ok(Checked::Read(QueueDueBatch {
        records,
        drained: selection.drained,
        next_due: selection.next_due,
        dead_letter_target,
    }))
}

#[derive(Clone, Copy)]
struct DueEntry {
    mark: QueueDueMark,
    id: MessageId,
    expires_at: TimestampMillis,
}

fn decode_due_entry(prefix: &[u8], entry: &crate::KvEntry) -> StorageResult<DueEntry> {
    let (due_at, id) = decode_due_key(prefix, &entry.key)?;
    let (leased, expires_at) = decode_due_value(&entry.value)?;
    Ok(DueEntry {
        mark: QueueDueMark { due_at, leased },
        id,
        expires_at,
    })
}

pub(crate) async fn read_due_head<A: KvAttempt>(
    native: &A,
    incarnation: QueueIncarnation,
    limit: NonZeroU32,
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<Vec<QueueDueMark>>> {
    let prefix = [DUE_PREFIX, incarnation.as_bytes()].concat();
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV queue next due has no upper bound"))?;
    let range = KvRange {
        entry_bounds: None,
        begin: prefix.clone(),
        end,
        reverse: false,
        limit,
        max_bytes: MESSAGE_READ_BYTES,
    };
    let page = match check {
        None => native.scan(range, ReadProtection::Snapshot).await?,
        Some(expected) => {
            let name = &expected.queue.queue_name;
            let [head, catalog] = native
                .scan_batch_exact(
                    vec![range, queue_catalog::queue_name_range(name)],
                    ReadProtection::Snapshot,
                )
                .await?
                .into_array::<2>()?;
            let current = queue_catalog::decode_named_page(catalog, name)?;
            if current.as_ref() != Some(expected) {
                return Ok(Checked::Moved(current));
            }
            head
        }
    };
    page.entries
        .iter()
        .map(|entry| decode_due_entry(&prefix, entry).map(|entry| entry.mark))
        .collect::<StorageResult<Vec<_>>>()
        .map(Checked::Read)
}

pub(crate) async fn read_messages_by_receipt<A: KvAttempt>(
    native: &A,
    incarnation: QueueIncarnation,
    receipts: Vec<ReceiptHandle>,
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<Exact<Option<QueueMessageState>>>> {
    let keys = receipts
        .iter()
        .map(|receipt| receipt_key(incarnation, receipt))
        .collect();
    let values = match get_conflict_checked(native, keys, check).await? {
        Checked::Read(values) => values,
        Checked::Moved(current) => return Ok(Checked::Moved(current)),
    };
    let ids = values.try_map(|value| {
        value
            .map(|bytes| {
                let id: [u8; MESSAGE_ID_LEN] = bytes
                    .try_into()
                    .map_err(|_| StorageError::internal("KV queue receipt index has invalid ID"))?;
                Ok::<_, StorageError>(MessageId::from_bytes(id))
            })
            .transpose()
    })?;
    let found = ids.iter().flatten().copied().collect::<Vec<_>>();
    let mut states = read_message_states_by_id(native, incarnation, found)
        .await?
        .into_iter();
    ids.try_zip_map(receipts, |receipt, id| {
        if id.is_none() {
            return Ok(None);
        }
        let state = states
            .next()
            .flatten()
            .ok_or_else(|| StorageError::internal("KV queue receipt index has no message"))?;
        if state
            .receipt_handle
            .as_ref()
            .map(|handle| handle.0.as_str())
            != Some(receipt.0.as_str())
        {
            return Err(StorageError::internal(
                "KV queue receipt index differs from message",
            ));
        }
        Ok(Some(state))
    })
    .map(Checked::Read)
}

/// States and bodies in input order, explicit missing rows; one protected call.
pub(crate) async fn read_messages_by_id<A: KvAttempt>(
    native: &A,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<Exact<Option<QueueMessageRecord>>>> {
    Ok(match read_parts(native, incarnation, &ids, check).await? {
        Checked::Read(parts) => Checked::Read(
            parts.map(|part| part.map(|(state, body)| QueueMessageRecord::from_parts(state, body))),
        ),
        Checked::Moved(current) => Checked::Moved(current),
    })
}

/// States in input order, explicit missing rows; no body is read.
pub(crate) async fn read_message_states_by_id<A: KvAttempt>(
    native: &A,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
) -> StorageResult<Exact<Option<QueueMessageState>>> {
    if ids.is_empty() {
        return Ok(Exact::empty());
    }
    let keys = ids.iter().map(|id| state_key(incarnation, *id)).collect();
    let values = native
        .get_batch_exact(keys, ReadProtection::Conflict)
        .await?;
    values.try_zip_map(ids, |id, value| {
        value
            .map(|bytes| decode_state(&bytes, incarnation, id))
            .transpose()
    })
}

/// One protected `get_batch` of all state keys, then all body keys.
async fn read_parts<A: KvAttempt>(
    native: &A,
    incarnation: QueueIncarnation,
    ids: &[MessageId],
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<Exact<Option<(QueueMessageState, QueueMessageBody)>>>> {
    if ids.is_empty() && check.is_none() {
        return Ok(Checked::Read(Exact::empty()));
    }
    let mut keys = Vec::with_capacity(ids.len() * 2);
    keys.extend(ids.iter().map(|id| state_key(incarnation, *id)));
    keys.extend(ids.iter().map(|id| body_key(incarnation, *id)));
    let values = match get_conflict_checked(native, keys, check).await? {
        Checked::Read(values) => values,
        Checked::Moved(current) => return Ok(Checked::Moved(current)),
    };
    let mut values = values.into_iter();
    let states = values.by_ref().take(ids.len()).collect::<Vec<_>>();
    let parts = ids
        .iter()
        .zip(states)
        .zip(values)
        .map(|((id, state), body)| match (state, body) {
            (None, None) => Ok(None),
            (Some(state), Some(body)) => Ok(Some((
                decode_state(&state, incarnation, *id)?,
                storage_serde::from_bytes(&body)?,
            ))),
            _ => Err(StorageError::internal(
                "KV queue message has a state or a body only",
            )),
        })
        .collect::<StorageResult<Vec<_>>>()?;
    // One part for each id: the state half and the body half are `ids.len()`.
    Exact::try_new(ids.len(), parts).map(Checked::Read)
}

/// One protected presence read of the state keys of new messages. A message
/// deleted earlier in this attempt reads as absent (the attempt reads its own
/// writes). With `check`, the hinted catalog key rides in the same call (W7-H).
pub(crate) async fn require_absent<A: KvAttempt>(
    native: &A,
    records: &[QueueMessageRecord],
    check: Option<&QueueCatalogRecord>,
) -> StorageResult<Checked<()>> {
    if records.is_empty() && check.is_none() {
        return Ok(Checked::Read(()));
    }
    reject_repeated(
        records
            .iter()
            .map(|record| (record.incarnation, record.message.message_id))
            .collect(),
    )?;
    let keys = records
        .iter()
        .map(|record| state_key(record.incarnation, record.message.message_id))
        .collect();
    let present: Vec<bool> = match check {
        None => native
            .get_batch_presence_exact(keys, ReadProtection::Conflict)
            .await?
            .into_vec(),
        Some(_) => match get_conflict_checked(native, keys, check).await? {
            Checked::Read(values) => values.iter().map(Option::is_some).collect(),
            Checked::Moved(current) => return Ok(Checked::Moved(current)),
        },
    };
    if present.into_iter().any(|present| present) {
        return Err(StorageError::write_miss(
            "queue_message",
            MissExpectation::Absent,
        ));
    }
    Ok(Checked::Read(()))
}

/// The insert check of `records` from an absence read that an earlier wave
/// of this attempt made under `incarnation` (W8-E-e); `checked` holds each
/// read id and whether its state key was present. `None` when a record is
/// not covered: the caller reads as `require_absent` does. Nothing of this
/// attempt wrote message keys between that read and this insert.
pub(crate) fn checked_absence(
    records: &[QueueMessageRecord],
    incarnation: QueueIncarnation,
    checked: &[(MessageId, bool)],
) -> Option<StorageResult<()>> {
    let mut present = false;
    for record in records {
        if record.incarnation != incarnation {
            return None;
        }
        // At most 32 ids: a linear scan.
        let (_, is_present) = checked
            .iter()
            .find(|(id, _)| *id == record.message.message_id)?;
        present |= *is_present;
    }
    if let Err(error) = reject_repeated(
        records
            .iter()
            .map(|record| (record.incarnation, record.message.message_id))
            .collect(),
    ) {
        return Some(Err(error));
    }
    if present {
        return Some(Err(StorageError::write_miss(
            "queue_message",
            MissExpectation::Absent,
        )));
    }
    Some(Ok(()))
}

/// Writes state, body and indexes of messages that `require_absent` checked.
pub(crate) fn stage_inserts<A: KvAttempt>(
    native: &mut A,
    records: Vec<QueueMessageRecord>,
) -> StorageResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut changes = Vec::with_capacity(records.len() * 5);
    for record in records {
        let (state, body) = record.into_parts();
        changes.push(KvMutation::Put {
            key: body_key(state.incarnation, state.message_id),
            value: storage_serde::to_bytes(&body)?,
        });
        put_state(&state, &mut changes)?;
    }
    native.stage(changes)
}

/// Replaces states that this attempt read with protection; the body is not
/// touched. Index keys move with the state.
pub(crate) fn write_message_states<A: KvAttempt>(
    native: &mut A,
    changes: Vec<(QueueMessageState, QueueMessageState)>,
) -> StorageResult<()> {
    if changes.is_empty() {
        return Ok(());
    }
    reject_repeated(
        changes
            .iter()
            .map(|(state, _)| (state.incarnation, state.message_id))
            .collect(),
    )?;
    let mut mutations = Vec::with_capacity(changes.len() * 5);
    for (state, previous) in changes {
        if state.incarnation != previous.incarnation || state.message_id != previous.message_id {
            return Err(StorageError::internal(
                "KV queue state change names two messages",
            ));
        }
        delete_indexes(&previous, &mut mutations);
        put_state(&state, &mut mutations)?;
    }
    native.stage(mutations)
}

/// Deletes states that this attempt read with protection, their indexes and
/// their bodies; the body delete clears its chunk span without a read.
pub(crate) fn delete_messages<A: KvAttempt>(
    native: &mut A,
    states: Vec<QueueMessageState>,
) -> StorageResult<()> {
    if states.is_empty() {
        return Ok(());
    }
    reject_repeated(
        states
            .iter()
            .map(|state| (state.incarnation, state.message_id))
            .collect(),
    )?;
    let mut mutations = Vec::with_capacity(states.len() * 4);
    for state in states {
        delete_indexes(&state, &mut mutations);
        mutations.push(KvMutation::DeleteUnchunked {
            key: state_key(state.incarnation, state.message_id),
        });
        mutations.push(KvMutation::Delete {
            key: body_key(state.incarnation, state.message_id),
        });
    }
    native.stage(mutations)
}

pub(crate) fn reject_repeated(ids: Vec<(QueueIncarnation, MessageId)>) -> StorageResult<()> {
    if ids
        .iter()
        .enumerate()
        .any(|(index, id)| ids[..index].contains(id))
    {
        return Err(StorageError::internal(
            "KV queue write batch repeats a message key",
        ));
    }
    Ok(())
}

/// Order matters: an index key that a change keeps (same due time or
/// receipt) is deleted by `delete_indexes` first and put again here.
fn put_state(state: &QueueMessageState, out: &mut Vec<KvMutation>) -> StorageResult<()> {
    out.push(KvMutation::PutUnchunked {
        key: state_key(state.incarnation, state.message_id),
        value: storage_serde::to_bytes(state)?,
    });
    out.push(KvMutation::PutUnchunked {
        key: due_key(state),
        value: due_value(state),
    });
    if let Some(receipt) = &state.receipt_handle {
        out.push(KvMutation::PutUnchunked {
            key: receipt_key(state.incarnation, receipt),
            value: state.message_id.as_bytes().to_vec(),
        });
    }
    Ok(())
}

fn delete_indexes(state: &QueueMessageState, out: &mut Vec<KvMutation>) {
    out.push(KvMutation::DeleteUnchunked {
        key: due_key(state),
    });
    if let Some(receipt) = &state.receipt_handle {
        out.push(KvMutation::DeleteUnchunked {
            key: receipt_key(state.incarnation, receipt),
        });
    }
}

/// Two bounded regions (decision D6): due (`<= captured_at`) and later.
/// Each stops after `scan_limit` entries; pages are sized by bytes. Each entry
/// gives its physical facts to the provider's bucket rule.
pub(crate) async fn count_queue_messages<A: KvAttempt>(
    native: &A,
    incarnation: QueueIncarnation,
    captured_at: TimestampMillis,
    scan_limit: NonZeroU32,
) -> StorageResult<QueueMessageCounts> {
    let prefix = [DUE_PREFIX, incarnation.as_bytes()].concat();
    let split = prefix_upper_bound(&[prefix.as_slice(), &ordered_timestamp(captured_at)].concat())
        .ok_or_else(|| StorageError::internal("KV queue count split overflowed"))?;
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV queue count has no upper bound"))?;
    let mut counts = QueueMessageCounts::default();
    for (begin, end) in [(prefix.clone(), split.clone()), (split, end)] {
        count_region(
            native,
            &prefix,
            begin,
            end,
            scan_limit,
            |due, leased, expires| {
                let facts = QueueCountFacts {
                    due: due <= captured_at,
                    leased,
                    live: expires > captured_at,
                };
                counts.add(facts, 1);
            },
        )
        .await?;
    }
    Ok(counts)
}

async fn count_region<A: KvAttempt>(
    native: &A,
    prefix: &[u8],
    mut begin: Vec<u8>,
    end: Vec<u8>,
    scan_limit: NonZeroU32,
    mut count: impl FnMut(TimestampMillis, bool, TimestampMillis),
) -> StorageResult<()> {
    let mut remaining = scan_limit.get();
    while let Some(limit) = NonZeroU32::new(remaining.min(COUNT_PAGE_ENTRIES)) {
        let page = native
            .scan(
                KvRange {
                    entry_bounds: None,
                    begin,
                    end: end.clone(),
                    reverse: false,
                    limit,
                    max_bytes: MESSAGE_READ_BYTES,
                },
                ReadProtection::Snapshot,
            )
            .await?;
        for entry in &page.entries {
            let (due, _) = decode_due_key(prefix, &entry.key)?;
            let (claimed, expires) = decode_due_value(&entry.value)?;
            count(due, claimed, expires);
        }
        let read = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
        remaining = remaining.saturating_sub(read);
        if !page.more {
            return Ok(());
        }
        let last = page
            .entries
            .last()
            .ok_or_else(|| StorageError::internal("KV queue count page cannot advance"))?;
        begin = last.key.clone();
        begin.push(0);
    }
    Ok(())
}

/// About 1 MiB of due-index entries (about 50 bytes each).
const COUNT_PAGE_ENTRIES: u32 = 20_000;

pub(crate) async fn read_message_cleanup_page<A: KvAttempt>(
    native: &A,
    request: &QueueCleanupRequest,
) -> StorageResult<QueueCleanupPage> {
    let mut begin = QUEUE_STATE_PREFIX.to_vec();
    if let Some(cursor) = request.exclusive_start {
        begin = state_key(cursor.incarnation, cursor.message_id);
        begin.push(0);
    }
    let end = prefix_upper_bound(QUEUE_STATE_PREFIX)
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
            .map(decode_state_entry)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

fn body_key(incarnation: QueueIncarnation, id: MessageId) -> Vec<u8> {
    [MESSAGE_PREFIX, incarnation.as_bytes(), id.as_bytes()].concat()
}

pub(crate) fn state_key(incarnation: QueueIncarnation, id: MessageId) -> Vec<u8> {
    [QUEUE_STATE_PREFIX, incarnation.as_bytes(), id.as_bytes()].concat()
}

fn decode_state(
    bytes: &[u8],
    incarnation: QueueIncarnation,
    id: MessageId,
) -> StorageResult<QueueMessageState> {
    let state: QueueMessageState = storage_serde::from_bytes(bytes)?;
    if state.incarnation != incarnation || state.message_id != id {
        return Err(StorageError::internal(
            "KV queue state key differs from row",
        ));
    }
    Ok(state)
}

fn decode_state_entry(entry: crate::KvEntry) -> StorageResult<QueueMessageState> {
    let suffix = entry
        .key
        .strip_prefix(QUEUE_STATE_PREFIX)
        .ok_or_else(|| StorageError::internal("KV queue state row has another prefix"))?;
    let (incarnation, id) = suffix
        .split_at_checked(16)
        .ok_or_else(|| StorageError::internal("KV queue state key is too short"))?;
    let incarnation = QueueIncarnation::try_from(incarnation)?;
    let id: [u8; MESSAGE_ID_LEN] = id
        .try_into()
        .map_err(|_| StorageError::internal("KV queue state key has invalid ID"))?;
    decode_state(&entry.value, incarnation, MessageId::from_bytes(id))
}

fn due_key(state: &QueueMessageState) -> Vec<u8> {
    [
        DUE_PREFIX,
        state.incarnation.as_bytes(),
        &ordered_timestamp(state.visible_at()),
        state.message_id.as_bytes(),
    ]
    .concat()
}

fn due_value(state: &QueueMessageState) -> Vec<u8> {
    let mut value = Vec::with_capacity(9);
    value.push(u8::from(state.receipt_handle.is_some()));
    value.extend_from_slice(&state.expires_at.timestamp_millis().to_be_bytes());
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
