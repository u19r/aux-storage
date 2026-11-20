//! The delivery payload and state keys for RocksDB and FoundationDB (W3S
//! plan 5.5). A payload key is written once and deleted with its state. A
//! state key is `<prefix><destination id:16><due:8, sign flipped><id>`: the
//! key holds the only due time (R2), and a state change deletes the old key
//! and puts the new one in the same attempt. A fenced write reads the
//! locator key with conflict protection first.

use std::num::NonZeroU32;

use pubsub_provider::{
    AskMode, ClaimRows, ClaimScan, DeliveryFence, DeliveryLocator, DeliveryPayload,
    DeliveryRecordId, DeliveryState, DestinationAsk, DestinationHead, DestinationId, PayloadRow,
    RecordObservations, RecordReads, StateChange, StateRow, SweepCursor, SweepLane, SweepPage,
};
use queue_provider::QueueIncarnation;
use storage_types::{
    MissExpectation, ReadProtection, StorageError, StorageResult, TimestampMillis,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{CATALOG_BYTES, PUBSUB_DELIVERY_PAYLOAD_PREFIX, PUBSUB_DELIVERY_STATE_PREFIX},
    range::prefix_upper_bound,
};

/// Due entries that a claim reads per ask at `Snapshot` before it selects
/// its slot (design 7.1).
const CLAIM_WINDOW: NonZeroU32 = NonZeroU32::MIN.saturating_add(256 - 1);
/// The sweep splits the destination space by the high 4 bits of the id;
/// each wave reads the next destination of every open lane.
const SWEEP_LANES: u8 = 16;

pub(crate) async fn store_new_deliveries<A: KvAttempt>(
    native: &mut A,
    rows: Vec<(DeliveryPayload, DeliveryState)>,
) -> StorageResult<()> {
    let keys = rows
        .iter()
        .map(|(payload, _)| payload_key(&payload.id))
        .collect();
    let existing = native.get_batch(keys, ReadProtection::Conflict).await?;
    if existing.iter().any(Option::is_some) {
        return Err(StorageError::write_miss(
            "pubsub_delivery",
            MissExpectation::Absent,
        ));
    }
    let mut changes = Vec::with_capacity(rows.len() * 2);
    for (payload, state) in rows {
        if payload.id != state.id {
            return Err(StorageError::internal("delivery payload and state differ"));
        }
        changes.push(KvMutation::Put {
            key: payload_key(&payload.id),
            value: payload.encode()?,
        });
        changes.push(KvMutation::Put {
            key: state_key(&state.locator()),
            value: state.encode_value()?,
        });
    }
    native.stage(changes)
}

/// One snapshot wave over the asked partitions, the slot choice of each
/// ask, then one conflict wave of the chosen state and payload keys:
/// concurrent claims conflict only on a shared record (design 7.2).
pub(crate) async fn scan_claimable<A: KvAttempt>(
    native: &mut A,
    scan: &ClaimScan,
) -> StorageResult<storage_types::Exact<ClaimRows>> {
    let ranges = scan
        .asks
        .iter()
        .map(|ask| partition_range(ask.destination.id(), CLAIM_WINDOW))
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = native
        .scan_batch_exact(ranges, ReadProtection::Snapshot)
        .await?;
    let mut chosen = Vec::with_capacity(scan.asks.len());
    for (ask, page) in scan.asks.iter().zip(pages) {
        let window_full = page.more || page.entries.len() >= CLAIM_WINDOW.get() as usize;
        let entries = page
            .entries
            .iter()
            .map(|entry| decode_locator(&entry.key).map(|locator| (locator, &entry.value)))
            .collect::<StorageResult<Vec<_>>>()?;
        let candidates: Vec<usize> = (0..entries.len())
            .filter(|index| is_candidate(ask, &entries[*index], scan.now))
            .collect();
        let positions = scan.slot(ask).positions(candidates.len(), window_full);
        let picked: Vec<usize> = positions
            .iter()
            .map(|position| candidates[*position])
            .collect();
        let next_due = (0..entries.len())
            .find(|index| !picked.contains(index))
            .map(|index| entries[index].0.due_at);
        let locators = picked
            .into_iter()
            .map(|index| entries[index].0.clone())
            .collect();
        chosen.push((ask.destination.id(), locators, next_due));
    }
    let keys = chosen
        .iter()
        .flat_map(|(_, locators, _): &(_, Vec<DeliveryLocator>, _)| locators)
        .flat_map(|locator| [state_key(locator), payload_key(&locator.id)])
        .collect();
    // Values alternate state, payload, one pair per locator in `keys` order.
    let (states, payloads): (Vec<_>, Vec<_>) = native
        .get_batch_exact(keys, ReadProtection::Conflict)
        .await?
        .into_iter()
        .enumerate()
        .partition(|(index, _)| index % 2 == 0);
    let mut pairs = states.into_iter().zip(payloads);
    let mut claims = Vec::with_capacity(chosen.len());
    for (destination, locators, next_due) in chosen {
        let mut rows = Vec::with_capacity(locators.len());
        for (locator, ((_, state), (_, payload))) in locators.into_iter().zip(pairs.by_ref()) {
            // A state that a settle removed after the snapshot is skipped.
            let Some(state) = state else { continue };
            rows.push(decode_rows(locator, &state, payload.as_deref()));
        }
        claims.push(ClaimRows {
            destination,
            rows,
            next_due,
        });
    }
    storage_types::Exact::try_new(scan.asks.len(), claims)
}

fn is_candidate(
    ask: &DestinationAsk,
    (locator, value): &(DeliveryLocator, &Vec<u8>),
    now: TimestampMillis,
) -> bool {
    if locator.due_at > now {
        return false;
    }
    match ask.mode {
        AskMode::Due | AskMode::Probe => true,
        AskMode::DeadlineOnly => DeliveryState::decode_value(locator, value)
            .is_ok_and(|state| state.retry_deadline <= now),
    }
}

/// The claim read these keys with conflict protection in this attempt.
pub(crate) async fn store_claimed_states<A: KvAttempt>(
    native: &mut A,
    claims: Vec<(DeliveryState, DeliveryState)>,
) -> StorageResult<()> {
    let mut changes = Vec::with_capacity(claims.len() * 2);
    for (previous, next) in claims {
        changes.extend(move_state(&previous.locator(), &next)?);
    }
    if changes.is_empty() {
        return Ok(());
    }
    native.stage(changes)
}

pub(crate) async fn store_delivery_states<A: KvAttempt>(
    native: &mut A,
    changes: Vec<StateChange>,
) -> StorageResult<Vec<bool>> {
    let held = read_fences(native, changes.iter().map(|change| &change.expected)).await?;
    let mut mutations = Vec::with_capacity(changes.len() * 2);
    for (change, is_held) in changes.iter().zip(&held) {
        if *is_held {
            mutations.extend(move_state(&change.expected.locator, &change.next)?);
        }
    }
    if !mutations.is_empty() {
        native.stage(mutations)?;
    }
    Ok(held)
}

pub(crate) async fn delete_deliveries<A: KvAttempt>(
    native: &mut A,
    fences: Vec<DeliveryFence>,
) -> StorageResult<Vec<bool>> {
    let held = read_fences(native, fences.iter()).await?;
    stage_delivery_deletes(
        native,
        fences
            .into_iter()
            .zip(&held)
            .filter(|(_, is_held)| **is_held)
            .map(|(fence, _)| fence)
            .collect(),
    )?;
    Ok(held)
}

/// The state and payload deletes of fences read as held in this attempt.
pub(crate) fn stage_delivery_deletes<A: KvAttempt>(
    native: &mut A,
    fences: Vec<DeliveryFence>,
) -> StorageResult<()> {
    if fences.is_empty() {
        return Ok(());
    }
    let mutations = fences
        .iter()
        .flat_map(|fence| {
            [
                KvMutation::Delete {
                    key: state_key(&fence.locator),
                },
                KvMutation::Delete {
                    key: payload_key(&fence.locator.id),
                },
            ]
        })
        .collect();
    native.stage(mutations)
}

/// The fence states (`Conflict`), the queue catalog row (`Snapshot`) and,
/// with `hinted`, the state keys of `reads.message_ids` under that
/// incarnation (`Conflict`) in one `get_batch_mixed` (W3 C8, W8-E-e). The
/// second value is the presence of each message id, in input order; empty
/// without `hinted`.
pub(crate) async fn load_records<A: KvAttempt>(
    native: &mut A,
    reads: RecordReads<'_>,
    hinted: Option<QueueIncarnation>,
) -> StorageResult<(RecordObservations, Vec<bool>)> {
    let absence = hinted.filter(|_| reads.queue_name.is_some());
    let absence_count = absence.map_or(0, |_| reads.message_ids.len());
    let count = reads.fences.len() + usize::from(reads.queue_name.is_some()) + absence_count;
    if count == 0 {
        return Ok((
            RecordObservations {
                held: storage_types::Exact::empty(),
                queue: None,
            },
            Vec::new(),
        ));
    }
    let mut keys = Vec::with_capacity(count);
    let mut protections = Vec::with_capacity(count);
    for fence in reads.fences {
        keys.push(state_key(&fence.locator));
        protections.push(ReadProtection::Conflict);
    }
    if let Some(name) = reads.queue_name {
        keys.push(crate::queue_catalog::name_key(name));
        protections.push(ReadProtection::Snapshot);
    }
    if let Some(incarnation) = absence {
        for id in reads.message_ids {
            keys.push(crate::queue_message::state_key(incarnation, *id));
            protections.push(ReadProtection::Conflict);
        }
    }
    let mut values = native
        .get_batch_mixed_exact(keys, protections)
        .await?
        .into_vec();
    let present = values
        .split_off(count - absence_count)
        .into_iter()
        .map(|value| value.is_some())
        .collect();
    let queue = match reads.queue_name {
        Some(name) => values
            .pop()
            .flatten()
            .map(|bytes| crate::queue_catalog::decode_named_value(&bytes, name))
            .transpose()?,
        None => None,
    };
    let held = reads
        .fences
        .iter()
        .zip(values)
        .map(|(fence, value)| fence_holds(fence, value.as_deref()))
        .collect();
    Ok((
        RecordObservations {
            held: storage_types::Exact::try_new(reads.fences.len(), held)?,
            queue,
        },
        present,
    ))
}

/// One conflict wave of the fence locators: `true` where the stored state
/// still has the fence's generation and nonce.
async fn read_fences<'a, A: KvAttempt>(
    native: &mut A,
    fences: impl Iterator<Item = &'a DeliveryFence>,
) -> StorageResult<Vec<bool>> {
    let fences: Vec<&DeliveryFence> = fences.collect();
    if fences.is_empty() {
        return Ok(Vec::new());
    }
    let keys = fences
        .iter()
        .map(|fence| state_key(&fence.locator))
        .collect();
    let values = native
        .get_batch_exact(keys, ReadProtection::Conflict)
        .await?;
    Ok(fences
        .iter()
        .zip(values)
        .map(|(fence, value)| fence_holds(fence, value.as_deref()))
        .collect())
}

fn fence_holds(fence: &DeliveryFence, value: Option<&[u8]>) -> bool {
    value.is_some_and(|bytes| {
        DeliveryState::decode_value(&fence.locator, bytes).is_ok_and(|state| fence.holds(&state))
    })
}

/// The key moves; its value bytes stay as they are.
pub(crate) async fn move_undecodable_state<A: KvAttempt>(
    native: &mut A,
    locator: &DeliveryLocator,
    until: TimestampMillis,
) -> StorageResult<()> {
    let Some(value) = native
        .get(&state_key(locator), ReadProtection::Conflict)
        .await?
    else {
        return Err(StorageError::write_miss(
            "pubsub_delivery_state",
            MissExpectation::Present,
        ));
    };
    let moved = DeliveryLocator {
        due_at: until,
        ..locator.clone()
    };
    native.stage(vec![
        KvMutation::Delete {
            key: state_key(locator),
        },
        KvMutation::Put {
            key: state_key(&moved),
            value,
        },
    ])
}

/// Delete one state key that must still be there, and the record's payload
/// (one state key per record).
pub(crate) async fn delete_state_at<A: KvAttempt>(
    native: &mut A,
    locator: &DeliveryLocator,
) -> StorageResult<()> {
    if native
        .get(&state_key(locator), ReadProtection::Conflict)
        .await?
        .is_none()
    {
        return Err(StorageError::write_miss(
            "pubsub_delivery_state",
            MissExpectation::Present,
        ));
    }
    native.stage(vec![
        KvMutation::Delete {
            key: state_key(locator),
        },
        KvMutation::Delete {
            key: payload_key(&locator.id),
        },
    ])
}

/// Waves of one `scan(limit 1)` per open lane, each starting after the last
/// destination of its lane, until `limit` destinations are visited or every
/// lane is done; read-only at `Snapshot`.
pub(crate) async fn scan_destination_heads<A: KvAttempt>(
    native: &mut A,
    after: &SweepCursor,
    limit: NonZeroU32,
) -> StorageResult<SweepPage> {
    let mut lanes = lanes_of(after);
    let mut heads = Vec::new();
    let mut visited = 0_usize;
    while visited < limit.get() as usize {
        let open: Vec<usize> = (0..lanes.len())
            .filter(|lane| lanes[*lane] != SweepLane::Done)
            .collect();
        if open.is_empty() {
            break;
        }
        let ranges = open
            .iter()
            .map(|lane| lane_range(*lane, lanes[*lane]))
            .collect::<StorageResult<Vec<_>>>()?;
        let pages = native.scan_batch(ranges, ReadProtection::Snapshot).await?;
        let mut undecodable = Vec::new();
        for (lane, page) in open.into_iter().zip(pages) {
            let Some(entry) = page.entries.first() else {
                lanes[lane] = SweepLane::Done;
                continue;
            };
            let locator = decode_locator(&entry.key)?;
            lanes[lane] = SweepLane::After(Some(locator.destination));
            visited += 1;
            match DeliveryState::decode_value(&locator, &entry.value) {
                Ok(state) => heads.push(DestinationHead {
                    destination: state.destination,
                    head_due: locator.due_at,
                    head_retry_deadline: state.retry_deadline,
                }),
                Err(_) => undecodable.push(locator),
            }
        }
        heads.extend(undecodable_heads(native, undecodable).await?);
    }
    let is_done = lanes.iter().all(|lane| *lane == SweepLane::Done);
    Ok(SweepPage {
        heads,
        next: (!is_done).then_some(SweepCursor(lanes)),
    })
}

/// A head whose state does not decode still names its destination, so that
/// a claim is asked and sets the head aside (W3S review finding 2). The
/// destination text comes from the record's payload, which carries it; the
/// head's own due time stands for its retry deadline. A head whose payload
/// does not decode either is skipped.
async fn undecodable_heads<A: KvAttempt>(
    native: &mut A,
    locators: Vec<DeliveryLocator>,
) -> StorageResult<Vec<DestinationHead>> {
    if locators.is_empty() {
        return Ok(Vec::new());
    }
    let payloads = native
        .get_batch(
            locators
                .iter()
                .map(|locator| payload_key(&locator.id))
                .collect(),
            ReadProtection::Snapshot,
        )
        .await?;
    Ok(locators
        .into_iter()
        .zip(payloads)
        .filter_map(|(locator, bytes)| {
            let payload = DeliveryPayload::decode(&bytes?, &locator.id).ok()?;
            (payload.destination.id() == locator.destination).then_some(DestinationHead {
                destination: payload.destination,
                head_due: locator.due_at,
                head_retry_deadline: locator.due_at,
            })
        })
        .collect())
}

/// A cursor of one lane (a pass start) becomes one lane per id range: lanes
/// below the start are done for this pass, the start's lane continues after
/// it, and later lanes start at their beginning.
fn lanes_of(after: &SweepCursor) -> Vec<SweepLane> {
    if after.0.len() == usize::from(SWEEP_LANES) {
        return after.0.clone();
    }
    let start = match after.0.first() {
        Some(SweepLane::After(Some(id))) => Some(*id),
        Some(SweepLane::Done) => return vec![SweepLane::Done; usize::from(SWEEP_LANES)],
        _ => None,
    };
    (0..SWEEP_LANES)
        .map(|lane| match start {
            None => SweepLane::After(None),
            Some(id) => match lane.cmp(&(id.as_bytes()[0] >> 4)) {
                std::cmp::Ordering::Less => SweepLane::Done,
                std::cmp::Ordering::Equal => SweepLane::After(Some(id)),
                std::cmp::Ordering::Greater => SweepLane::After(None),
            },
        })
        .collect()
}

fn lane_range(lane: usize, position: SweepLane) -> StorageResult<KvRange> {
    let lane = u8::try_from(lane).map_err(|_| StorageError::internal("sweep lane is invalid"))?;
    let begin = match position {
        SweepLane::After(Some(id)) => upper_bound(&partition_prefix(id))?,
        SweepLane::After(None) | SweepLane::Done => {
            [PUBSUB_DELIVERY_STATE_PREFIX, &[lane << 4]].concat()
        }
    };
    let end = if lane + 1 == SWEEP_LANES {
        upper_bound(PUBSUB_DELIVERY_STATE_PREFIX)?
    } else {
        [PUBSUB_DELIVERY_STATE_PREFIX, &[(lane + 1) << 4]].concat()
    };
    Ok(KvRange {
        entry_bounds: None,
        begin,
        end,
        reverse: false,
        limit: NonZeroU32::MIN,
        max_bytes: CATALOG_BYTES,
    })
}

/// The audit read of one delivery: its payload names its partition; the
/// partition is scanned for the id (tests and workloads only).
pub(crate) async fn load_delivery<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    id: &DeliveryRecordId,
) -> StorageResult<Option<(StateRow, PayloadRow)>> {
    let Some(bytes) = native.get(&payload_key(id), protection).await? else {
        return Ok(None);
    };
    let payload = DeliveryPayload::decode(&bytes, id)?;
    let prefix = partition_prefix(payload.destination.id());
    let mut begin = prefix.clone();
    let end = upper_bound(&prefix)?;
    loop {
        let range = KvRange {
            entry_bounds: None,
            begin: begin.clone(),
            end: end.clone(),
            reverse: false,
            limit: CLAIM_WINDOW,
            max_bytes: CATALOG_BYTES,
        };
        let page = native.scan(range, protection).await?;
        for entry in &page.entries {
            let locator = decode_locator(&entry.key)?;
            if &locator.id == id {
                let (state, _) = decode_rows(locator, &entry.value, None);
                return Ok(Some((state, PayloadRow::Decoded(Box::new(payload)))));
            }
        }
        let Some(last) = page.entries.last().filter(|_| page.more) else {
            return Ok(None);
        };
        begin = [last.key.as_slice(), &[0]].concat();
    }
}

fn decode_rows(
    locator: DeliveryLocator,
    state: &[u8],
    payload: Option<&[u8]>,
) -> (StateRow, PayloadRow) {
    let payload = match payload {
        None => PayloadRow::Missing,
        Some(bytes) => DeliveryPayload::decode(bytes, &locator.id)
            .map_or(PayloadRow::Undecodable, |payload| {
                PayloadRow::Decoded(Box::new(payload))
            }),
    };
    let state = DeliveryState::decode_value(&locator, state)
        .map_or(StateRow::Undecodable(locator), StateRow::Decoded);
    (state, payload)
}

fn move_state(from: &DeliveryLocator, next: &DeliveryState) -> StorageResult<[KvMutation; 2]> {
    Ok([
        KvMutation::Delete {
            key: state_key(from),
        },
        KvMutation::Put {
            key: state_key(&next.locator()),
            value: next.encode_value()?,
        },
    ])
}

fn partition_range(destination: DestinationId, limit: NonZeroU32) -> StorageResult<KvRange> {
    let begin = partition_prefix(destination);
    Ok(KvRange {
        entry_bounds: None,
        end: upper_bound(&begin)?,
        begin,
        reverse: false,
        limit,
        max_bytes: CATALOG_BYTES,
    })
}

fn partition_prefix(destination: DestinationId) -> Vec<u8> {
    [PUBSUB_DELIVERY_STATE_PREFIX, destination.as_bytes()].concat()
}

pub(crate) fn state_key(locator: &DeliveryLocator) -> Vec<u8> {
    let ordered = (locator.due_at.timestamp_millis() as u64) ^ (1_u64 << 63);
    [
        partition_prefix(locator.destination).as_slice(),
        &ordered.to_be_bytes(),
        locator.id.0.as_bytes(),
    ]
    .concat()
}

pub(crate) fn payload_key(id: &DeliveryRecordId) -> Vec<u8> {
    [PUBSUB_DELIVERY_PAYLOAD_PREFIX, id.0.as_bytes()].concat()
}

fn decode_locator(key: &[u8]) -> StorageResult<DeliveryLocator> {
    let invalid = || StorageError::internal("KV delivery state key is invalid");
    let suffix = key
        .strip_prefix(PUBSUB_DELIVERY_STATE_PREFIX)
        .ok_or_else(invalid)?;
    let destination: [u8; 16] = suffix
        .get(..16)
        .and_then(|id| id.try_into().ok())
        .ok_or_else(invalid)?;
    let due: [u8; 8] = suffix
        .get(16..24)
        .and_then(|due| due.try_into().ok())
        .ok_or_else(invalid)?;
    let id = std::str::from_utf8(suffix.get(24..).ok_or_else(invalid)?).map_err(|_| invalid())?;
    Ok(DeliveryLocator {
        destination: DestinationId::from_bytes(destination),
        due_at: TimestampMillis::from_timestamp((u64::from_be_bytes(due) ^ (1_u64 << 63)) as i64),
        id: DeliveryRecordId(id.to_owned()),
    })
}

fn upper_bound(prefix: &[u8]) -> StorageResult<Vec<u8>> {
    prefix_upper_bound(prefix).ok_or_else(|| StorageError::internal("KV delivery bound is invalid"))
}
