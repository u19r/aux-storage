use crate::partition_family::ordered_log_hash;

const PARTITIONED_QUEUE_LOGICAL_PREFIX: &[u8] = b"pqueue/";
const PARTITIONED_QUEUE_SCATTER_MARKER: &[u8] = b"auxq/";

pub(super) fn prefix_bytes(prefix: Option<&Vec<u8>>, key: &[u8]) -> Vec<u8> {
    if let Some(slot) = partitioned_queue_slot(key) {
        return scatter_partitioned_queue_key(prefix, slot, key);
    }
    if let Some(prefix) = prefix {
        let mut composed = prefix.clone();
        composed.extend_from_slice(key);
        composed
    } else {
        key.to_vec()
    }
}

pub(super) fn strip_prefix<'a>(key: &'a [u8], prefix: Option<&Vec<u8>>) -> &'a [u8] {
    let key = strip_partitioned_queue_scatter(key);
    if let Some(prefix) = prefix
        && key.starts_with(prefix)
    {
        return &key[prefix.len()..];
    }
    key
}

fn partitioned_queue_slot(key: &[u8]) -> Option<&[u8]> {
    let slot_start = PARTITIONED_QUEUE_LOGICAL_PREFIX.len();
    let slot_end = slot_start.checked_add(4)?;
    if key.len() <= slot_end || !key.starts_with(PARTITIONED_QUEUE_LOGICAL_PREFIX) {
        return None;
    }
    if key.get(slot_end) != Some(&b'/') {
        return None;
    }
    Some(&key[slot_start..slot_end])
}

fn scatter_partitioned_queue_key(
    prefix: Option<&Vec<u8>>,
    placement_slot: &[u8],
    key: &[u8],
) -> Vec<u8> {
    let bucket = 0x10u8.saturating_add((ordered_log_hash(placement_slot) % 224) as u8);
    let mut composed = Vec::with_capacity(
        1 + PARTITIONED_QUEUE_SCATTER_MARKER.len()
            + placement_slot.len()
            + 1
            + prefix.map_or(0, Vec::len)
            + key.len(),
    );
    composed.push(bucket);
    composed.extend_from_slice(PARTITIONED_QUEUE_SCATTER_MARKER);
    composed.extend_from_slice(placement_slot);
    composed.push(b'/');
    if let Some(prefix) = prefix {
        composed.extend_from_slice(prefix);
    }
    composed.extend_from_slice(key);
    composed
}

fn strip_partitioned_queue_scatter(key: &[u8]) -> &[u8] {
    let marker_start = 1;
    let marker_end = marker_start + PARTITIONED_QUEUE_SCATTER_MARKER.len();
    let slot_end = marker_end + 4;
    let payload_start = slot_end + 1;
    if key.len() <= payload_start
        || !key[marker_start..].starts_with(PARTITIONED_QUEUE_SCATTER_MARKER)
        || key.get(slot_end) != Some(&b'/')
    {
        return key;
    }
    &key[payload_start..]
}
