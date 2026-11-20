//! Wake slot keys: one never-cleared key per slot; the value is the
//! commit versionstamp of the latest write that staged the slot.

use queue_provider::QueueWakeSlot;

use crate::constants::QUEUE_WAKE_PREFIX;

pub(crate) fn queue_wake_key(slot: QueueWakeSlot) -> Vec<u8> {
    [QUEUE_WAKE_PREFIX, &slot.index().to_be_bytes()].concat()
}

/// The slot of a logical key (provider prefix already removed).
#[cfg(feature = "foundationdb")]
pub(crate) fn decode_queue_wake_key(key: &[u8]) -> Option<QueueWakeSlot> {
    let bytes: [u8; 2] = key.strip_prefix(QUEUE_WAKE_PREFIX)?.try_into().ok()?;
    QueueWakeSlot::from_index(u16::from_be_bytes(bytes))
}
