//! Ordered keys shared by KV stream append, read, cursor, and catalog lowerers.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, SystemStreamPosition};
use stream_provider::{PointerOwner, StreamItem, StreamPointerReference};

use crate::constants::{
    SYSTEM_STREAM_EFFECT_PREFIX, SYSTEM_STREAM_ITEM_PREFIX, SYSTEM_STREAM_POINTER_OWNER_PREFIX,
    SYSTEM_STREAM_POINTER_REFERENCE_PREFIX, SYSTEM_STREAM_RECORD_PREFIX,
};

pub(super) fn item_row_key(row: &StreamItem) -> StorageResult<Vec<u8>> {
    let stream_name = row
        .stream_name
        .as_ref()
        .ok_or_else(|| StorageError::internal("planned Dynamo stream row has no stream name"))?;
    item_key(stream_name, row.id)
}

pub fn item_key(stream_name: &StreamName, id: StreamItemId) -> StorageResult<Vec<u8>> {
    let mut key = item_row_prefix(stream_name)?;
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}

pub fn item_row_prefix(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    let mut key = SYSTEM_STREAM_ITEM_PREFIX.to_vec();
    append_segment(&mut key, stream_name.as_ref())?;
    Ok(key)
}

pub fn pointer_owner_key(
    owner_stream: &StreamName,
    owner_item_id: StreamItemId,
) -> StorageResult<Vec<u8>> {
    let mut key = SYSTEM_STREAM_POINTER_OWNER_PREFIX.to_vec();
    append_segment(&mut key, owner_stream.as_ref())?;
    key.extend_from_slice(owner_item_id.as_bytes());
    Ok(key)
}

pub(crate) fn pointer_reference_prefix(item_stream: &StreamName) -> StorageResult<Vec<u8>> {
    let mut key = SYSTEM_STREAM_POINTER_REFERENCE_PREFIX.to_vec();
    append_segment(&mut key, item_stream.as_ref())?;
    Ok(key)
}

pub fn pointer_reference_key(reference: &StreamPointerReference) -> StorageResult<Vec<u8>> {
    let mut key = pointer_reference_prefix(&reference.item_stream)?;
    key.extend_from_slice(reference.item_item_id.unwrap_or_default().as_bytes());
    key.push(reference.owner as u8);
    append_segment(&mut key, reference.owner_stream.as_ref())?;
    key.extend_from_slice(reference.owner_item_id.as_bytes());
    Ok(key)
}

pub(crate) fn decode_pointer_reference_key(key: &[u8]) -> StorageResult<StreamPointerReference> {
    let rest = key
        .strip_prefix(SYSTEM_STREAM_POINTER_REFERENCE_PREFIX)
        .ok_or_else(invalid_pointer_reference)?;
    let (item_stream, rest) = read_segment(rest)?;
    let id_len = StreamItemId::default().as_bytes().len();
    let Some((item_id, rest)) = rest.split_at_checked(id_len) else {
        return Err(invalid_pointer_reference());
    };
    let Some((&owner, rest)) = rest.split_first() else {
        return Err(invalid_pointer_reference());
    };
    let (owner_stream, owner_id) = read_segment(rest)?;
    Ok(StreamPointerReference {
        owner: PointerOwner::try_from(owner)?,
        owner_stream: StreamName::new(owner_stream),
        owner_item_id: StreamItemId::try_from(owner_id).map_err(|_| invalid_pointer_reference())?,
        item_stream: StreamName::new(item_stream),
        item_item_id: Some(
            StreamItemId::try_from(item_id).map_err(|_| invalid_pointer_reference())?,
        )
        .filter(|id| *id != StreamItemId::default()),
    })
}

pub(crate) fn read_segment(bytes: &[u8]) -> StorageResult<(&[u8], &[u8])> {
    let Some((length, rest)) = bytes.split_at_checked(2) else {
        return Err(invalid_pointer_reference());
    };
    let length = u16::from_be_bytes([length[0], length[1]]) as usize;
    rest.split_at_checked(length)
        .ok_or_else(invalid_pointer_reference)
}

fn invalid_pointer_reference() -> StorageError {
    StorageError::internal("KV stream pointer reference key is invalid")
}

pub(crate) fn record_prefix(position: &SystemStreamPosition) -> Vec<u8> {
    let mut key = SYSTEM_STREAM_RECORD_PREFIX.to_vec();
    // Logical IDs exclude control characters, including NUL. Termination
    // preserves the same bytewise shard/lineage order as the SQL C collation
    // and the shared trim cursor comparator.
    key.extend_from_slice(position.shard_id().as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(position.lineage_id().as_str().as_bytes());
    key.push(0);
    key
}

pub(crate) fn record_key(position: &SystemStreamPosition) -> StorageResult<Vec<u8>> {
    let mut key = record_prefix(position);
    if let Some(stamp) = position.offset().versionstamp() {
        key.extend_from_slice(&stamp);
        return Ok(key);
    }
    let order = position.offset().order().map_err(|error| {
        StorageError::internal(&format!("invalid planned stream offset: {error}"))
    })?;
    key.extend_from_slice(&order.to_be_bytes());
    key.extend_from_slice(position.offset().as_str().as_bytes());
    Ok(key)
}

pub(crate) fn effect_key(effect_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(SYSTEM_STREAM_EFFECT_PREFIX.len() + effect_id.len());
    key.extend_from_slice(SYSTEM_STREAM_EFFECT_PREFIX);
    key.extend_from_slice(effect_id.as_bytes());
    key
}

pub(crate) fn append_segment(key: &mut Vec<u8>, bytes: &[u8]) -> StorageResult<()> {
    let len = u16::try_from(bytes.len()).map_err(|_| {
        StorageError::internal("planned Dynamo stream key segment exceeds 65535 bytes")
    })?;
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(bytes);
    Ok(())
}

/// The owner key supplies the committed identity for its reverse-key template.
pub(crate) fn resolve_pointer_owner(
    mut reverse_key: Vec<u8>,
    id: StreamItemId,
) -> StorageResult<Vec<u8>> {
    let length = reverse_key.len();
    let tail = reverse_key
        .get_mut(length.saturating_sub(12)..)
        .ok_or_else(invalid_pointer_reference)?;
    if tail.len() != 12 {
        return Err(invalid_pointer_reference());
    }
    if tail[..10] == [255; 10] {
        tail.copy_from_slice(id.as_bytes());
    }
    Ok(reverse_key)
}

pub(crate) fn retired_key(position: &SystemStreamPosition) -> Vec<u8> {
    let record = record_prefix(position);
    let mut key = crate::constants::SYSTEM_STREAM_RETIRED_PREFIX.to_vec();
    key.extend_from_slice(&record[SYSTEM_STREAM_RECORD_PREFIX.len()..]);
    key
}
