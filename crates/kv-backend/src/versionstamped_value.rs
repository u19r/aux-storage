//! One complete logical stamp is lowered before any native mutation is staged.

use storage_types::{StorageError, StorageResult};

use crate::{
    KvMutation,
    constants::{KV_CHUNK_BYTES, KV_VERSIONSTAMP_PLACEHOLDER},
    mutation_preparation::expand_mutations,
    value_layout::ChunkAddress,
};

pub(crate) struct VersionstampedValue {
    pub(crate) changes: Vec<KvMutation>,
    pub(crate) stamped_key: Vec<u8>,
    pub(crate) stamped_value: Vec<u8>,
    pub(crate) offset: u32,
}

impl VersionstampedValue {
    pub(crate) fn try_new(
        key: Vec<u8>,
        value: Vec<u8>,
        offset: u32,
        native_limit: usize,
    ) -> StorageResult<Self> {
        let position = usize::try_from(offset).map_err(|_| invalid_slot())?;
        let stamp_bytes = KV_VERSIONSTAMP_PLACEHOLDER.len();
        if position
            .checked_add(stamp_bytes)
            .is_none_or(|end| end > value.len())
        {
            return Err(invalid_slot());
        }
        // The native versionstamped-value operand appends its offset trailer.
        let inline_limit = native_limit
            .checked_sub(std::mem::size_of::<u32>())
            .ok_or_else(invalid_slot)?;
        let encoded_length = value.len().checked_add(1).ok_or_else(invalid_slot)?;
        let (stamped_key, local_offset) = if encoded_length <= inline_limit {
            (key.clone(), position)
        } else {
            let local = position % KV_CHUNK_BYTES;
            if local + stamp_bytes > KV_CHUNK_BYTES {
                return Err(invalid_slot());
            }
            (
                ChunkAddress::prepare(key.as_slice())?.chunk_key(position / KV_CHUNK_BYTES)?,
                local,
            )
        };
        let offset = u32::try_from(local_offset).map_err(|_| invalid_slot())?;
        let mut changes = expand_mutations(vec![KvMutation::Put { key, value }], inline_limit)?;
        let position = changes
            .iter()
            .position(|change| matches!(change, KvMutation::Put { key, .. } if key == &stamped_key))
            .ok_or_else(invalid_slot)?;
        let KvMutation::Put {
            key: stamped_key,
            value: stamped_value,
        } = changes.remove(position)
        else {
            return Err(invalid_slot());
        };
        Ok(Self {
            changes,
            stamped_key,
            stamped_value,
            offset,
        })
    }
}

fn invalid_slot() -> StorageError {
    StorageError::internal("KV versionstamp slot exceeds its physical value layout")
}
