//! KV key layouts for GSI build records and materialized projections.

use sha2::{Digest as _, Sha256};
use storage_types::{
    AttributeMap, BoundTableInfo, IndexName, ItemKey, StorageError, StorageResult, WireItem,
};

use crate::{
    catalog::{KvIndexKind, KvTableIdentity},
    constants::{
        GSI_BUILD_FAMILY, GSI_NAME_LOCK_FAMILY, GSI_PROJECTION_FAMILY, GSI_RUNNABLE_FAMILY,
        GSI_TOMBSTONE_FAMILY,
    },
};

pub(super) struct ProjectionKeys {
    pub live: Vec<u8>,
    pub tombstone: Vec<u8>,
}

impl ProjectionKeys {
    pub(super) fn try_from_suffix(
        table_id: u32,
        index_id: u16,
        suffix: Vec<u8>,
    ) -> StorageResult<Self> {
        let physical = |family: u8, suffix: &[u8]| -> StorageResult<Vec<u8>> {
            let count = 7usize
                .checked_add(suffix.len())
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut key = Vec::new();
            key.try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            key.push(family);
            key.extend_from_slice(&index_incarnation(table_id, index_id));
            key.extend_from_slice(suffix);
            Ok(key)
        };
        Ok(Self {
            live: physical(GSI_PROJECTION_FAMILY, &suffix)?,
            tombstone: physical(GSI_TOMBSTONE_FAMILY, &suffix)?,
        })
    }
}

fn hash_partition(suffix: &[u8]) -> [u8; 32] {
    Sha256::digest(suffix).into()
}

pub(super) fn projection_keys(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    key: &storage_backfill::GsiProjectionKey,
) -> StorageResult<ProjectionKeys> {
    let index_id = identity
        .indexes
        .iter()
        .find(|candidate| {
            candidate.index_name == key.index_name && candidate.index_kind == KvIndexKind::UserGsi
        })
        .ok_or_else(|| StorageError::internal("GSI projection has no KV index identity"))?
        .index_id;
    ProjectionKeys::try_from_suffix(identity.table_id, index_id, projection_suffix(table, key)?)
}

pub(super) fn projection_suffix(
    table: &BoundTableInfo,
    key: &storage_backfill::GsiProjectionKey,
) -> StorageResult<Vec<u8>> {
    ordered_projection_suffix(&key.item_key(&table.info)?)
}

pub(crate) fn ordered_projection_key(
    identity: &KvTableIdentity,
    index_id: u16,
    key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    // Native order and public continuation evidence use the same complete key.
    Ok([
        vec![GSI_PROJECTION_FAMILY],
        index_incarnation(identity.table_id, index_id).to_vec(),
        ordered_projection_suffix(key)?,
    ]
    .concat())
}

pub(crate) fn ordered_projection_suffix(key: &ItemKey) -> StorageResult<Vec<u8>> {
    let word = compact_partition(key, key.sorted_storage_suffix()?)?;
    if word.len() > crate::constants::GSI_PROJECTION_SUFFIX_MAX_BYTES {
        return Err(StorageError::internal(
            "GSI ordered suffix exceeds its layout",
        ));
    }
    Ok(word)
}

pub(crate) fn ordered_projection_prefix(key: &ItemKey) -> StorageResult<Vec<u8>> {
    compact_partition(key, key.sorted_storage_prefix()?)
}

fn compact_partition(key: &ItemKey, encoded: Vec<u8>) -> StorageResult<Vec<u8>> {
    let name = key
        .index_id()
        .ok_or_else(|| StorageError::internal("GSI physical key has no index"))?;
    let partition = ItemKey::index_prefix(
        key.table_name().clone(),
        name.clone(),
        key.hash_key().clone(),
        None,
    )
    .sorted_storage_suffix()?;
    let tail = encoded
        .strip_prefix(partition.as_slice())
        .ok_or_else(|| StorageError::internal("GSI partition encoding differs from full key"))?;
    let count = 32usize
        .checked_add(tail.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut suffix = Vec::new();
    suffix
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    suffix.extend_from_slice(&hash_partition(&partition));
    suffix.extend_from_slice(tail);
    Ok(suffix)
}

pub(crate) fn gsi_name_lock_key(table_id: u32, index: &IndexName) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + index.as_ref().len());
    key.push(GSI_NAME_LOCK_FAMILY);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(index.as_ref().as_bytes());
    key
}

/// A GSI incarnation is the table ID followed by its stable index ID.
pub(crate) fn index_incarnation(table_id: u32, index_id: u16) -> [u8; 6] {
    let mut bytes = [0; 6];
    bytes[..4].copy_from_slice(&table_id.to_be_bytes());
    bytes[4..].copy_from_slice(&index_id.to_be_bytes());
    bytes
}

pub(super) fn gsi_build_key(table_id: u32, index_id: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(7);
    key.push(GSI_BUILD_FAMILY);
    key.extend_from_slice(&index_incarnation(table_id, index_id));
    key
}

pub(super) fn gsi_runnable_key(table_id: u32, index_id: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(7);
    key.push(GSI_RUNNABLE_FAMILY);
    key.extend_from_slice(&index_incarnation(table_id, index_id));
    key
}

pub(super) fn require_projection_key(
    table: &BoundTableInfo,
    index: &IndexName,
    physical: &[u8],
    image: &WireItem,
) -> StorageResult<()> {
    let definition = table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|definition| &definition.index_name == index)
        .ok_or_else(|| StorageError::internal("KV projection index is absent"))?;
    let attributes = AttributeMap::try_from(image)?;
    let key = ItemKey::from_key_schema_for_index(
        table.table_name.clone(),
        &table.key_schema,
        index,
        &definition.key_schema,
        &attributes,
    )?
    .ok_or_else(|| StorageError::internal("KV projection image lacks its key"))?;
    if physical.get(crate::constants::GSI_PROJECTION_KEY_PREFIX_BYTES..)
        != Some(ordered_projection_suffix(&key)?.as_slice())
    {
        return Err(StorageError::internal(
            "KV projection image differs from its physical key",
        ));
    }
    Ok(())
}
