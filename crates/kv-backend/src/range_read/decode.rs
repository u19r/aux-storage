//! Decode one physical KV range entry and preserve shared page evidence.

use storage_provider::BoundRangeReadPlan;
use storage_types::{
    BoundTableInfo, ItemKey, LogicalItemSize, StorageError, StorageResult, WireItem,
};

use super::projection::RangeContent;
use crate::constants::{GSI_PROJECTION_KEY_PREFIX_BYTES, PRIMARY_ITEM_KEY_PREFIX_BYTES};

pub(super) fn decode_range_entry(
    table: &BoundTableInfo,
    bound: &BoundRangeReadPlan<'_>,
    physical: &[u8],
    content: RangeContent,
) -> StorageResult<(WireItem, ItemKey, LogicalItemSize)> {
    let wire = match content {
        RangeContent::Primary(primary) if bound.index.is_none() => primary.stored.item,
        RangeContent::Projection(projection) if bound.index.is_some() => projection.image,
        _ => {
            return Err(StorageError::internal(
                "Range content differs from selected native view",
            ));
        }
    };
    let (key, size) = range_evidence(table, bound, physical, &wire)?;
    Ok((wire, key, size))
}

pub(super) fn range_evidence(
    table: &BoundTableInfo,
    bound: &BoundRangeReadPlan<'_>,
    physical: &[u8],
    wire: &WireItem,
) -> StorageResult<(ItemKey, LogicalItemSize)> {
    let offset = if bound.index.is_some() {
        GSI_PROJECTION_KEY_PREFIX_BYTES
    } else {
        PRIMARY_ITEM_KEY_PREFIX_BYTES
    };
    let suffix = physical
        .get(offset..)
        .ok_or_else(|| StorageError::internal("native range key is truncated"))?;
    let key = if let Some(index) = bound.index.as_ref() {
        // Compact GSI partitions retain a hash, so the verified leaf supplies
        // its preimage. The caller checks the entire ordered key against the
        // root-fenced leaf, including its partition digest and table suffix.
        let attributes = storage_types::AttributeMap::try_from(wire)?;
        ItemKey::from_key_schema_for_index(
            table.table_name.clone(),
            &table.key_schema,
            &index.index_name,
            &index.key_schema,
            &attributes,
        )?
        .ok_or_else(|| StorageError::internal("KV GSI image lacks an index key"))?
    } else {
        ItemKey::from_sorted_storage_suffix(&table.info, None, suffix)?
    };
    if !wire.has_verified_keys(&key.key_attributes(&table.info)?)? {
        return Err(StorageError::internal(
            "KV range image differs from physical key",
        ));
    }
    let size = wire.logical_size()?;
    Ok((key, size))
}
