//! Ordered private history-hold observations for SystemStream retention.

use std::num::NonZeroU32;

use storage_backfill::GsiBuild;
use storage_types::{
    StorageError, StorageResult, SystemStreamPosition, SystemStreamScope, storage_serde,
};

use super::key_layout::{gsi_build_key, hold_prefix};
use crate::{
    KvAttempt, KvRange, ReadProtection, constants::CATALOG_BYTES, range::prefix_upper_bound,
    stream_checkpoint,
};

/// Read at most one minimum hold per bounded catalog lineage. Build rows are
/// then fetched together and checked against the indexed hold identity.
pub(crate) async fn load_trim_protection_rows<A: KvAttempt>(
    attempt: &mut A,
    scope: &SystemStreamScope,
    lineages: &[SystemStreamPosition],
) -> StorageResult<(Vec<SystemStreamPosition>, Vec<SystemStreamPosition>)> {
    let frontier_prefix = stream_checkpoint::frontier_prefix(scope)?;
    let frontier_limit = u32::try_from(storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES + 1)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| StorageError::internal("SystemStream frontier limit is invalid"))?;
    let prefixes = lineages
        .iter()
        .map(|lineage| hold_prefix(scope, lineage))
        .collect::<StorageResult<Vec<_>>>()?;
    let mut ranges = vec![KvRange {
        entry_bounds: None,
        begin: frontier_prefix.clone(),
        end: prefix_upper_bound(&frontier_prefix).ok_or_else(|| {
            StorageError::internal("KV SystemStream frontier prefix has no upper bound")
        })?,
        reverse: false,
        limit: frontier_limit,
        max_bytes: CATALOG_BYTES,
    }];
    ranges.extend(
        prefixes
            .iter()
            .map(|prefix| {
                Ok(KvRange {
                    entry_bounds: None,
                    begin: prefix.clone(),
                    end: prefix_upper_bound(prefix).ok_or_else(|| {
                        StorageError::internal("KV GSI hold prefix has no upper bound")
                    })?,
                    reverse: false,
                    limit: NonZeroU32::MIN,
                    max_bytes: CATALOG_BYTES,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?,
    );
    let pages = attempt.scan_batch(ranges, ReadProtection::Conflict).await?;
    if pages.len() != lineages.len() + 1 {
        return Err(StorageError::internal("KV GSI hold scan count differs"));
    }
    let mut pages = pages.into_iter();
    let frontiers = stream_checkpoint::decode_protected_frontiers(
        pages.next().ok_or_else(invalid_hold)?,
        scope,
    )?;
    let mut positions = Vec::new();
    let mut keys = Vec::new();
    let mut incarnations = Vec::new();
    for ((page, prefix), lineage) in pages.zip(prefixes).zip(lineages) {
        if page.entries.len() > 1 {
            return Err(StorageError::internal(
                "KV GSI minimum hold scan is unbounded",
            ));
        }
        let Some(entry) = page.entries.into_iter().next() else {
            continue;
        };
        let suffix = entry
            .key
            .strip_prefix(prefix.as_slice())
            .ok_or_else(|| StorageError::internal("KV GSI hold has another scope or lineage"))?;
        let offset_len = suffix.len().checked_sub(6).ok_or_else(invalid_hold)?;
        let (offset, incarnation) = suffix
            .split_at_checked(offset_len)
            .ok_or_else(invalid_hold)?;
        let incarnation: [u8; 6] = incarnation.try_into().map_err(|_| invalid_hold())?;
        if !entry.value.is_empty() {
            return Err(invalid_hold());
        }
        let position = SystemStreamPosition::try_new(
            lineage.shard_id().as_str(),
            lineage.lineage_id().as_str(),
            "0",
        )
        .map_err(|_| invalid_hold())?;
        let position = match offset.len() {
            12 => position
                .with_versionstamp(offset.try_into().map_err(|_| invalid_hold())?)
                .map_err(|_| invalid_hold())?,
            8 => SystemStreamPosition::try_new(
                position.shard_id().as_str(),
                position.lineage_id().as_str(),
                format!(
                    "{:020}",
                    u64::from_be_bytes(offset.try_into().map_err(|_| invalid_hold())?)
                ),
            )
            .map_err(|_| invalid_hold())?,
            _ => return Err(invalid_hold()),
        };
        keys.push(gsi_build_key(
            u32::from_be_bytes(incarnation[..4].try_into().map_err(|_| invalid_hold())?),
            u16::from_be_bytes(incarnation[4..].try_into().map_err(|_| invalid_hold())?),
        ));
        incarnations.push(incarnation);
        positions.push(position);
    }
    let builds = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if builds.len() != positions.len() {
        return Err(StorageError::internal("KV GSI hold build count differs"));
    }
    for ((bytes, incarnation), position) in builds.into_iter().zip(incarnations).zip(&positions) {
        let bytes = bytes.ok_or_else(invalid_hold)?;
        let build: GsiBuild<Vec<u8>> = storage_serde::from_bytes(&bytes)?;
        if build.target.incarnation.as_ref() != incarnation.as_slice() {
            return Err(invalid_hold());
        }
        build.validate_private_hold(scope, position)?;
    }
    Ok((frontiers, positions))
}

fn invalid_hold() -> StorageError {
    StorageError::internal("KV GSI private history hold is invalid")
}
