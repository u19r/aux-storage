//! Regional pair state of one KV table: the binding port and the delete-marker
//! family.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use regional_replication::message::LineageRef;
use storage_provider::{
    LineageCursor, MarkerPage, ProgressDirection, RegionalCatalogAttempt, RegionalMarkerAttempt,
    RegionalProgressAttempt,
};
use storage_types::{
    BoundTableInfo, KeyAttributes, PairId, ReadProtection, RegionalBinding, RegionalMarker,
    StorageError, StorageResult, SystemStreamLineageId, SystemStreamOffset, SystemStreamShardId,
    TableBinding, regional_marker_item_key,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    catalog::{KvCatalogAttempt, table_id_from_history},
    range::prefix_upper_bound,
    table_key_layout::{TableSubFamily, table_key, table_prefix},
};

/// `T | table_id | 'D' | sorted primary key suffix`.
pub(crate) fn regional_marker_key(
    table_id: u32,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<Vec<u8>> {
    table_key(
        table_id,
        TableSubFamily::RegionalMarker,
        &[&regional_marker_item_key(table, key)?],
    )
}

pub(crate) fn put_marker(key: Vec<u8>, marker: RegionalMarker) -> KvMutation {
    KvMutation::PutUnchunked {
        key,
        value: marker.encode().to_vec(),
    }
}

pub(crate) fn delete_marker(key: Vec<u8>) -> KvMutation {
    KvMutation::DeleteUnchunked { key }
}

impl<A: KvAttempt> RegionalCatalogAttempt for KvCatalogAttempt<'_, A> {
    async fn store_regional_binding(
        &mut self,
        table: &BoundTableInfo,
        regional: Option<Arc<RegionalBinding>>,
    ) -> StorageResult<()> {
        let identity = self.load_table_identity(table).await?;
        let binding = TableBinding {
            regional,
            ..self.current_binding(table)
        };
        self.replace_table_layout(table, identity, table.info.as_ref().clone(), binding)
            .await?;
        Ok(())
    }
}

/// At most this many lineage records per (pair, direction); a table has few
/// lineages.
const PROGRESS_LIST_LIMIT: u32 = 4_096;
/// Value: 1 flag byte + an offset (bounded by
/// `SYSTEM_STREAM_OFFSET_MAX_BYTES`).
const PROGRESS_VALUE_BYTES: usize = 512;

fn invalid_progress() -> StorageError {
    StorageError::internal("invalid stored regional progress")
}

/// `T | tid | 'P' | pair | direction`.
pub(crate) fn progress_prefix(
    table_id: u32,
    pair: PairId,
    direction: Option<ProgressDirection>,
) -> StorageResult<Vec<u8>> {
    let direction = direction.map(|direction| [direction as u8]);
    table_key(
        table_id,
        TableSubFamily::RegionalProgress,
        &[
            pair.as_bytes(),
            direction.as_ref().map_or(&[][..], |byte| &byte[..]),
        ],
    )
}

pub(crate) fn progress_key(
    table_id: u32,
    pair: PairId,
    direction: ProgressDirection,
    lineage: &LineageRef,
) -> StorageResult<Vec<u8>> {
    table_key(
        table_id,
        TableSubFamily::RegionalProgress,
        &[
            pair.as_bytes(),
            &[direction as u8],
            lineage.shard.as_str().as_bytes(),
            &[0],
            lineage.lineage.as_str().as_bytes(),
            &[0],
        ],
    )
}

pub(crate) fn encode_progress(cursor: &LineageCursor) -> Vec<u8> {
    let offset = cursor.through.as_ref().map_or("", |offset| offset.as_str());
    let mut value = Vec::with_capacity(1 + offset.len());
    value.push(u8::from(cursor.sealed));
    value.extend_from_slice(offset.as_bytes());
    value
}

pub(crate) fn decode_progress(value: &[u8]) -> StorageResult<LineageCursor> {
    let (&flag, offset) = value.split_first().ok_or_else(invalid_progress)?;
    let sealed = match flag {
        0 => false,
        1 => true,
        _ => return Err(invalid_progress()),
    };
    let through = if offset.is_empty() {
        None
    } else {
        let text = std::str::from_utf8(offset).map_err(|_| invalid_progress())?;
        Some(SystemStreamOffset::try_new(text).map_err(|_| invalid_progress())?)
    };
    Ok(LineageCursor { through, sealed })
}

/// The lineage of a key below `progress_prefix(.., Some(direction))` (suffix
/// after the prefix).
pub(crate) fn decode_progress_lineage(suffix: &[u8]) -> StorageResult<LineageRef> {
    let mut parts = suffix.split(|byte| *byte == 0);
    let (Some(shard), Some(lineage), Some(rest), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid_progress());
    };
    if !rest.is_empty() {
        return Err(invalid_progress());
    }
    let text = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| invalid_progress())
    };
    Ok(LineageRef {
        shard: SystemStreamShardId::try_new(text(shard)?).map_err(|_| invalid_progress())?,
        lineage: SystemStreamLineageId::try_new(text(lineage)?).map_err(|_| invalid_progress())?,
    })
}

impl<A: KvAttempt> RegionalProgressAttempt for KvCatalogAttempt<'_, A> {
    async fn read_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
        lineages: &[LineageRef],
    ) -> StorageResult<Vec<Option<LineageCursor>>> {
        let table_id = table_id_from_history(table.history_id)?;
        let keys = lineages
            .iter()
            .map(|lineage| progress_key(table_id, pair, direction, lineage))
            .collect::<StorageResult<Vec<_>>>()?;
        let values = self
            .attempt
            .get_batch(keys, ReadProtection::Conflict)
            .await?;
        if values.len() != lineages.len() {
            return Err(StorageError::internal(
                "KV progress read returned the wrong count",
            ));
        }
        values
            .into_iter()
            .map(|value| value.as_deref().map(decode_progress).transpose())
            .collect()
    }

    async fn list_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
    ) -> StorageResult<Vec<(LineageRef, LineageCursor)>> {
        let table_id = table_id_from_history(table.history_id)?;
        let begin = progress_prefix(table_id, pair, Some(direction))?;
        let end = prefix_upper_bound(&begin).ok_or_else(invalid_progress)?;
        let page = self
            .attempt
            .scan_unchunked(
                KvRange {
                    begin: begin.clone(),
                    end,
                    reverse: false,
                    limit: NonZeroU32::new(PROGRESS_LIST_LIMIT).ok_or_else(invalid_progress)?,
                    max_bytes: NonZeroUsize::new(
                        PROGRESS_LIST_LIMIT as usize * (PROGRESS_VALUE_BYTES + 512),
                    )
                    .ok_or_else(invalid_progress)?,
                    entry_bounds: None,
                },
                ReadProtection::Snapshot,
                NonZeroUsize::new(PROGRESS_VALUE_BYTES).ok_or_else(invalid_progress)?,
            )
            .await?;
        if page.more {
            return Err(StorageError::internal(
                "a table has more regional progress records than the list bound",
            ));
        }
        page.entries
            .into_iter()
            .map(|entry| {
                let suffix = entry.key.get(begin.len()..).ok_or_else(invalid_progress)?;
                Ok((
                    decode_progress_lineage(suffix)?,
                    decode_progress(&entry.value)?,
                ))
            })
            .collect()
    }

    fn stage_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
        lineage: &LineageRef,
        cursor: &LineageCursor,
    ) -> StorageResult<()> {
        let key = progress_key(
            table_id_from_history(table.history_id)?,
            pair,
            direction,
            lineage,
        )?;
        self.append_dynamo_mutations(vec![KvMutation::PutUnchunked {
            key,
            value: encode_progress(cursor),
        }])
    }

    fn clear_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
    ) -> StorageResult<()> {
        let begin = progress_prefix(table_id_from_history(table.history_id)?, pair, None)?;
        let end = prefix_upper_bound(&begin).ok_or_else(invalid_progress)?;
        self.append_dynamo_mutations(vec![KvMutation::ClearUnchunkedRange { begin, end }])
    }

    fn clear_table_regional_progress(&mut self, table: &BoundTableInfo) -> StorageResult<()> {
        let begin = table_prefix(
            table_id_from_history(table.history_id)?,
            TableSubFamily::RegionalProgress,
        )
        .to_vec();
        let end = prefix_upper_bound(&begin).ok_or_else(invalid_progress)?;
        self.append_dynamo_mutations(vec![KvMutation::ClearUnchunkedRange { begin, end }])
    }
}

impl<A: KvAttempt> RegionalMarkerAttempt for KvCatalogAttempt<'_, A> {
    fn stage_regional_marker(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
        marker: Option<RegionalMarker>,
    ) -> StorageResult<()> {
        let marker_key = regional_marker_key(table_id_from_history(table.history_id)?, table, key)?;
        let change = match marker {
            Some(marker) => put_marker(marker_key, marker),
            None => delete_marker(marker_key),
        };
        self.append_dynamo_mutations(vec![change])
    }

    async fn list_regional_markers(
        &mut self,
        table: &BoundTableInfo,
        after: Option<&[u8]>,
        limit: NonZeroU32,
    ) -> StorageResult<MarkerPage> {
        let prefix = table_prefix(
            table_id_from_history(table.history_id)?,
            TableSubFamily::RegionalMarker,
        );
        let end =
            prefix_upper_bound(&prefix).ok_or_else(|| StorageError::internal("marker range"))?;
        let begin = match after {
            // The smallest key above `prefix | after`.
            Some(after) => [&prefix[..], after, &[0]].concat(),
            None => prefix.to_vec(),
        };
        let page = self
            .attempt
            .scan_unchunked(
                KvRange {
                    begin,
                    end,
                    reverse: false,
                    limit,
                    max_bytes: NonZeroUsize::new(limit.get() as usize * 2_048)
                        .ok_or_else(|| StorageError::internal("marker range"))?,
                    entry_bounds: None,
                },
                ReadProtection::Snapshot,
                NonZeroUsize::new(64)
                    .ok_or_else(|| StorageError::internal("marker value bound"))?,
            )
            .await?;
        let markers = page
            .entries
            .into_iter()
            .map(|entry| {
                let suffix = entry
                    .key
                    .get(prefix.len()..)
                    .ok_or_else(|| StorageError::internal("marker key"))?
                    .to_vec();
                Ok((suffix, RegionalMarker::decode(&entry.value)?))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        if markers.is_empty() && page.more {
            return Err(StorageError::internal("marker page made no progress"));
        }
        Ok(MarkerPage {
            markers,
            more: page.more,
        })
    }

    fn clear_regional_markers(&mut self, table: &BoundTableInfo) -> StorageResult<()> {
        let begin = table_prefix(
            table_id_from_history(table.history_id)?,
            TableSubFamily::RegionalMarker,
        )
        .to_vec();
        let end =
            prefix_upper_bound(&begin).ok_or_else(|| StorageError::internal("marker range"))?;
        self.append_dynamo_mutations(vec![KvMutation::ClearUnchunkedRange { begin, end }])
    }
}
