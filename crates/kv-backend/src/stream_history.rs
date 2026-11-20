//! SystemStream history bounds on the caller's native KV snapshot.

use std::{collections::HashMap, num::NonZeroU64};

use futures::future::try_join;
use serde::{Deserialize, Serialize};
use storage_provider::{SYSTEM_STREAM_SNAPSHOT_BYTES, SYSTEM_STREAM_SNAPSHOT_ROWS};
use storage_types::{
    SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, StorageEnum, StorageError, StorageResult,
    SystemStreamCheckpoint, SystemStreamPosition, SystemStreamScope, storage_serde,
};
use stream_provider::{
    SystemStreamHistory, SystemStreamHistoryBounds, SystemStreamLineage, SystemStreamLineageCatalog,
};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::{
        SYSTEM_STREAM_CATALOG_EPOCH_KEY as CATALOG_EPOCH_KEY,
        SYSTEM_STREAM_LINEAGE_PREFIX as LINEAGE_PREFIX, SYSTEM_STREAM_SCHEMA_KEY as SCHEMA_KEY,
        SYSTEM_STREAM_SCOPE_KEY as SCOPE_KEY,
    },
};

// Persisted identifiers are open-ended strings, validated when the row is
// decoded.
#[derive(Deserialize, Serialize)]
struct ScopeRow {
    version: u8,
    scope_id: String,
    generation: u64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct LineageRow {
    pub scope_id: String,
    pub generation: u64,
    pub shard_id: String,
    pub lineage_id: String,
    pub parent_lineage_id: Option<String>,
    pub sealed: bool,
    pub initial_sequence: NonZeroU64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_after_offset: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaRow {
    version: u8,
    record_format: u8,
}

/// Install the authority bootstrap only on an empty store.
/// Existing data is validated rather than overwritten.
pub(crate) async fn initialize_system_stream<A: KvAttempt>(
    attempt: &mut A,
    initial_stream_lineages: storage_types::InitialStreamLineages,
) -> StorageResult<()> {
    let [scope, epoch] = attempt
        .get_batch(
            vec![SCOPE_KEY.to_vec(), CATALOG_EPOCH_KEY.to_vec()],
            ReadProtection::Conflict,
        )
        .await?
        .try_into()
        .map_err(|_| unavailable())?;
    if scope.is_none() {
        if epoch.is_some() {
            return Err(unavailable());
        }
        let rows = initial_rows(initial_stream_lineages)?;
        attempt
            .root_attempt()
            .native_record_catalog_change()
            .await?;
        attempt.stage(rows)?;
    }
    let schema = attempt
        .get(SCHEMA_KEY, ReadProtection::Conflict)
        .await?
        .ok_or_else(unavailable)?;
    let schema: SchemaRow = storage_serde::from_bytes(&schema)?;
    if schema.version != storage_types::SYSTEM_STREAM_SCOPE_VERSION
        || schema.record_format != STREAM_ENTRY_RECORD_FORMAT
    {
        return Err(unavailable());
    }
    capture_high(attempt).await?;
    Ok(())
}

fn initial_rows(
    initial_stream_lineages: storage_types::InitialStreamLineages,
) -> StorageResult<Vec<KvMutation>> {
    let scope = SystemStreamScope::try_new(storage_types::ambient::uuid_v4().to_string(), 1)
        .map_err(|_| unavailable())?;
    let scope_id = scope.scope_id().as_str().to_owned();
    let rows = [
        (
            SCOPE_KEY.to_vec(),
            storage_serde::to_bytes(&ScopeRow {
                version: scope.version(),
                scope_id: scope_id.clone(),
                generation: 1,
            })?,
        ),
        (
            SCHEMA_KEY.to_vec(),
            storage_serde::to_bytes(&SchemaRow {
                record_format: STREAM_ENTRY_RECORD_FORMAT,
                version: scope.version(),
            })?,
        ),
        (CATALOG_EPOCH_KEY.to_vec(), 1u64.to_be_bytes().to_vec()),
    ];
    let mut rows = rows.into_iter().collect::<Vec<_>>();
    let catalog = SystemStreamLineageCatalog::initial_roots(scope, initial_stream_lineages)
        .map_err(|_| unavailable())?;
    for lineage in catalog.iter() {
        rows.push((
            segmented_key(LINEAGE_PREFIX, lineage.lineage_id().as_str())?,
            encode_lineage(&LineageRow {
                scope_id: scope_id.clone(),
                generation: 1,
                shard_id: lineage.shard_id().as_str().to_owned(),
                lineage_id: lineage.lineage_id().as_str().to_owned(),
                parent_lineage_id: None,
                sealed: false,
                initial_sequence: NonZeroU64::MIN,
                sealed_after_offset: None,
            })?,
        ));
    }
    Ok(rows
        .into_iter()
        .map(|(key, value)| KvMutation::Put { key, value })
        .collect())
}

/// Capture a stream frontier from the native snapshot and protected catalog.
pub async fn capture_high<A: KvAttempt>(attempt: &mut A) -> StorageResult<SystemStreamCheckpoint> {
    let (_, history) = read_history_protected(attempt).await?;
    history.capture_high().map_err(|_| unavailable())
}

/// Load only protected immutable routing facts; snapshot bounds stay in full
/// history.
/// The keys and ranges `read_writer_routes` reads, for a cached-route
/// conflict without a read.
pub(crate) fn writer_route_keys() -> StorageResult<(Vec<Vec<u8>>, Vec<crate::KvRange>)> {
    Ok((
        vec![SCOPE_KEY.to_vec()],
        vec![prefix_range(LINEAGE_PREFIX)?],
    ))
}

pub(crate) async fn read_writer_routes<A: KvAttempt>(
    attempt: &A,
) -> StorageResult<std::sync::Arc<SystemStreamLineageCatalog>> {
    let (values, pages) = try_join(
        attempt.get_batch(vec![SCOPE_KEY.to_vec()], ReadProtection::Conflict),
        attempt.scan_batch(
            vec![prefix_range(LINEAGE_PREFIX)?],
            ReadProtection::Conflict,
        ),
    )
    .await?;
    let mut values = values.into_iter();
    let bytes = values.next().flatten().ok_or_else(unavailable)?;
    if values.next().is_some() {
        return Err(unavailable());
    }
    let scope = decode_scope(&bytes)?;
    let mut pages = pages.into_iter();
    let page = pages.next().ok_or_else(unavailable)?;
    if pages.next().is_some()
        || page.more
        || page.entries.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES
    {
        return Err(unavailable());
    }
    let lineages = page
        .entries
        .into_iter()
        .map(|entry| {
            decode_lineage_metadata(
                &scope,
                &entry.key,
                decode_stored_lineage(attempt, &entry.value)?,
            )
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(std::sync::Arc::new(
        SystemStreamLineageCatalog::try_new(scope, lineages).map_err(|_| unavailable())?,
    ))
}

pub(crate) async fn read_history_protected<A: KvAttempt>(
    attempt: &A,
) -> StorageResult<(u64, SystemStreamHistory)> {
    let (epoch, history) =
        read_history_with_protection(attempt, ReadProtection::Conflict, ReadProtection::Conflict)
            .await?;
    Ok((epoch, history))
}

pub(crate) async fn read_page_start<A: KvAttempt>(
    attempt: &A,
    protection: ReadProtection,
) -> StorageResult<SystemStreamHistory> {
    let (_, history) = read_history_with_protection(attempt, protection, protection).await?;
    Ok(history)
}

/// Read one page snapshot while protecting only the catalog identity.
pub(crate) async fn read_page_start_snapshot<A: KvAttempt>(
    attempt: &A,
    protection: ReadProtection,
) -> StorageResult<SystemStreamHistory> {
    let (_, history) =
        read_history_with_protection(attempt, protection, ReadProtection::Snapshot).await?;
    Ok(history)
}

async fn read_history_with_protection<A: KvAttempt>(
    attempt: &A,
    catalog_protection: ReadProtection,
    sequence_protection: ReadProtection,
) -> StorageResult<(u64, SystemStreamHistory)> {
    let keys = vec![CATALOG_EPOCH_KEY.to_vec(), SCOPE_KEY.to_vec()];
    let ranges = vec![prefix_range(LINEAGE_PREFIX)?];
    let ((values, pages), native_version) = try_join(
        try_join(
            attempt.get_batch(keys, catalog_protection),
            attempt.scan_batch(ranges, sequence_protection),
        ),
        async {
            if is_versionstamped(attempt) {
                attempt.native_read_version().await
            } else {
                attempt.native_snapshot_sequence().await
            }
        },
    )
    .await?;
    let mut values = values.into_iter();
    let epoch = values.next().ok_or_else(unavailable)?;
    let scope_bytes = values.next().ok_or_else(unavailable)?;
    if values.next().is_some() {
        return Err(unavailable());
    }
    let epoch = epoch.ok_or_else(unavailable)?;
    if epoch.len() != 8 {
        return Err(unavailable());
    }
    let epoch = u64::from_be_bytes(epoch.try_into().map_err(|_| unavailable())?);
    if epoch == 0 {
        return Err(unavailable());
    }

    let scope_bytes = scope_bytes.ok_or_else(unavailable)?;
    let scope = decode_scope(&scope_bytes)?;

    let mut pages = pages.into_iter();
    let page = pages.next().ok_or_else(unavailable)?;
    if page.more || page.entries.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        return Err(unavailable());
    }
    let lineages = page
        .entries
        .into_iter()
        .map(|entry| {
            let row = decode_stored_lineage(attempt, &entry.value)?;
            Ok((entry.key, row))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let history = if is_versionstamped(attempt) {
        if pages.next().is_some() {
            return Err(unavailable());
        }
        decode_native_history(scope, lineages, native_version.ok_or_else(unavailable)?)?
    } else {
        if pages.next().is_some() {
            return Err(unavailable());
        }
        decode_sparse_history(scope, lineages, native_version.ok_or_else(unavailable)?)?
    };
    Ok((epoch, history))
}

pub(crate) fn prefix_range(prefix: &[u8]) -> StorageResult<KvRange> {
    let mut end = prefix.to_vec();
    *end.last_mut().ok_or_else(unavailable)? += 1;
    Ok(KvRange {
        entry_bounds: None,
        begin: prefix.to_vec(),
        end,
        reverse: false,
        limit: SYSTEM_STREAM_SNAPSHOT_ROWS,
        max_bytes: SYSTEM_STREAM_SNAPSHOT_BYTES,
    })
}

fn decode_scope(bytes: &[u8]) -> StorageResult<SystemStreamScope> {
    let row: ScopeRow = storage_serde::from_bytes(bytes).map_err(|_| unavailable())?;
    SystemStreamScope::try_new_versioned(row.version, row.scope_id, row.generation)
        .map_err(|_| unavailable())
}

fn decode_stored_lineage<A: KvAttempt>(attempt: &A, bytes: &[u8]) -> StorageResult<LineageRow> {
    let row = decode_lineage_row(bytes)?;
    if (is_versionstamped(attempt) && row.sealed != (bytes.first() == Some(&NATIVE_SEALED_LINEAGE)))
        || (!is_versionstamped(attempt) && bytes.first() != Some(&ORDINARY_LINEAGE))
    {
        return Err(unavailable());
    }
    Ok(row)
}

fn decode_lineage_metadata(
    scope: &SystemStreamScope,
    key: &[u8],
    row: LineageRow,
) -> StorageResult<SystemStreamLineage> {
    if row.scope_id != scope.scope_id().as_str()
        || row.generation != scope.generation()
        || key != segmented_key(LINEAGE_PREFIX, &row.lineage_id)?
        || row.sealed != row.sealed_after_offset.is_some()
        || row
            .sealed_after_offset
            .is_some_and(|end| end < row.initial_sequence.get())
    {
        return Err(unavailable());
    }
    SystemStreamLineage::try_new(
        row.shard_id,
        row.lineage_id,
        row.parent_lineage_id,
        row.sealed,
    )
    .map_err(|_| unavailable())
}

fn decode_lineage(
    scope: &SystemStreamScope,
    key: Vec<u8>,
    row: LineageRow,
    by_shard: &HashMap<String, u64>,
) -> StorageResult<(SystemStreamLineage, SystemStreamHistoryBounds)> {
    let next_sequence = by_shard
        .get(&row.shard_id)
        .copied()
        .and_then(NonZeroU64::new)
        .ok_or_else(unavailable)?;
    let sealed_after = row
        .sealed_after_offset
        .map(|value| NonZeroU64::new(value).ok_or_else(unavailable))
        .transpose()?;
    let initial_sequence = row.initial_sequence;
    let lineage = decode_lineage_metadata(scope, &key, row)?;
    let bounds = SystemStreamHistoryBounds {
        lineage_id: lineage.lineage_id().clone(),
        initial_sequence,
        next_sequence,
        sealed_after,
    };
    Ok((lineage, bounds))
}

pub(crate) fn segmented_key(prefix: &[u8], value: &str) -> StorageResult<Vec<u8>> {
    let length = u32::try_from(value.len()).map_err(|_| unavailable())?;
    let mut key = Vec::with_capacity(prefix.len() + 4 + value.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(value.as_bytes());
    Ok(key)
}

fn unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}

const STREAM_ENTRY_RECORD_FORMAT: u8 = 8;
pub(crate) const ORDINARY_LINEAGE: u8 = 0;
pub(crate) const NATIVE_SEALED_LINEAGE: u8 = 1;

fn is_versionstamped<A: KvAttempt>(attempt: &A) -> bool {
    matches!(
        attempt.backend_engine(),
        storage_common::diagnostics::NativeEngine::FoundationDb
    )
}

pub(crate) fn encode_lineage(row: &LineageRow) -> StorageResult<Vec<u8>> {
    let mut bytes = vec![ORDINARY_LINEAGE];
    bytes.extend(storage_serde::to_bytes(row)?);
    Ok(bytes)
}

pub(crate) fn encode_native_sealed_lineage(row: &LineageRow) -> StorageResult<Vec<u8>> {
    if !row.sealed || row.sealed_after_offset.is_some() {
        return Err(unavailable());
    }
    let mut bytes = vec![NATIVE_SEALED_LINEAGE];
    bytes.extend_from_slice(&crate::constants::KV_VERSIONSTAMP_PLACEHOLDER);
    bytes.extend(storage_serde::to_bytes(row)?);
    Ok(bytes)
}

pub(crate) fn decode_lineage_row(bytes: &[u8]) -> StorageResult<LineageRow> {
    let (&tag, body) = bytes.split_first().ok_or_else(unavailable)?;
    match tag {
        ORDINARY_LINEAGE => storage_serde::from_bytes(body).map_err(|_| unavailable()),
        NATIVE_SEALED_LINEAGE => {
            let stamp: [u8; 10] = body
                .get(..10)
                .ok_or_else(unavailable)?
                .try_into()
                .map_err(|_| unavailable())?;
            let version = u64::from_be_bytes(stamp[..8].try_into().map_err(|_| unavailable())?);
            let end = version.checked_add(1).ok_or_else(unavailable)?;
            let mut row: LineageRow =
                storage_serde::from_bytes(&body[10..]).map_err(|_| unavailable())?;
            if !row.sealed || row.sealed_after_offset.is_some() || end < row.initial_sequence.get()
            {
                return Err(unavailable());
            }
            row.sealed_after_offset = Some(end);
            Ok(row)
        }
        _ => Err(unavailable()),
    }
}

pub(crate) fn decode_native_history(
    scope: SystemStreamScope,
    lineages: Vec<(Vec<u8>, LineageRow)>,
    version: u64,
) -> StorageResult<SystemStreamHistory> {
    let next = version.checked_add(1).ok_or_else(unavailable)?;
    let mut by_shard = HashMap::with_capacity(lineages.len());
    let mut frontiers = Vec::with_capacity(lineages.len());
    let next_sequence = NonZeroU64::new(next).ok_or_else(unavailable)?;
    for (_, row) in &lineages {
        let high = stream_provider::lineage_frontier_high(
            row.initial_sequence,
            next_sequence,
            row.sealed_after_offset,
            next,
        )?;
        if by_shard.insert(row.shard_id.clone(), high + 1).is_some() {
            return Err(unavailable());
        }
        frontiers.push(
            SystemStreamPosition::try_new(
                &row.shard_id,
                &row.lineage_id,
                format!("{high:020}-vsffffffff"),
            )
            .map_err(|_| unavailable())?,
        );
    }
    let mut catalog = Vec::with_capacity(lineages.len());
    let mut bounds = Vec::with_capacity(lineages.len());
    for (key, row) in lineages {
        let (lineage, bound) = decode_lineage(&scope, key, row, &by_shard)?;
        catalog.push(lineage);
        bounds.push(bound);
    }
    let catalog = SystemStreamLineageCatalog::try_new(scope, catalog).map_err(|_| unavailable())?;
    Ok(SystemStreamHistory::try_new(catalog, bounds)
        .map_err(|_| unavailable())?
        .with_commit_ordering(frontiers, NonZeroU64::new(next).ok_or_else(unavailable)?))
}

pub(crate) fn decode_sparse_history(
    scope: SystemStreamScope,
    lineages: Vec<(Vec<u8>, LineageRow)>,
    sequence: u64,
) -> StorageResult<SystemStreamHistory> {
    let next = sequence.checked_add(1).ok_or_else(unavailable)?;
    let mut by_shard = HashMap::with_capacity(lineages.len());
    let mut frontiers = Vec::with_capacity(lineages.len());
    let next_sequence = NonZeroU64::new(next).ok_or_else(unavailable)?;
    for (_, row) in &lineages {
        let high = stream_provider::lineage_frontier_high(
            row.initial_sequence,
            next_sequence,
            row.sealed_after_offset,
            next,
        )?;
        if by_shard.insert(row.shard_id.clone(), high + 1).is_some() {
            return Err(unavailable());
        }
        frontiers.push(
            SystemStreamPosition::try_new(&row.shard_id, &row.lineage_id, format!("{high:020}"))
                .map_err(|_| unavailable())?,
        );
    }
    let mut catalog = Vec::with_capacity(lineages.len());
    let mut bounds = Vec::with_capacity(lineages.len());
    for (key, row) in lineages {
        let (lineage, bound) = decode_lineage(&scope, key, row, &by_shard)?;
        catalog.push(lineage);
        bounds.push(bound);
    }
    let catalog = SystemStreamLineageCatalog::try_new(scope, catalog).map_err(|_| unavailable())?;
    Ok(SystemStreamHistory::try_new(catalog, bounds)
        .map_err(|_| unavailable())?
        .with_sparse_numeric_ordering(frontiers, NonZeroU64::new(next).ok_or_else(unavailable)?))
}
