//! Protected SystemStream allocation bounds on the caller's KV attempt.

use std::{collections::HashMap, num::NonZeroU64};

use futures::future::try_join;
use serde::{Deserialize, Serialize};
use storage_provider::{SYSTEM_STREAM_SNAPSHOT_BYTES, SYSTEM_STREAM_SNAPSHOT_ROWS};
use storage_types::{
    SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, StorageEnum, StorageError, StorageResult,
    SystemStreamCheckpoint, SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope,
    storage_serde,
};
use stream_provider::{
    DYNAMO_SYSTEM_LINEAGE_ID, DYNAMO_SYSTEM_SHARD_ID, SystemStreamCheckpointReceipt,
    SystemStreamHistory, SystemStreamHistoryBounds, SystemStreamLineage,
    SystemStreamLineageCatalog, SystemStreamTrimPageRequest,
};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::{
        SYSTEM_STREAM_CATALOG_EPOCH_KEY as CATALOG_EPOCH_KEY,
        SYSTEM_STREAM_LINEAGE_PREFIX as LINEAGE_PREFIX, SYSTEM_STREAM_SCHEMA_KEY as SCHEMA_KEY,
        SYSTEM_STREAM_SCOPE_KEY as SCOPE_KEY, SYSTEM_STREAM_SEQUENCE_PREFIX as SEQUENCE_PREFIX,
    },
    gsi, stream_checkpoint,
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

#[derive(Deserialize, Serialize)]
pub(crate) struct SequenceRow {
    pub scope_id: String,
    pub generation: u64,
    pub shard_id: String,
    pub next_offset: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaRow {
    version: u8,
    record_format: u8,
}

/// Install the authority bootstrap only on an empty store.
/// Existing data is validated rather than overwritten.
pub(crate) async fn initialize_system_stream<A: KvAttempt>(attempt: &mut A) -> StorageResult<()> {
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
        let mut rows = initial_rows()?;
        if attempt.native_read_version().await?.is_some() {
            for change in &mut rows {
                if let KvMutation::Put { key, value } = change
                    && key.starts_with(SEQUENCE_PREFIX)
                {
                    *value = vec![0; 12];
                }
            }
        }
        attempt.stage(rows)?;
    }
    let schema = attempt
        .get(SCHEMA_KEY, ReadProtection::Conflict)
        .await?
        .ok_or_else(unavailable)?;
    let schema: SchemaRow = storage_serde::from_bytes(&schema)?;
    if schema.version != storage_types::SYSTEM_STREAM_SCOPE_VERSION || schema.record_format != 5 {
        return Err(unavailable());
    }
    capture_high(attempt).await?;
    Ok(())
}

fn initial_rows() -> StorageResult<Vec<KvMutation>> {
    let scope = SystemStreamScope::try_new(uuid::Uuid::new_v4().to_string(), 1)
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
                record_format: 5,
                version: scope.version(),
            })?,
        ),
        (CATALOG_EPOCH_KEY.to_vec(), 1u64.to_be_bytes().to_vec()),
        (
            segmented_key(LINEAGE_PREFIX, DYNAMO_SYSTEM_LINEAGE_ID)?,
            storage_serde::to_bytes(&LineageRow {
                scope_id: scope_id.clone(),
                generation: 1,
                shard_id: DYNAMO_SYSTEM_SHARD_ID.to_owned(),
                lineage_id: DYNAMO_SYSTEM_LINEAGE_ID.to_owned(),
                parent_lineage_id: None,
                sealed: false,
                initial_sequence: NonZeroU64::MIN,
                sealed_after_offset: None,
            })?,
        ),
        (
            segmented_key(SEQUENCE_PREFIX, DYNAMO_SYSTEM_SHARD_ID)?,
            storage_serde::to_bytes(&SequenceRow {
                scope_id,
                generation: 1,
                shard_id: DYNAMO_SYSTEM_SHARD_ID.to_owned(),
                next_offset: 1,
            })?,
        ),
    ];
    Ok(rows
        .into_iter()
        .map(|(key, value)| KvMutation::Put { key, value })
        .collect())
}

/// Capture a stream frontier with protected sequence rows. The catalog epoch
/// fences topology changes; ordinary writers advance only their selected
/// shard sequence.
pub async fn capture_high<A: KvAttempt>(attempt: &mut A) -> StorageResult<SystemStreamCheckpoint> {
    let (_, history) = read_history_protected(attempt).await?;
    history.capture_high().map_err(|_| unavailable())
}

/// Confirm that the requested frontier remains reachable in protected history.
pub(crate) async fn validate_frontier<A: KvAttempt>(
    attempt: &mut A,
    current: &SystemStreamCheckpoint,
    through: &SystemStreamCheckpoint,
) -> StorageResult<()> {
    read_history_protected(attempt)
        .await?
        .1
        .plan_page(current, through, 1)
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    Ok(())
}

/// Protect and advance the selected logical sequence in this native attempt.
/// History discovery is a snapshot; the selected point read validates its
/// offset and establishes the native write conflict without locking shards
/// chosen by unrelated writers.
pub(crate) struct KvSystemWriteState {
    history: SystemStreamHistory,
    sequences: Vec<(Vec<u8>, SequenceRow)>,
    native_ordinal: u16,
    is_commit_ordered: bool,
}

impl KvSystemWriteState {
    pub(crate) fn new((_, history): (u64, SystemStreamHistory)) -> Self {
        Self {
            is_commit_ordered: history.uses_commit_versionstamps(),
            history,
            sequences: Vec::new(),
            native_ordinal: 0,
        }
    }

    pub(crate) async fn reserve<A: KvAttempt>(
        &mut self,
        attempt: &A,
        routing_key: &[u8],
    ) -> StorageResult<SystemStreamPosition> {
        if self.is_commit_ordered {
            let ordinal = self.native_ordinal;
            self.native_ordinal = ordinal.checked_add(1).ok_or_else(unavailable)?;
            return self.history.pending_commit_position(routing_key, ordinal);
        }
        let allocation = self.history.reserve_write(routing_key)?;
        let key = segmented_key(SEQUENCE_PREFIX, allocation.position.shard_id().as_str())?;
        if let Some((_, row)) = self.sequences.iter_mut().find(|(known, _)| known == &key) {
            if row.next_offset != allocation.expected_offset.get() {
                return Err(unavailable());
            }
            row.next_offset = allocation.next_offset.get();
            return Ok(allocation.position);
        }
        let bytes = attempt
            .get(&key, ReadProtection::Conflict)
            .await?
            .ok_or_else(unavailable)?;
        let row: SequenceRow = storage_serde::from_bytes(&bytes).map_err(|_| unavailable())?;
        let scope = self.history.as_ref().source_scope();
        if row.scope_id != scope.scope_id().as_str()
            || row.generation != scope.generation()
            || row.shard_id != allocation.position.shard_id().as_str()
            || row.next_offset != allocation.expected_offset.get()
        {
            return Err(unavailable());
        }
        let row = SequenceRow {
            next_offset: allocation.next_offset.get(),
            ..row
        };
        self.sequences.push((key, row));
        Ok(allocation.position)
    }

    pub(crate) fn drain_mutations(&mut self) -> StorageResult<Vec<KvMutation>> {
        let mut mutations = Vec::new();
        mutations
            .try_reserve_exact(self.sequences.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (key, row) in self.sequences.drain(..) {
            mutations.push(KvMutation::Put {
                key,
                value: storage_serde::to_bytes(&row)?,
            });
        }
        Ok(mutations)
    }
}

/// Decode the KV authority keyspace into the shared validated history model.
/// SQL reads different rows into that same model; its lineage and sequence
/// validation and writer selection live in `stream-provider`.
pub(crate) async fn read_history<A: KvAttempt>(
    attempt: &A,
) -> StorageResult<(u64, SystemStreamHistory)> {
    let (epoch, history, _) = read_history_with_protection(
        attempt,
        ReadProtection::Conflict,
        ReadProtection::Snapshot,
        None,
    )
    .await?;
    Ok((epoch, history))
}

pub(crate) async fn read_history_protected<A: KvAttempt>(
    attempt: &A,
) -> StorageResult<(u64, SystemStreamHistory)> {
    let (epoch, history, _) = read_history_with_protection(
        attempt,
        ReadProtection::Conflict,
        ReadProtection::Conflict,
        None,
    )
    .await?;
    Ok((epoch, history))
}

/// Keep catalog, checkpoint, and private hold observations in the caller's
/// protected attempt. The shared history model owns all trim decisions.
pub(crate) async fn load_trim_boundaries<A: KvAttempt>(
    attempt: &mut A,
    request: &SystemStreamTrimPageRequest,
) -> StorageResult<Vec<SystemStreamPosition>> {
    let (_, history) = read_history_protected(attempt).await?;
    history
        .validate_scope(request.scope())
        .map_err(|_| unavailable())?;
    let lineages = history.trim_hold_probes().map_err(|_| unavailable())?;
    let (frontiers, minima) =
        gsi::load_trim_protection_rows(attempt, request.scope(), &lineages).await?;
    history
        .trim_boundaries(request, &frontiers, &minima)
        .map_err(|_| unavailable())
}

/// Discovery needs one coherent read snapshot but does not commit a write.
pub(crate) async fn read_history_for_catalog<A: KvAttempt>(
    attempt: &A,
) -> StorageResult<SystemStreamHistory> {
    read_history_with_protection(
        attempt,
        ReadProtection::Snapshot,
        ReadProtection::Snapshot,
        None,
    )
    .await
    .map(|(_, history, _)| history)
}

pub(crate) async fn read_page_start<A: KvAttempt>(
    attempt: &A,
    token: Option<&SystemStreamCheckpointToken>,
) -> StorageResult<(SystemStreamHistory, Option<SystemStreamCheckpointReceipt>)> {
    let (_, history, receipt) = read_history_with_protection(
        attempt,
        ReadProtection::Conflict,
        ReadProtection::Conflict,
        token,
    )
    .await?;
    Ok((history, receipt))
}

async fn read_history_with_protection<A: KvAttempt>(
    attempt: &A,
    catalog_protection: ReadProtection,
    sequence_protection: ReadProtection,
    token: Option<&SystemStreamCheckpointToken>,
) -> StorageResult<(
    u64,
    SystemStreamHistory,
    Option<SystemStreamCheckpointReceipt>,
)> {
    let mut keys = vec![CATALOG_EPOCH_KEY.to_vec(), SCOPE_KEY.to_vec()];
    if let Some(token) = token {
        keys.push(stream_checkpoint::key_layout::token(token));
    }
    let ((values, pages), native_version) = try_join(
        try_join(
            attempt.get_batch(keys, catalog_protection),
            attempt.scan_batch(
                vec![
                    prefix_range(LINEAGE_PREFIX)?,
                    prefix_range(SEQUENCE_PREFIX)?,
                ],
                sequence_protection,
            ),
        ),
        attempt.native_read_version(),
    )
    .await?;
    let mut values = values.into_iter();
    let epoch = values.next().ok_or_else(unavailable)?;
    let scope_bytes = values.next().ok_or_else(unavailable)?;
    let receipt = if token.is_some() {
        values
            .next()
            .ok_or_else(unavailable)?
            .map(|bytes| storage_serde::from_bytes(&bytes))
            .transpose()?
    } else {
        None
    };
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
    let row: ScopeRow = storage_serde::from_bytes(&scope_bytes).map_err(|_| unavailable())?;
    let scope = SystemStreamScope::try_new_versioned(row.version, row.scope_id, row.generation)
        .map_err(|_| unavailable())?;

    let [lineages, sequences] = pages.try_into().map_err(|_| unavailable())?;
    let lineages = decode_rows::<LineageRow>(lineages)?;
    let mut commit_frontiers = Vec::new();
    let sequences = if native_version.is_some() {
        if sequences.more {
            return Err(unavailable());
        }
        sequences
            .entries
            .into_iter()
            .map(|entry| {
                let (key, bytes) = (entry.key, entry.value);
                let stamp: [u8; 12] = bytes.as_slice().try_into().map_err(|_| unavailable())?;
                let lineage = lineages
                    .iter()
                    .find(|(_, lineage)| {
                        segmented_key(SEQUENCE_PREFIX, &lineage.shard_id)
                            .is_ok_and(|expected| expected == key)
                    })
                    .ok_or_else(unavailable)?;
                let lineage = &lineage.1;
                let position =
                    SystemStreamPosition::try_new(&lineage.shard_id, &lineage.lineage_id, "0")
                        .map_err(|_| unavailable())?
                        .with_versionstamp(stamp)
                        .map_err(|_| unavailable())?;
                commit_frontiers.push(position);
                let native_high =
                    u64::from_be_bytes(stamp[..8].try_into().map_err(|_| unavailable())?)
                        .checked_add(1)
                        .ok_or_else(unavailable)?;
                let next_offset = lineage
                    .sealed_after_offset
                    .unwrap_or(native_high.max(lineage.initial_sequence.get()));
                Ok((
                    key,
                    SequenceRow {
                        scope_id: lineage.scope_id.clone(),
                        generation: lineage.generation,
                        shard_id: lineage.shard_id.clone(),
                        next_offset,
                    },
                ))
            })
            .collect::<StorageResult<Vec<_>>>()?
    } else {
        decode_rows::<SequenceRow>(sequences)?
    };
    let history = decode_history(scope, lineages, sequences)?;
    let history = if native_version.is_some() {
        history.with_commit_ordering(commit_frontiers)
    } else {
        history
    };
    Ok((epoch, history, receipt))
}

fn prefix_range(prefix: &[u8]) -> StorageResult<KvRange> {
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

fn decode_rows<T: serde::de::DeserializeOwned>(
    page: crate::KvRangePage,
) -> StorageResult<Vec<(Vec<u8>, T)>> {
    if page.more || page.entries.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        return Err(unavailable());
    }
    page.entries
        .into_iter()
        .map(|entry| {
            let row = storage_serde::from_bytes(&entry.value).map_err(|_| unavailable())?;
            Ok((entry.key, row))
        })
        .collect()
}

fn decode_history(
    scope: SystemStreamScope,
    lineages: Vec<(Vec<u8>, LineageRow)>,
    sequences: Vec<(Vec<u8>, SequenceRow)>,
) -> StorageResult<SystemStreamHistory> {
    let mut by_shard = HashMap::with_capacity(sequences.len());
    for (key, row) in sequences {
        if row.scope_id != scope.scope_id().as_str()
            || row.generation != scope.generation()
            || key != segmented_key(SEQUENCE_PREFIX, &row.shard_id)?
            || by_shard
                .insert(row.shard_id.clone(), row.next_offset)
                .is_some()
        {
            return Err(unavailable());
        }
    }
    let mut catalog = Vec::with_capacity(lineages.len());
    let mut bounds = Vec::with_capacity(lineages.len());
    for (key, row) in lineages {
        let (lineage, bound) = decode_lineage(&scope, key, row, &by_shard)?;
        catalog.push(lineage);
        bounds.push(bound);
    }
    if by_shard.keys().any(|shard| {
        !catalog
            .iter()
            .any(|lineage| lineage.shard_id().as_str() == shard)
    }) {
        return Err(unavailable());
    }
    let catalog = SystemStreamLineageCatalog::try_new(scope, catalog).map_err(|_| unavailable())?;
    SystemStreamHistory::try_new(catalog, bounds).map_err(|_| unavailable())
}

fn decode_lineage(
    scope: &SystemStreamScope,
    key: Vec<u8>,
    row: LineageRow,
    by_shard: &HashMap<String, u64>,
) -> StorageResult<(SystemStreamLineage, SystemStreamHistoryBounds)> {
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
    let next_sequence = by_shard
        .get(&row.shard_id)
        .copied()
        .and_then(NonZeroU64::new)
        .ok_or_else(unavailable)?;
    let sealed_after = row
        .sealed_after_offset
        .map(|value| NonZeroU64::new(value).ok_or_else(unavailable))
        .transpose()?;
    let lineage = SystemStreamLineage::try_new(
        row.shard_id,
        row.lineage_id,
        row.parent_lineage_id,
        row.sealed,
    )
    .map_err(|_| unavailable())?;
    let bounds = SystemStreamHistoryBounds {
        lineage_id: lineage.lineage_id().clone(),
        initial_sequence: row.initial_sequence,
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
