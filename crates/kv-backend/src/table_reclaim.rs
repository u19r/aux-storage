//! Physical table-family pages selected by the shared reclaim transition.

use std::{future::Future, num::NonZeroU32};

use storage_provider::{
    TableReclaimAttempt, TableReclaimPhase, TableReclaimStep, TableReclaimTarget,
};
use storage_types::{
    ReadProtection, StorageError, StorageResult, TableHistoryId, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    catalog::{
        KvCatalogAttempt, encode_name_lock, metadata_key, name_lock_key, table_id_from_history,
        verify_deleted_metadata,
    },
    constants::{
        CATALOG_BYTES, GSI_AUTHORITY_FAMILY, GSI_BRANCH_FAMILY, GSI_BUILD_FAMILY,
        GSI_NAME_LOCK_FAMILY, GSI_PROJECTION_FAMILY, GSI_RUNNABLE_FAMILY, GSI_TOMBSTONE_FAMILY,
        PRIMARY_ITEM_FAMILY, PRIMARY_TOMBSTONE_PREFIX, TABLE_RECLAIM_DUE_PREFIX,
        TABLE_RECLAIM_PREFIX, TTL_INDEX_FAMILY,
    },
    range::prefix_upper_bound,
};

const RECLAIM_PAGE_ROWS: NonZeroU32 = NonZeroU32::MIN.saturating_add(64 - 1);

impl<A: KvAttempt> TableReclaimAttempt for KvCatalogAttempt<'_, A> {
    fn load_reclaim_target(
        &mut self,
        observed_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<Option<TableReclaimTarget>>> + Send {
        load_target(self.attempt, observed_at)
    }

    fn delete_reclaim_page(
        &mut self,
        target: &TableReclaimTarget,
    ) -> impl Future<Output = StorageResult<TableReclaimStep>> + Send {
        delete_page(self.attempt, target)
    }

    async fn store_reclaim_phase(
        &mut self,
        target: &TableReclaimTarget,
        next: Option<TableReclaimPhase>,
    ) -> StorageResult<()> {
        let table_id = table_id_from_history(target.history_id)?;
        let key = marker_key(target, table_id);
        let changes = match next {
            Some(phase) => {
                let mut next_target = target.clone();
                next_target.phase = phase;
                let next_key = marker_key(&next_target, table_id);
                let mut changes = Vec::with_capacity(if key == next_key { 1 } else { 2 });
                if key != next_key {
                    changes.push(KvMutation::Delete { key });
                }
                changes.push(KvMutation::Put {
                    key: next_key,
                    value: storage_serde::to_bytes(&next_target)?,
                });
                changes
            }
            None => vec![
                KvMutation::Delete {
                    key: metadata_key(table_id),
                },
                KvMutation::Delete { key },
            ],
        };
        self.attempt.stage(changes)
    }
}

pub(crate) fn reclaim_key(table_id: u32) -> Vec<u8> {
    let mut key = TABLE_RECLAIM_PREFIX.to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

fn due_key(table_id: u32, locked_until: TimestampMillis) -> Vec<u8> {
    let mut key = Vec::with_capacity(TABLE_RECLAIM_DUE_PREFIX.len() + 12);
    key.extend_from_slice(TABLE_RECLAIM_DUE_PREFIX);
    let ordered = (locked_until.timestamp_millis() as u64 ^ (1_u64 << 63)).to_be_bytes();
    key.extend_from_slice(&ordered);
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

fn marker_key(target: &TableReclaimTarget, table_id: u32) -> Vec<u8> {
    if target.phase == TableReclaimPhase::NameReservation {
        due_key(table_id, target.name_locked_until)
    } else {
        reclaim_key(table_id)
    }
}

async fn load_target<A: KvAttempt>(
    attempt: &mut A,
    observed_at: TimestampMillis,
) -> StorageResult<Option<TableReclaimTarget>> {
    let core_end = prefix_upper_bound(TABLE_RECLAIM_PREFIX)
        .ok_or_else(|| StorageError::internal("KV reclaim prefix has no upper bound"))?;
    let due_end = if observed_at.timestamp_millis() == i64::MAX {
        prefix_upper_bound(TABLE_RECLAIM_DUE_PREFIX)
            .ok_or_else(|| StorageError::internal("KV reclaim due prefix has no upper bound"))?
    } else {
        let mut end = TABLE_RECLAIM_DUE_PREFIX.to_vec();
        let next = observed_at.timestamp_millis() + 1;
        end.extend_from_slice(&(next as u64 ^ (1_u64 << 63)).to_be_bytes());
        end
    };
    let pages = attempt
        .scan_batch(
            vec![
                KvRange {
                    entry_bounds: None,
                    begin: TABLE_RECLAIM_PREFIX.to_vec(),
                    end: core_end,
                    reverse: false,
                    limit: NonZeroU32::MIN,
                    max_bytes: CATALOG_BYTES,
                },
                KvRange {
                    entry_bounds: None,
                    begin: TABLE_RECLAIM_DUE_PREFIX.to_vec(),
                    end: due_end,
                    reverse: false,
                    limit: NonZeroU32::MIN,
                    max_bytes: CATALOG_BYTES,
                },
            ],
            ReadProtection::Conflict,
        )
        .await?;
    let [core, due]: [crate::KvRangePage; 2] = pages
        .try_into()
        .map_err(|_| StorageError::internal("KV reclaim scan batch count differs"))?;
    let Some((entry, due_marker)) = core
        .entries
        .into_iter()
        .next()
        .map(|entry| (entry, false))
        .or_else(|| due.entries.into_iter().next().map(|entry| (entry, true)))
    else {
        return Ok(None);
    };
    let target: TableReclaimTarget = storage_serde::from_bytes(&entry.value)?;
    let table_id = table_id_from_history(target.history_id)?;
    if entry.key != marker_key(&target, table_id)
        || due_marker != (target.phase == TableReclaimPhase::NameReservation)
    {
        return Err(StorageError::internal(
            "KV reclaim marker key differs from target",
        ));
    }
    let metadata = attempt
        .get(&metadata_key(table_id), ReadProtection::Conflict)
        .await?
        .ok_or_else(|| StorageError::internal("KV reclaim marker has no table metadata"))?;
    verify_deleted_metadata(&metadata, &target)?;
    Ok(Some(target))
}

async fn delete_page<A: KvAttempt>(
    attempt: &mut A,
    target: &TableReclaimTarget,
) -> StorageResult<TableReclaimStep> {
    if target.phase == TableReclaimPhase::NameReservation {
        return delete_name_reservation(attempt, target).await;
    }
    let table_id = table_id_from_history(target.history_id)?;
    if target.phase == TableReclaimPhase::MetadataCleanup {
        return verify_gsi_cleanup(attempt, table_id).await;
    }
    let mut prefixes = vec![family_prefix(target.phase, table_id, target.history_id)?];
    if target.phase == TableReclaimPhase::GsiProjections {
        for family in [GSI_AUTHORITY_FAMILY, GSI_BRANCH_FAMILY] {
            let mut prefix = vec![family];
            prefix.extend_from_slice(&table_id.to_be_bytes());
            prefixes.push(prefix);
        }
    }
    delete_family_pages(attempt, prefixes).await
}

pub(crate) async fn delete_family_pages<A: KvAttempt>(
    attempt: &mut A,
    prefixes: Vec<Vec<u8>>,
) -> StorageResult<TableReclaimStep> {
    let ranges = prefixes
        .iter()
        .map(|prefix| {
            let end = prefix_upper_bound(prefix)
                .ok_or_else(|| StorageError::internal("KV reclaim family has no upper bound"))?;
            Ok(KvRange {
                entry_bounds: None,
                begin: prefix.clone(),
                end,
                reverse: false,
                limit: RECLAIM_PAGE_ROWS,
                max_bytes: CATALOG_BYTES,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt
        .native_scan_batch(ranges, ReadProtection::Conflict)
        .await?;
    if pages.len() != prefixes.len() {
        return Err(StorageError::internal("KV reclaim scan count differs"));
    }
    let mut more = false;
    let mut changes = Vec::new();
    for (prefix, page) in prefixes.into_iter().zip(pages) {
        if page.entries.len() > RECLAIM_PAGE_ROWS.get() as usize
            || (page.more && page.entries.is_empty())
        {
            return Err(StorageError::internal("KV reclaim page cannot advance"));
        }
        more |= page.more;
        for entry in page.entries {
            if !entry.key.starts_with(&prefix) {
                return Err(StorageError::internal(
                    "KV reclaim scan crossed its table family",
                ));
            }
            changes.push(delete_reclaimed_key(entry.key));
        }
    }
    if !changes.is_empty() {
        attempt.stage(changes)?;
    }
    Ok(if more {
        TableReclaimStep::Advanced
    } else {
        TableReclaimStep::Complete
    })
}

fn delete_reclaimed_key(key: Vec<u8>) -> KvMutation {
    if matches!(
        key.first(),
        Some(&GSI_BRANCH_FAMILY)
            | Some(&GSI_PROJECTION_FAMILY)
            | Some(&GSI_AUTHORITY_FAMILY)
            | Some(&GSI_TOMBSTONE_FAMILY)
    ) {
        KvMutation::DeleteUnchunked { key }
    } else {
        KvMutation::Delete { key }
    }
}

async fn verify_gsi_cleanup<A: KvAttempt>(
    attempt: &mut A,
    table_id: u32,
) -> StorageResult<TableReclaimStep> {
    let ranges = [GSI_BUILD_FAMILY, GSI_RUNNABLE_FAMILY]
        .map(|family| {
            let mut begin = Vec::with_capacity(5);
            begin.push(family);
            begin.extend_from_slice(&table_id.to_be_bytes());
            let end = prefix_upper_bound(&begin)
                .ok_or_else(|| StorageError::internal("KV GSI family has no upper bound"))?;
            Ok(KvRange {
                entry_bounds: None,
                begin,
                end,
                reverse: false,
                limit: NonZeroU32::MIN,
                max_bytes: CATALOG_BYTES,
            })
        })
        .into_iter()
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt.scan_batch(ranges, ReadProtection::Conflict).await?;
    if pages.len() != 2
        || pages
            .iter()
            .any(|page| page.more || !page.entries.is_empty())
    {
        return Err(StorageError::internal(
            "KV deleted table still owns GSI builds",
        ));
    }
    Ok(TableReclaimStep::Complete)
}

async fn delete_name_reservation<A: KvAttempt>(
    attempt: &mut A,
    target: &TableReclaimTarget,
) -> StorageResult<TableReclaimStep> {
    let table_id = table_id_from_history(target.history_id)?;
    let key = name_lock_key(&target.table_name);
    let current = attempt.get(&key, ReadProtection::Conflict).await?;
    if current.as_deref() == Some(encode_name_lock(table_id, target.name_locked_until).as_slice()) {
        attempt.stage(vec![KvMutation::Delete { key }])?;
    }
    Ok(TableReclaimStep::Complete)
}

fn family_prefix(
    phase: TableReclaimPhase,
    table_id: u32,
    history_id: TableHistoryId,
) -> StorageResult<Vec<u8>> {
    let family = match phase {
        TableReclaimPhase::PrimaryTombstones => {
            let mut prefix = PRIMARY_TOMBSTONE_PREFIX.to_vec();
            prefix.extend_from_slice(history_id.as_ref());
            return Ok(prefix);
        }
        TableReclaimPhase::NameReservation => {
            return Err(StorageError::internal(
                "KV name reservation has no table family",
            ));
        }
        TableReclaimPhase::PrimaryItems => PRIMARY_ITEM_FAMILY,
        TableReclaimPhase::GsiProjections => GSI_PROJECTION_FAMILY,
        TableReclaimPhase::GsiTombstones => GSI_TOMBSTONE_FAMILY,
        TableReclaimPhase::TtlIndex => TTL_INDEX_FAMILY,
        TableReclaimPhase::GsiNameLocks => GSI_NAME_LOCK_FAMILY,
        TableReclaimPhase::MetadataCleanup => {
            return Err(StorageError::internal(
                "KV GSI cleanup has no single table family",
            ));
        }
    };
    let mut prefix = Vec::with_capacity(5);
    prefix.push(family);
    prefix.extend_from_slice(&table_id.to_be_bytes());
    Ok(prefix)
}
