//! Physical table-family pages selected by the shared reclaim transition.

use std::{future::Future, num::NonZeroU32};

use storage_provider::{
    TableReclaimAttempt, TableReclaimPhase, TableReclaimStep, TableReclaimTarget,
};
use storage_types::{ReadProtection, StorageError, StorageResult, TableHistoryId, storage_serde};

use crate::{
    KvAttempt, KvMutation, KvRange,
    catalog::{KvCatalogAttempt, metadata_key, table_id_from_history, verify_deleted_metadata},
    constants::{
        CATALOG_BYTES, GSI_BUILD_FAMILY, GSI_NAME_LOCK_FAMILY, GSI_PROJECTION_FAMILY,
        GSI_RUNNABLE_FAMILY, GSI_TOMBSTONE_FAMILY, PRIMARY_ITEM_FAMILY, PRIMARY_TOMBSTONE_PREFIX,
        TABLE_RECLAIM_PREFIX, TTL_INDEX_FAMILY,
    },
    range::prefix_upper_bound,
};

const RECLAIM_PAGE_ROWS: NonZeroU32 = NonZeroU32::MIN.saturating_add(64 - 1);

impl<A: KvAttempt> TableReclaimAttempt for KvCatalogAttempt<'_, A> {
    async fn load_table_system_rows(
        &mut self,
        history: TableHistoryId,
        limit: NonZeroU32,
    ) -> StorageResult<storage_provider::TableSystemHistoryPage> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result =
            crate::table_history_reclaim::load_table_system_rows(catalog.attempt, history, limit)
                .await;
        operation.complete(result)
    }

    async fn store_table_system_removals(
        &mut self,
        history: TableHistoryId,
        rows: &[storage_types::SystemHistoryRecord],
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = crate::table_history_reclaim::store_table_system_removals(
            catalog.attempt,
            history,
            rows,
        );
        operation.complete(result)
    }

    fn load_reclaim_target(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<TableReclaimTarget>>> + Send {
        load_target(self.attempt)
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
        let key = reclaim_key(table_id);
        let changes = match next {
            Some(phase) => {
                let mut next_target = target.clone();
                next_target.phase = phase;
                vec![KvMutation::Put {
                    key,
                    value: storage_serde::to_bytes(&next_target)?,
                }]
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

async fn load_target<A: KvAttempt>(attempt: &mut A) -> StorageResult<Option<TableReclaimTarget>> {
    let page = attempt
        .scan(
            KvRange::try_for_prefix(
                TABLE_RECLAIM_PREFIX.to_vec(),
                NonZeroU32::MIN,
                CATALOG_BYTES,
            )?,
            ReadProtection::Conflict,
        )
        .await?;
    let Some(entry) = page.entries.into_iter().next() else {
        return Ok(None);
    };
    let target: TableReclaimTarget = storage_serde::from_bytes(&entry.value)?;
    let table_id = table_id_from_history(target.history_id)?;
    if entry.key != reclaim_key(table_id) {
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
    if matches!(
        target.phase,
        TableReclaimPhase::ExternalData | TableReclaimPhase::SystemHistory
    ) {
        return Err(StorageError::internal(
            "Table reclaim phase requires its domain owner",
        ));
    }
    if target.phase == TableReclaimPhase::TableHistory {
        return crate::table_history_reclaim::delete_table_history_rows(attempt, target.history_id)
            .await;
    }
    if target.phase == TableReclaimPhase::ExplicitAppends {
        let stream = target.history_id.append_stream_name();
        crate::stream_write::counter::delete_user_sequence(attempt, &stream)?;
        return delete_family_pages(
            attempt,
            vec![
                crate::stream_write::key_layout::item_row_prefix(&stream)?,
                crate::stream_cursor::cursor_stream_prefix(&stream)?,
                crate::stream_write::receipt_prefix(target.history_id),
            ],
        )
        .await;
    }
    let table_id = table_id_from_history(target.history_id)?;
    if target.phase == TableReclaimPhase::MetadataCleanup {
        verify_gsi_cleanup(attempt, table_id).await?;
        crate::retention::delete_table_retention(
            attempt,
            storage_provider::StreamTrimScope::table(
                storage_provider::table_retention_scope_id(target.history_id),
                target.table_name.clone(),
            ),
        )
        .await?;
        attempt.stage(vec![KvMutation::Delete {
            key: crate::ttl_policy::ttl_config_key(table_id),
        }])?;
        return Ok(TableReclaimStep::Complete);
    }
    let prefixes = vec![family_prefix(target.phase, table_id, target.history_id)?];
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
    if matches!(key.first(), Some(&GSI_TOMBSTONE_FAMILY)) {
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
        TableReclaimPhase::PrimaryItems => PRIMARY_ITEM_FAMILY,
        TableReclaimPhase::GsiProjections => GSI_PROJECTION_FAMILY,
        TableReclaimPhase::GsiTombstones => GSI_TOMBSTONE_FAMILY,
        TableReclaimPhase::TtlIndex => TTL_INDEX_FAMILY,
        TableReclaimPhase::GsiNameLocks => GSI_NAME_LOCK_FAMILY,
        TableReclaimPhase::ExternalData
        | TableReclaimPhase::MetadataCleanup
        | TableReclaimPhase::ExplicitAppends
        | TableReclaimPhase::SystemHistory
        | TableReclaimPhase::TableHistory => {
            return Err(StorageError::internal(
                "KV reclaim phase has no single table family",
            ));
        }
    };
    let mut prefix = Vec::with_capacity(5);
    prefix.push(family);
    prefix.extend_from_slice(&table_id.to_be_bytes());
    Ok(prefix)
}
