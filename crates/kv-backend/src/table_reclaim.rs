//! Physical table-family pages selected by the shared reclaim transition.

use std::{future::Future, num::NonZeroU32};

use storage_backfill::GsiBuildTarget;
use storage_provider::{ReclaimAttempt, TableReclaimPhase, TableReclaimStep, TableReclaimTarget};
use storage_types::{
    ReadProtection, StorageError, StorageResult, TableHistoryId, maintenance::TargetIncarnation,
    storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    catalog::{
        KvCatalogAttempt, decode_index_incarnation, metadata_key, table_id_from_history,
        verify_deleted_metadata,
    },
    constants::{CATALOG_BYTES, GSI_RUNNABLE_FAMILY, TABLE_RECLAIM_PREFIX},
    range::prefix_upper_bound,
    table_key_layout::{TableSubFamily, table_data_range, table_prefix, table_stream_range},
};

const RECLAIM_PAGE_ROWS: NonZeroU32 = NonZeroU32::MIN.saturating_add(64 - 1);

/// How a reclaimed family stores values: a chunkable head is deleted with its
/// chunk span; an unchunked key is deleted alone.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimValues {
    Chunkable,
    Unchunked,
}

impl<A: KvAttempt> ReclaimAttempt for KvCatalogAttempt<'_, A> {
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
            None if clears_ranges(self.attempt) => {
                // The whole table root leaves in this commit: the data range
                // with the catalog record, and the stream part with every
                // table-owned chunk.
                let (begin, _) = table_data_range(table_id);
                let (_, end) = table_stream_range(table_id);
                self.attempt
                    .native_stage(vec![KvMutation::DeleteRange { begin, end }])?;
                vec![KvMutation::Delete { key }]
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

    async fn load_index_incarnation(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> StorageResult<Option<TargetIncarnation>> {
        let bytes = self
            .attempt
            .get(
                &metadata_key(table_id_from_history(history)?),
                ReadProtection::Conflict,
            )
            .await?
            .ok_or_else(|| {
                StorageError::internal("KV active index binding has no table metadata")
            })?;
        decode_index_incarnation(&bytes, history, target)
    }

    async fn delete_index_page(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
        _native: Option<storage_provider::NativeGsiRetirement>,
    ) -> StorageResult<TableReclaimStep> {
        let table_id = table_id_from_history(history)?;
        let scope = target.incarnation.as_ref();
        if scope.len() != 6 || scope[..4] != table_id.to_be_bytes() {
            return Err(StorageError::internal(
                "KV retired index scope differs from table history",
            ));
        }
        let index_id = u16::from_be_bytes([scope[4], scope[5]]);
        let prefix = crate::gsi::index_entry_prefix(table_id, index_id)?;
        crate::table_reclaim::delete_family_pages(
            self.attempt,
            vec![(prefix, crate::table_reclaim::ReclaimValues::Chunkable)],
        )
        .await
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
    if matches!(target.phase, TableReclaimPhase::ExternalData) {
        return Err(StorageError::internal(
            "Table reclaim phase requires its domain owner",
        ));
    }
    if target.phase == TableReclaimPhase::StreamEntries {
        return crate::table_history_reclaim::delete_table_history_rows(attempt, target.history_id)
            .await;
    }
    if target.phase == TableReclaimPhase::ExplicitAppends {
        let stream = target.history_id.append_stream_name();
        crate::stream_write::counter::delete_user_sequence(attempt, &stream)?;
        return delete_family_pages(
            attempt,
            vec![
                (
                    crate::stream_write::key_layout::item_row_prefix(&stream)?,
                    ReclaimValues::Chunkable,
                ),
                (
                    crate::stream_cursor::cursor_stream_prefix(&stream)?,
                    ReclaimValues::Chunkable,
                ),
                (
                    crate::stream_write::receipt_prefix(target.history_id)?,
                    ReclaimValues::Unchunked,
                ),
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
    match target.phase {
        // The final phase transition clears the whole data range natively.
        TableReclaimPhase::PrimaryItems if !clears_ranges(attempt) => {
            delete_table_data_page(attempt, table_id).await
        }
        _ => Ok(TableReclaimStep::Complete),
    }
}

pub(crate) async fn delete_family_pages<A: KvAttempt>(
    attempt: &mut A,
    families: Vec<(Vec<u8>, ReclaimValues)>,
) -> StorageResult<TableReclaimStep> {
    let ranges = families
        .iter()
        .map(|(prefix, _)| {
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
        .native_scan_batch_exact(ranges, ReadProtection::Conflict)
        .await?;
    let mut more = false;
    let mut changes = Vec::new();
    for ((prefix, values), page) in families.into_iter().zip(pages) {
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
            changes.push(match values {
                ReclaimValues::Chunkable => KvMutation::Delete { key: entry.key },
                ReclaimValues::Unchunked => KvMutation::DeleteUnchunked { key: entry.key },
            });
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

/// Engines whose attempt stages one native range clear.
fn clears_ranges<A: KvAttempt>(attempt: &A) -> bool {
    matches!(
        attempt.backend_engine(),
        storage_common::diagnostics::NativeEngine::FoundationDb
    )
}

/// One page of the table's data range, around the catalog record that the
/// final phase transition removes: one batched scan of the two halves.
async fn delete_table_data_page<A: KvAttempt>(
    attempt: &mut A,
    table_id: u32,
) -> StorageResult<TableReclaimStep> {
    let (begin, end) = table_data_range(table_id);
    let catalog = metadata_key(table_id);
    let after_catalog = [catalog.as_slice(), &[0]].concat();
    let ranges = [(begin, catalog), (after_catalog, end)]
        .map(|(begin, end)| KvRange {
            entry_bounds: None,
            begin,
            end,
            reverse: false,
            limit: RECLAIM_PAGE_ROWS,
            max_bytes: CATALOG_BYTES,
        })
        .to_vec();
    let pages = attempt
        .native_scan_batch_exact(ranges, ReadProtection::Conflict)
        .await?;
    let mut more = false;
    let mut changes = Vec::new();
    for page in pages {
        if page.entries.len() > RECLAIM_PAGE_ROWS.get() as usize
            || (page.more && page.entries.is_empty())
        {
            return Err(StorageError::internal("KV reclaim page cannot advance"));
        }
        more |= page.more;
        changes.extend(
            page.entries
                .into_iter()
                .map(|entry| KvMutation::Delete { key: entry.key }),
        );
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

async fn verify_gsi_cleanup<A: KvAttempt>(
    attempt: &mut A,
    table_id: u32,
) -> StorageResult<TableReclaimStep> {
    let mut runnable = Vec::with_capacity(5);
    runnable.push(GSI_RUNNABLE_FAMILY);
    runnable.extend_from_slice(&table_id.to_be_bytes());
    let ranges = [
        table_prefix(table_id, TableSubFamily::GsiBuild).to_vec(),
        runnable,
    ]
    .map(|begin| {
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
    let pages = attempt
        .scan_batch_exact(ranges, ReadProtection::Conflict)
        .await?;
    if pages
        .iter()
        .any(|page| page.more || !page.entries.is_empty())
    {
        return Err(StorageError::internal(
            "KV deleted table still owns GSI builds",
        ));
    }
    Ok(TableReclaimStep::Complete)
}
