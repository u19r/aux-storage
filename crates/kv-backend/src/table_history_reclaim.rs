//! The immutable table ID bounds source-index and history-row cleanup.

use std::num::{NonZeroU32, NonZeroUsize};

use storage_provider::{TableReclaimStep, TableSystemHistoryPage};
use storage_types::{
    ReadProtection, StorageError, StorageResult, SystemHistoryRecord, TableHistoryId, storage_serde,
};

use crate::{
    KvAttempt, KvEntryBounds, KvMutation, KvRange, constants::CATALOG_BYTES,
    stream_write::key_layout,
};

pub(crate) async fn load_table_system_rows<A: KvAttempt>(
    attempt: &mut A,
    history: TableHistoryId,
    limit: NonZeroU32,
) -> StorageResult<TableSystemHistoryPage> {
    let prefix = key_layout::table_record_prefix(history);
    let range = KvRange::try_for_prefix(prefix.clone(), limit.saturating_add(1), CATALOG_BYTES)?;
    let page = attempt.scan(range, ReadProtection::Conflict).await?;
    let more = page.more || page.entries.len() > limit.get() as usize;
    let keys = page
        .entries
        .into_iter()
        .take(limit.get() as usize)
        .map(|entry| key_layout::table_record_source_key(history, &entry.key))
        .collect::<StorageResult<Vec<_>>>()?;
    let values = attempt
        .get_batch(keys.clone(), ReadProtection::Conflict)
        .await?;
    if values.len() != keys.len() {
        return Err(invalid_page());
    }
    let records = keys
        .into_iter()
        .zip(values)
        .map(|(key, value)| {
            crate::stream_write::record::decode_record(&key, &value.ok_or_else(invalid_page)?)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(TableSystemHistoryPage { records, more })
}

pub(crate) fn store_table_system_removals<A: KvAttempt>(
    attempt: &mut A,
    history: TableHistoryId,
    rows: &[SystemHistoryRecord],
) -> StorageResult<()> {
    let mut changes = Vec::with_capacity(rows.len() * 2);
    for record in rows {
        if record.pointer().is_some() {
            return Err(invalid_page());
        }
        changes.push(KvMutation::Put {
            key: key_layout::record_key(record.position())?,
            value: storage_serde::to_bytes(record)?,
        });
        changes.push(KvMutation::Delete {
            key: key_layout::table_record_key(history, record.position())?,
        });
    }
    attempt.stage(changes)
}

pub(crate) async fn delete_table_history_rows<A: KvAttempt>(
    attempt: &mut A,
    history: TableHistoryId,
) -> StorageResult<TableReclaimStep> {
    let prefix = key_layout::table_history_row_prefix(history);
    let mut range = KvRange::try_for_prefix(
        prefix,
        crate::constants::TABLE_HISTORY_RECLAIM_ROWS,
        CATALOG_BYTES,
    )?;
    let maximum = storage_types::ItemHistoryRecord::encoded_length_bound()
        + storage_types::ItemHistoryRecord::metadata_length_bound();
    // The logical history address is already bounded by its metadata envelope.
    range.entry_bounds = Some(KvEntryBounds::new(
        NonZeroUsize::new(storage_types::ItemHistoryRecord::metadata_length_bound())
            .ok_or_else(invalid_page)?,
        NonZeroUsize::new(maximum).ok_or_else(invalid_page)?,
    ));
    let pages = attempt
        .native_scan_batch(vec![range], ReadProtection::Conflict)
        .await?;
    let [page]: [crate::KvRangePage; 1] = pages.try_into().map_err(|_| invalid_page())?;
    let more = page.more;
    let deletes = page
        .entries
        .into_iter()
        .map(|entry| KvMutation::Delete { key: entry.key })
        .collect();
    attempt.stage(deletes)?;
    Ok(if more {
        TableReclaimStep::Advanced
    } else {
        TableReclaimStep::Complete
    })
}

fn invalid_page() -> StorageError {
    StorageError::internal("Invalid table-owned source history page")
}
