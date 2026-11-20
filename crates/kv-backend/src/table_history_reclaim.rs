//! One incarnation's entry keys determine exact feed deletions without payload
//! hydration.

use storage_provider::TableReclaimStep;
use storage_types::{ReadProtection, StorageError, StorageResult, TableHistoryId};

use crate::{KvAttempt, KvMutation, KvRange, constants::CATALOG_BYTES};

pub(crate) async fn delete_table_history_rows<A: KvAttempt>(
    attempt: &mut A,
    history: TableHistoryId,
) -> StorageResult<TableReclaimStep> {
    let prefix = crate::stream_write::key_layout::table_entry_prefix(history)?;
    let range = KvRange::try_for_prefix(
        prefix.clone(),
        crate::constants::TABLE_HISTORY_RECLAIM_ROWS,
        CATALOG_BYTES,
    )?;
    // Native heads suffice: the entry suffix is exactly the feed address. The
    // ordinary delete lowerer clears any value chunks in this same attempt.
    let pages = attempt
        .native_scan_batch(vec![range], ReadProtection::Conflict)
        .await?;
    let [page]: [crate::KvRangePage; 1] = pages.try_into().map_err(|_| invalid_page())?;
    if page.entries.len() > crate::constants::TABLE_HISTORY_RECLAIM_ROWS.get() as usize
        || page.more && page.entries.is_empty()
    {
        return Err(invalid_page());
    }
    let mut deletes = Vec::with_capacity(page.entries.len() * 2);
    for entry in page.entries {
        let suffix = entry
            .key
            .strip_prefix(prefix.as_slice())
            .filter(|suffix| !suffix.is_empty())
            .ok_or_else(invalid_page)?;
        let mut feed = crate::constants::SYSTEM_STREAM_RECORD_PREFIX.to_vec();
        feed.extend_from_slice(suffix);
        deletes.push(KvMutation::Delete { key: entry.key });
        deletes.push(KvMutation::Delete { key: feed });
    }
    attempt.stage(deletes)?;
    Ok(if page.more {
        TableReclaimStep::Advanced
    } else {
        TableReclaimStep::Complete
    })
}

fn invalid_page() -> StorageError {
    StorageError::internal("Invalid table-owned canonical entry page")
}
