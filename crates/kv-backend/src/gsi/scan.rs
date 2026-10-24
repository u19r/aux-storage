//! Protected primary-item scan for one GSI build page.

use std::num::NonZeroU32;

use storage_backfill::{GsiPageLimit, GsiScanPageEnd, GsiScanSourcePage, GsiScannedSourceItem};
use storage_provider::maintenance::GsiScanSource;
use storage_types::{BoundTableInfo, StorageError, StorageResult};

use super::read::target_index;
use crate::{
    KvAttempt, KvRange, KvRangePage, ReadProtection,
    catalog::KvTableIdentity,
    constants::{PAGE_READ_BYTES, PRIMARY_ITEM_FAMILY},
    item_read::{hydrate_primary_items, primary_item_key},
    range::prefix_upper_bound,
    stream_history::capture_high,
};

pub(crate) async fn read_scan_source_page<A: KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    identity: &KvTableIdentity,
    source: GsiScanSource<'_>,
) -> StorageResult<GsiScanSourcePage<Vec<u8>>> {
    let GsiScanSource {
        table,
        target,
        after,
        limit,
    } = source;
    target_index(identity, target)?;
    let mut page = catalog
        .attempt
        .scan(
            scan_range(identity, after, limit)?,
            ReadProtection::Conflict,
        )
        .await?;
    crate::item_read::select_primary_prefix(table, &mut page)?;
    let next = page.entries.last().map(|entry| entry.key.clone());
    let more = page.more;
    let items = decode_scan_entries(catalog, identity, table, page).await?;
    let end = if more {
        GsiScanPageEnd::Continue(
            next.ok_or_else(|| StorageError::internal("KV GSI scan page has no cursor"))?,
        )
    } else {
        GsiScanPageEnd::Complete(capture_high(catalog.attempt).await?)
    };
    Ok(GsiScanSourcePage {
        table: table.info.clone(),
        expected_after: after.cloned(),
        end,
        items,
    })
}

async fn decode_scan_entries<A: KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    page: KvRangePage,
) -> StorageResult<Vec<GsiScannedSourceItem>> {
    let requests = page
        .entries
        .iter()
        .map(|entry| (table, Some(entry.value.as_slice())))
        .collect();
    let hydrated = hydrate_primary_items(catalog, requests, PAGE_READ_BYTES.get()).await?;
    let mut items = Vec::with_capacity(page.entries.len());
    for (entry, item) in page.entries.into_iter().zip(hydrated) {
        let item = item.ok_or_else(|| StorageError::internal("Missing GSI source content"))?;
        let stored = item.stored;
        let key = table.info.table_key_from_wire(&stored.item)?;
        if primary_item_key(identity, table, &key)? != entry.key {
            return Err(StorageError::internal(
                "KV GSI source item differs from its key",
            ));
        }
        items.push(GsiScannedSourceItem {
            item: stored.item,
            indexers: stored.indexers,
            revision: item.revision,
        });
    }
    Ok(items)
}

fn scan_range(
    identity: &KvTableIdentity,
    after: Option<&Vec<u8>>,
    limit: GsiPageLimit,
) -> StorageResult<KvRange> {
    let mut prefix = vec![PRIMARY_ITEM_FAMILY];
    prefix.extend_from_slice(&identity.table_id.to_be_bytes());
    let mut begin = prefix.clone();
    if let Some(after) = after {
        if !after.starts_with(&prefix) {
            return Err(StorageError::internal(
                "KV GSI scan cursor belongs to another table",
            ));
        }
        begin = after.clone();
        begin.push(0);
    }
    Ok(KvRange {
        entry_bounds: None,
        begin,
        end: prefix_upper_bound(&prefix)
            .ok_or_else(|| StorageError::internal("KV GSI scan prefix has no upper bound"))?,
        reverse: false,
        limit: NonZeroU32::new(u32::from(limit))
            .ok_or_else(|| StorageError::internal("GSI scan limit is zero"))?,
        max_bytes: PAGE_READ_BYTES,
    })
}
