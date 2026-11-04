//! Batched qualified table-index ranges on the caller's KV attempt.

use storage_types::{StorageError, StorageResult};
use stream_provider::{StreamRowPage, TableHistoryIndexAttempt, TableHistoryIndexRead};

use crate::{KvAttempt, KvRange, KvRangePage, catalog::KvCatalogAttempt};

impl<A: KvAttempt> TableHistoryIndexAttempt for KvCatalogAttempt<'_, A> {
    async fn load_table_history_rows(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> StorageResult<Vec<StreamRowPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let ranges = reads
                .iter()
                .map(prepare_index_range)
                .collect::<StorageResult<Vec<_>>>()?;
            let requested = ranges
                .iter()
                .filter_map(|range| range.as_ref().map(|(_, range)| range.clone()))
                .collect::<Vec<_>>();
            let pages = catalog
                .attempt
                .scan_batch(requested, catalog.protection)
                .await?;
            if pages.len() != ranges.iter().filter(|range| range.is_some()).count() {
                return Err(invalid_index_range());
            }
            let mut pages = pages.into_iter();
            ranges
                .into_iter()
                .map(|range| match range {
                    Some((prefix, _)) => {
                        decode_index_page(&prefix, pages.next().ok_or_else(invalid_index_range)?)
                    }
                    None => Ok(StreamRowPage {
                        rows: Vec::new(),
                        more: false,
                    }),
                })
                .collect()
        }
        .await;
        operation.complete(result)
    }
}

fn prepare_index_range(read: &TableHistoryIndexRead) -> StorageResult<Option<(Vec<u8>, KvRange)>> {
    let stream = read.lane.physical_stream_name()?;
    let Some((prefix, mut range)) = super::range::read_range(&stream, &read.row_read())? else {
        return Ok(None);
    };
    let mut through = prefix.clone();
    through.extend_from_slice(read.through.native_key().as_bytes());
    through.push(0);
    range.end = range.end.min(through);
    if range.begin >= range.end {
        return Ok(None);
    }
    Ok(Some((prefix, range)))
}

fn decode_index_page(prefix: &[u8], page: KvRangePage) -> StorageResult<StreamRowPage> {
    let rows = page
        .entries
        .into_iter()
        .map(|entry| {
            let id = super::row::decode_stream_key(prefix, &entry.key)?;
            stream_provider::decode_table_history_index_row(id, &entry.value)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(StreamRowPage {
        rows,
        more: page.more,
    })
}

fn invalid_index_range() -> StorageError {
    StorageError::internal("Invalid KV table history index range")
}
