//! Bounded native heads and first chunks provide history cleanup metadata.

use std::num::{NonZeroU32, NonZeroUsize};

use storage_provider::maintenance::{ItemHistoryMetadataAttempt, ItemHistoryMetadataPage};
use storage_types::{
    ItemHistoryAddress, ItemStreamVersion, ReadProtection, StorageError, StorageResult,
    StreamItemId,
};
use stream_provider::StreamRowValue;

use crate::{
    KvAttempt, KvEntry, KvEntryBounds, KvRange, KvRangePage,
    catalog::KvCatalogAttempt,
    value_layout::{ValueHeader, parse_header},
    value_prefix::KvValuePrefix,
};

impl<A: KvAttempt> ItemHistoryMetadataAttempt for KvCatalogAttempt<'_, A> {
    async fn load_table_history_namespace(
        &mut self,
        history: storage_types::TableHistoryId,
        after: Option<&[u8]>,
    ) -> StorageResult<Option<storage_provider::maintenance::TableHistoryNamespace>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result =
            super::history_namespace::load_namespace(catalog.attempt, history, after).await;
        operation.complete(result)
    }

    async fn load_item_history_metadata_pages(
        &mut self,
        histories: &[ItemHistoryAddress],
    ) -> StorageResult<Vec<ItemHistoryMetadataPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = load_history_metadata_pages(catalog.attempt, histories).await;
        operation.complete(result)
    }
}

async fn load_history_metadata_pages<A: KvAttempt>(
    attempt: &A,
    histories: &[ItemHistoryAddress],
) -> StorageResult<Vec<ItemHistoryMetadataPage>> {
    if histories.is_empty() {
        return Ok(Vec::new());
    }
    let maximum_value = histories
        .iter()
        .try_fold(0usize, |maximum, history| {
            Ok::<_, StorageError>(
                maximum.max(StreamRowValue::history_metadata_prefix_bytes(history)?.get()),
            )
        })?
        .saturating_add(storage_types::LogicalItemSize::maximum_json_projection_bytes());
    let per_history = attempt
        .native_value_limit()
        .min(maximum_value)
        .saturating_mul(ItemHistoryMetadataPage::MAX_ENTRIES + 1)
        .max(1);
    let group_size = (crate::constants::HISTORY_METADATA_HEAD_BYTES / per_history).max(1);
    let mut pages = Vec::with_capacity(histories.len());
    for histories in histories.chunks(group_size) {
        pages.extend(load_metadata_group(attempt, histories).await?);
    }
    Ok(pages)
}

async fn load_metadata_group<A: KvAttempt>(
    attempt: &A,
    histories: &[ItemHistoryAddress],
) -> StorageResult<Vec<ItemHistoryMetadataPage>> {
    let (prefixes, ranges, bound) = prepare_metadata_ranges(histories)?;
    let call = attempt.settlement().begin_call()?;
    let loaded =
        crate::value_layout::load_range_heads(attempt, ranges, ReadProtection::Conflict).await;
    let pages = call.complete(loaded.inspect_err(|error| {
        storage_common::diagnostics::trace_native_failure(
            attempt.backend_engine(),
            storage_common::diagnostics::NativeOperation::KvScanBatch,
            error,
        )
    }))?;
    let pending = PreparedMetadataPrefixes::try_new(pages, bound)?;
    let pages = pending.load(attempt, bound).await?;
    pages
        .into_iter()
        .zip(prefixes)
        .zip(histories)
        .map(|((page, prefix), history)| decode_metadata_page(page, &prefix, history))
        .collect()
}

fn prepare_metadata_ranges(
    histories: &[ItemHistoryAddress],
) -> StorageResult<(Vec<Vec<u8>>, Vec<KvRange>, NonZeroUsize)> {
    let mut prefixes = Vec::with_capacity(histories.len());
    let mut ranges = Vec::with_capacity(histories.len());
    let mut bound = NonZeroUsize::MIN;
    for history in histories {
        let prefix =
            crate::stream_write::key_layout::item_row_prefix(&history.physical_stream_name()?)?;
        let prefix_bound = StreamRowValue::history_metadata_prefix_bytes(history)?;
        let value_bound = prefix_bound
            .saturating_add(storage_types::LogicalItemSize::maximum_json_projection_bytes());
        let key_bound = NonZeroUsize::new(prefix.len() + 12).ok_or_else(invalid_metadata_page)?;
        let page_bound = NonZeroUsize::new(
            value_bound
                .get()
                .saturating_add(key_bound.get())
                .saturating_mul(ItemHistoryMetadataPage::MAX_ENTRIES + 1),
        )
        .ok_or_else(invalid_metadata_page)?;
        let mut range = KvRange::try_for_prefix(
            prefix.clone(),
            NonZeroU32::MIN.saturating_add(ItemHistoryMetadataPage::MAX_ENTRIES as u32),
            page_bound,
        )?;
        range.entry_bounds = Some(KvEntryBounds::new(key_bound, value_bound));
        ranges.push(range);
        prefixes.push(prefix);
        bound = bound.max(prefix_bound);
    }
    Ok((prefixes, ranges, bound))
}

struct PendingMetadataRow {
    key: Vec<u8>,
    prefix: Option<KvValuePrefix>,
}

struct MetadataPageShape {
    count: usize,
    reached_tail: bool,
}

struct MetadataPrefixPage {
    rows: Vec<PendingMetadataRow>,
    reached_tail: bool,
}

struct PreparedMetadataPrefixes {
    rows: Vec<PendingMetadataRow>,
    shapes: Vec<MetadataPageShape>,
    chunked_rows: Vec<usize>,
}

impl PreparedMetadataPrefixes {
    fn try_new(pages: Vec<KvRangePage>, bound: NonZeroUsize) -> StorageResult<Self> {
        let mut prepared = Self {
            rows: Vec::new(),
            shapes: Vec::with_capacity(pages.len()),
            chunked_rows: Vec::new(),
        };
        for mut page in pages {
            if page.entries.len() > ItemHistoryMetadataPage::MAX_ENTRIES + 1 {
                return Err(invalid_metadata_page());
            }
            let reached_tail =
                !page.more && page.entries.len() <= ItemHistoryMetadataPage::MAX_ENTRIES;
            page.entries.truncate(ItemHistoryMetadataPage::MAX_ENTRIES);
            prepared.shapes.push(MetadataPageShape {
                count: page.entries.len(),
                reached_tail,
            });
            for entry in page.entries {
                prepared.include_head(entry, bound)?;
            }
        }
        Ok(prepared)
    }

    fn include_head(&mut self, entry: KvEntry, bound: NonZeroUsize) -> StorageResult<()> {
        let prefix = match parse_header(&entry.value)? {
            ValueHeader::Inline(value) => {
                let bytes = &value[..value.len().min(bound.get())];
                let mut copied = Vec::new();
                copied
                    .try_reserve_exact(bytes.len())
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                copied.extend_from_slice(bytes);
                Some(KvValuePrefix {
                    length: value.len(),
                    bytes: copied,
                })
            }
            ValueHeader::Chunked { .. } => {
                self.chunked_rows.push(self.rows.len());
                None
            }
        };
        self.rows.push(PendingMetadataRow {
            key: entry.key,
            prefix,
        });
        Ok(())
    }

    async fn load<A: KvAttempt>(
        mut self,
        attempt: &A,
        bound: NonZeroUsize,
    ) -> StorageResult<Vec<MetadataPrefixPage>> {
        if !self.chunked_rows.is_empty() {
            let keys = self
                .chunked_rows
                .iter()
                .map(|index| self.rows[*index].key.clone())
                .collect();
            let loaded = attempt
                .get_batch_prefix(keys, ReadProtection::Conflict, bound)
                .await?;
            if loaded.len() != self.chunked_rows.len() {
                return Err(invalid_metadata_page());
            }
            for (index, prefix) in self.chunked_rows.into_iter().zip(loaded) {
                self.rows[index].prefix = Some(prefix.ok_or_else(invalid_metadata_page)?);
            }
        }
        let mut rows = self.rows.into_iter();
        Ok(self
            .shapes
            .into_iter()
            .map(|shape| MetadataPrefixPage {
                rows: rows.by_ref().take(shape.count).collect(),
                reached_tail: shape.reached_tail,
            })
            .collect())
    }
}

fn decode_metadata_page(
    page: MetadataPrefixPage,
    prefix: &[u8],
    history: &ItemHistoryAddress,
) -> StorageResult<ItemHistoryMetadataPage> {
    let entries = page
        .rows
        .into_iter()
        .map(|row| decode_metadata_row(row, prefix, history))
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(ItemHistoryMetadataPage {
        entries,
        reached_tail: page.reached_tail,
    })
}

fn decode_metadata_row(
    row: PendingMetadataRow,
    prefix: &[u8],
    history: &ItemHistoryAddress,
) -> StorageResult<storage_types::ItemHistoryMetadata> {
    let suffix = row
        .key
        .strip_prefix(prefix)
        .ok_or_else(invalid_metadata_page)?;
    let id = StreamItemId::try_from(suffix).map_err(|_| invalid_metadata_page())?;
    let version = ItemStreamVersion::from(id);
    if StreamItemId::from(version) != id {
        return Err(invalid_metadata_page());
    }
    let value = row.prefix.ok_or_else(invalid_metadata_page)?;
    StreamRowValue::decode_history_header_prefix(&value.bytes, value.length, (history, version))
        .map(storage_types::ItemHistoryRecordHeader::into_metadata)
}

fn invalid_metadata_page() -> StorageError {
    StorageError::internal("Invalid KV item-history metadata page")
}
