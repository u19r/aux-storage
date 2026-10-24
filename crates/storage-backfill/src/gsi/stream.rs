use storage_types::{
    BoundTableInfo, ItemStreamVersion, StorageEnum, StorageError, StorageResult, StreamName,
    SystemStreamCheckpoint, WireItem, storage_serde,
};
use stream_provider::{
    DynamoDbStreamImages, StoredStreamPointer, StreamDataType, StreamItem,
    SystemStreamHistoryObservation, SystemStreamHistoryPage, SystemStreamHistoryRead,
    SystemStreamHistoryReadPage,
};

use super::{GsiSourceProjection, GsiStreamCandidate, GsiStreamPageEnd, GsiStreamSourcePage};

/// Assembles one logical GSI catch-up page from batched physical history reads.
pub struct GsiStreamPageAssembly<'a> {
    plan: &'a SystemStreamHistoryPage,
    observations: Vec<SystemStreamHistoryObservation<'a>>,
    more: Vec<bool>,
    candidates: Vec<GsiStreamCandidate>,
}

impl<'a> GsiStreamPageAssembly<'a> {
    #[must_use]
    pub fn new(plan: &'a SystemStreamHistoryPage) -> Self {
        let capacity = plan.as_ref().len();
        Self {
            plan,
            observations: Vec::with_capacity(capacity),
            more: Vec::with_capacity(capacity),
            candidates: Vec::new(),
        }
    }

    /// Returns true after a short physical page; later planned pages are
    /// unnecessary.
    pub fn push_pages(
        &mut self,
        reads: &'a [SystemStreamHistoryRead],
        pages: Vec<SystemStreamHistoryReadPage>,
    ) -> StorageResult<bool> {
        if pages.len() != reads.len() {
            return Err(StorageEnum::HistoryUnavailable.into());
        }
        for (read, page) in reads.iter().zip(pages) {
            let SystemStreamHistoryReadPage {
                retired_through,
                records,
                observations,
                pointers,
                more,
            } = page;
            if records.len() != observations.len() || observations.len() != pointers.len() {
                return Err(StorageEnum::HistoryUnavailable.into());
            }
            for pointer in pointers.into_iter().flatten() {
                self.candidates.push(GsiStreamCandidate::try_from(pointer)?);
            }
            let short = observations.len() < read.limit as usize;
            self.observations.push(SystemStreamHistoryObservation {
                retired_through,
                read,
                records: observations,
            });
            self.more.push(more);
            if short
                && (read.ordering == stream_provider::SystemStreamHistoryOrdering::Contiguous
                    || more)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn finish(
        self,
        table: &BoundTableInfo,
        through: &SystemStreamCheckpoint,
    ) -> StorageResult<GsiStreamSourcePage> {
        let next = self
            .plan
            .finish_bounded(&self.observations, &self.more)
            .map_err(|_| StorageEnum::HistoryUnavailable)?;
        let end = if next == *through {
            GsiStreamPageEnd::ReachedThrough
        } else {
            GsiStreamPageEnd::Continue(next)
        };
        Ok(GsiStreamSourcePage {
            table: table.clone(),
            end,
            candidates: self.candidates,
        })
    }
}

impl TryFrom<StreamItem> for GsiStreamCandidate {
    type Error = StorageError;

    fn try_from(item: StreamItem) -> StorageResult<Self> {
        if item.data_type != StreamDataType::StreamPointer
            || item.stream_name.as_ref() != Some(&StreamName::system_table_stream())
        {
            return Err(StorageError::internal(
                "GSI history row is not a SystemStream pointer",
            ));
        }
        Ok(Self {
            pointer_id: item.id,
            created_at: item.created_at,
            pointer: storage_serde::from_bytes(&item.data)?,
            referenced_items: None,
        })
    }
}

impl GsiStreamCandidate {
    pub fn into_projections(
        self,
        table: &storage_types::BoundTableInfo,
        index_name: &storage_types::IndexName,
    ) -> StorageResult<Vec<GsiSourceProjection>> {
        if !self.pointer.matches_table_history(table.history_id) {
            return Ok(Vec::new());
        }
        let (item_version, indexers, items) = match self.pointer {
            StoredStreamPointer::Embedded {
                table_name,
                item_revision,
                items,
                indexers,
                ..
            } => {
                if table_name != table.table_name {
                    return Ok(Vec::new());
                }
                let items = items
                    .into_iter()
                    .map(|item| StreamItem {
                        id: self.pointer_id,
                        stream_name: None,
                        data: item.data,
                        data_type: item.data_type,
                        created_at: self.created_at,
                    })
                    .collect();
                (item_revision, indexers, items)
            }
            StoredStreamPointer::Pointer {
                table_name,
                item_stream_version,
                item_revision,
                indexers,
                ..
            } => {
                if table_name != table.table_name {
                    return Ok(Vec::new());
                }
                let items = self.referenced_items.ok_or_else(|| {
                    StorageError::internal("GSI pointer target history is unavailable")
                })?;
                if items
                    .first()
                    .is_none_or(|item| ItemStreamVersion::from(item.id) != item_stream_version)
                {
                    return Err(StorageError::internal(
                        "GSI pointer target history is unavailable",
                    ));
                }
                (item_revision, indexers, items)
            }
        };
        let images = DynamoDbStreamImages::try_from(items.as_slice())?;
        let old_item = images.old.as_ref().map(WireItem::try_from).transpose()?;
        let new_item = images.new.as_ref().map(WireItem::try_from).transpose()?;
        GsiSourceProjection::from_stream_images(
            table,
            index_name,
            old_item,
            new_item,
            indexers,
            item_version,
        )
    }
}
