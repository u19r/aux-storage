//! One qualified source prefix supplies exact GSI target/predecessor snapshots.
use storage_types::{
    BoundTableInfo, ItemHistoryChange, ItemHistoryPointer, ItemRevision, StorageEnum, StorageError,
    StorageResult, SystemStreamCheckpoint, TableHistoryId,
};
use stream_provider::{
    ItemHistoryImages, ItemHistoryReadAttempt, SystemStreamHistoryObservation,
    SystemStreamHistoryPage, SystemStreamHistoryRead, SystemStreamHistoryReadPage,
};

use super::{GsiSourceProjection, GsiStreamCandidate, GsiStreamPageEnd, GsiStreamSourcePage};

pub struct GsiStreamPageAssembly<'a> {
    plan: &'a SystemStreamHistoryPage,
    target: TableHistoryId,
    observations: Vec<SystemStreamHistoryObservation<'a>>,
    more: Vec<bool>,
    pointers: Vec<ItemHistoryPointer>,
    admitted_bytes: usize,
}

impl<'a> GsiStreamPageAssembly<'a> {
    pub fn new(plan: &'a SystemStreamHistoryPage, target: TableHistoryId) -> Self {
        Self {
            plan,
            target,
            observations: Vec::new(),
            more: Vec::new(),
            pointers: Vec::new(),
            admitted_bytes: 0,
        }
    }

    pub fn push_pages(
        &mut self,
        reads: &'a [SystemStreamHistoryRead],
        pages: Vec<SystemStreamHistoryReadPage>,
    ) -> StorageResult<bool> {
        if pages.len() != reads.len() {
            return Err(StorageEnum::HistoryUnavailable.into());
        }
        for (read, page) in reads.iter().zip(pages) {
            if self.push_page(read, page)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn push_page(
        &mut self,
        read: &'a SystemStreamHistoryRead,
        page: SystemStreamHistoryReadPage,
    ) -> StorageResult<bool> {
        if page.records.len() != page.observations.len() {
            return Err(StorageEnum::HistoryUnavailable.into());
        }
        let mut selected = Vec::with_capacity(page.records.len());
        let mut more = page.more;
        for (record, observation) in page.records.into_iter().zip(page.observations) {
            if record.position() != &observation.position {
                return Err(StorageEnum::HistoryUnavailable.into());
            }
            record.require_scope(self.plan.expanded_checkpoint().scope())?;
            if let Some(pointer) = record
                .pointer()
                .filter(|pointer| pointer.metadata().address().history_id == self.target)
            {
                if !self.admit_pointer(pointer)? {
                    more = true;
                    break;
                }
                self.pointers.push(pointer.clone());
            }
            selected.push(observation);
        }
        if more && selected.is_empty() {
            return Ok(true);
        }
        let short = selected.len() < read.limit as usize;
        self.observations.push(SystemStreamHistoryObservation {
            retired_through: page.retired_through,
            read,
            records: selected,
        });
        self.more.push(more);
        Ok(short
            && (read.ordering == stream_provider::SystemStreamHistoryOrdering::Contiguous || more))
    }

    fn admit_pointer(&mut self, pointer: &ItemHistoryPointer) -> StorageResult<bool> {
        let envelope =
            stream_provider::StreamRowValue::history_envelope_bytes(pointer.metadata().address())?;
        let bytes = pointer.record_bytes()
            + envelope
            + pointer
                .predecessor_record_bytes_bound()
                .map_or(0, |bytes| bytes + envelope);
        let next = self
            .admitted_bytes
            .checked_add(bytes)
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        if next > crate::constants::GSI_HISTORY_READ_BYTES.get() {
            return Ok(false);
        }
        self.admitted_bytes = next;
        Ok(true)
    }

    pub async fn finish<A: ItemHistoryReadAttempt>(
        self,
        attempt: &mut A,
        table: &BoundTableInfo,
        through: &SystemStreamCheckpoint,
    ) -> StorageResult<GsiStreamSourcePage> {
        let next = self
            .plan
            .finish_bounded(&self.observations, &self.more)
            .map_err(|_| StorageEnum::HistoryUnavailable)?;
        let resolved = stream_provider::resolve_item_history(
            attempt,
            &self.pointers,
            ItemHistoryImages::WithPredecessor,
            crate::constants::GSI_HISTORY_READ_BYTES,
        )
        .await?;
        let candidates = resolved
            .into_iter()
            .map(|history| {
                history
                    .map(|history| GsiStreamCandidate { history })
                    .ok_or_else(|| StorageEnum::HistoryUnavailable.into())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(GsiStreamSourcePage {
            table: table.clone(),
            end: if next == *through {
                GsiStreamPageEnd::ReachedThrough
            } else {
                GsiStreamPageEnd::Continue(next)
            },
            candidates,
        })
    }
}

impl GsiStreamCandidate {
    pub fn into_projections(
        self,
        table: &BoundTableInfo,
        index_name: &storage_types::IndexName,
    ) -> StorageResult<Vec<GsiSourceProjection>> {
        let current = &self.history.current;
        if current.metadata().address().history_id != table.history_id {
            return Err(StorageError::internal("GSI history has another table"));
        }
        let (new_item, indexers) = match current.change() {
            ItemHistoryChange::Image { item, indexers } => {
                (Some(item.clone()), indexers.names().to_vec())
            }
            ItemHistoryChange::Deleted { .. } => (None, Vec::new()),
        };
        let old_item = self
            .history
            .predecessor
            .as_ref()
            .map(|entry| match entry.change() {
                ItemHistoryChange::Image { item, .. } => Ok(item.clone()),
                ItemHistoryChange::Deleted { .. } => {
                    Err(StorageError::internal("GSI predecessor is not an image"))
                }
            })
            .transpose()?;
        GsiSourceProjection::from_stream_images(
            table,
            index_name,
            old_item,
            new_item,
            indexers,
            ItemRevision::try_from(current.metadata().version().get())?,
        )
    }
}
