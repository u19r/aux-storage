//! Exact snapshot resolution shares visibility, admission and deduplication.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

use storage_types::{
    ItemHistoryEntry, ItemHistoryKind, ItemHistoryPointer, ItemHistoryRecordHeader,
    ItemHistoryReference, StorageError, StorageResult, TableHistoryId, TableHistorySchema,
    TableHistoryState,
};

use crate::{ItemHistoryReadAttempt, ItemHistoryReadBatch, StreamRowValue};

#[derive(Clone, Copy)]
pub enum ItemHistoryImages {
    Snapshot,
    WithPredecessor,
}

pub struct ResolvedItemHistory {
    pub table: Arc<TableHistorySchema>,
    pub current: Arc<ItemHistoryEntry>,
    pub predecessor: Option<Arc<ItemHistoryEntry>>,
}

impl TryFrom<&ResolvedItemHistory> for storage_types::StreamRecord {
    type Error = StorageError;

    fn try_from(resolved: &ResolvedItemHistory) -> StorageResult<Self> {
        use storage_types::{ItemHistoryChange, ItemHistoryPublication, KeyAttributes};
        let ItemHistoryPublication::Event { table_position, .. } =
            resolved.current.metadata().publication()
        else {
            return Err(StorageError::internal(
                "Unpublished history has no stream record",
            ));
        };
        let (keys, item) = match resolved.current.change() {
            ItemHistoryChange::Image { item, .. } => {
                let item = item.to_attribute_map()?;
                (
                    KeyAttributes::try_from_schema(&resolved.table.key_schema, &item)?,
                    Some(item),
                )
            }
            ItemHistoryChange::Deleted { key } => (
                KeyAttributes::try_from_schema(&resolved.table.key_schema, key)?,
                None,
            ),
        };
        Ok(Self {
            cursor: None,
            source_table_name: Some(storage_types::TableName::new(
                resolved.table.source_table_name.as_str(),
            )),
            keys,
            sequence_number: table_position.offset().as_str().to_owned(),
            item,
            system_position: Some(table_position.clone()),
        })
    }
}

/// Inactive table slots remain empty so a System page can advance its source
/// observations without exposing that table's payload. A bound table reader
/// requires its own slots to be present. All reads join the caller's snapshot.
pub async fn resolve_item_history<A: ItemHistoryReadAttempt>(
    attempt: &mut A,
    pointers: &[ItemHistoryPointer],
    images: ItemHistoryImages,
    maximum_bytes: NonZeroUsize,
) -> StorageResult<Vec<Option<ResolvedItemHistory>>> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let result = async {
        let active = load_active_histories(operation.as_mut(), pointers).await?;
        let plan = HistoryResolutionPlan::try_new(pointers, &active, images, maximum_bytes)?;
        if plan.references.is_empty() {
            return Ok(pointers.iter().map(|_| None).collect());
        }
        let headers = operation
            .as_mut()
            .load_item_history_headers(plan.read())
            .await?;
        plan.require_headers(pointers, &headers)?;
        let entries = operation
            .as_mut()
            .load_item_history_entries(plan.read())
            .await?;
        plan.resolve_entries(headers, entries)
    }
    .await;
    operation.complete(result)
}

async fn load_active_histories<A: ItemHistoryReadAttempt>(
    attempt: &mut A,
    pointers: &[ItemHistoryPointer],
) -> StorageResult<HashMap<TableHistoryId, Arc<TableHistorySchema>>> {
    let mut seen = HashSet::new();
    let histories = pointers
        .iter()
        .map(|pointer| pointer.metadata().address().history_id)
        .filter(|history| seen.insert(*history))
        .collect::<Vec<_>>();
    if histories.is_empty() {
        return Ok(HashMap::new());
    }
    let states = attempt.load_item_history_table_states(&histories).await?;
    if states.len() != histories.len() {
        return Err(invalid_resolution());
    }
    Ok(histories
        .into_iter()
        .zip(states)
        .filter_map(|(history, state)| match state {
            Some(TableHistoryState::Active(schema)) => Some((history, schema)),
            _ => None,
        })
        .collect())
}

struct HistoryResolutionIndex {
    table: Arc<TableHistorySchema>,
    current: usize,
    predecessor: Option<usize>,
}

struct HistoryResolutionPlan {
    references: Vec<ItemHistoryReference>,
    bounds: Vec<usize>,
    slots: Vec<Option<HistoryResolutionIndex>>,
    maximum_bytes: NonZeroUsize,
}

impl HistoryResolutionPlan {
    fn try_new(
        pointers: &[ItemHistoryPointer],
        active: &HashMap<TableHistoryId, Arc<TableHistorySchema>>,
        images: ItemHistoryImages,
        maximum_bytes: NonZeroUsize,
    ) -> StorageResult<Self> {
        let mut plan = Self {
            references: Vec::new(),
            bounds: Vec::new(),
            slots: Vec::with_capacity(pointers.len()),
            maximum_bytes,
        };
        let mut indexes = HashMap::new();
        for pointer in pointers {
            let Some(table) = active.get(&pointer.metadata().address().history_id) else {
                plan.slots.push(None);
                continue;
            };
            let current = plan.include_reference(
                &mut indexes,
                pointer.metadata().reference().clone(),
                pointer.record_bytes(),
            );
            let predecessor = match (images, pointer.metadata().previous_version()) {
                (ItemHistoryImages::WithPredecessor, Some(previous)) => {
                    let reference = ItemHistoryReference::try_new(
                        pointer.metadata().address().clone(),
                        previous,
                    )?;
                    let bound = pointer
                        .predecessor_record_bytes_bound()
                        .ok_or_else(invalid_resolution)?;
                    Some(plan.include_reference(&mut indexes, reference, bound))
                }
                _ => None,
            };
            plan.slots.push(Some(HistoryResolutionIndex {
                table: Arc::clone(table),
                current,
                predecessor,
            }));
        }
        plan.require_budget()?;
        Ok(plan)
    }

    fn include_reference(
        &mut self,
        indexes: &mut HashMap<ItemHistoryReference, usize>,
        reference: ItemHistoryReference,
        bound: usize,
    ) -> usize {
        if let Some(index) = indexes.get(&reference).copied() {
            self.bounds[index] = self.bounds[index].min(bound);
            return index;
        }
        let index = self.references.len();
        indexes.insert(reference.clone(), index);
        self.references.push(reference);
        self.bounds.push(bound);
        index
    }

    fn require_budget(&self) -> StorageResult<()> {
        let mut bytes = 0usize;
        for (reference, bound) in self.references.iter().zip(&self.bounds) {
            let envelope = StreamRowValue::history_envelope_bytes(reference.address())?;
            bytes = bytes
                .checked_add(*bound)
                .and_then(|bytes| bytes.checked_add(envelope))
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        }
        if bytes > self.maximum_bytes.get() {
            return Err(storage_types::InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }

    fn read(&self) -> ItemHistoryReadBatch<'_> {
        ItemHistoryReadBatch {
            references: &self.references,
            maximum_bytes: self.maximum_bytes,
        }
    }

    fn require_headers(
        &self,
        pointers: &[ItemHistoryPointer],
        headers: &[Option<ItemHistoryRecordHeader>],
    ) -> StorageResult<()> {
        if headers.len() != self.references.len() {
            return Err(invalid_resolution());
        }
        for ((header, reference), bound) in headers.iter().zip(&self.references).zip(&self.bounds) {
            let header = header.as_ref().ok_or_else(missing_history)?;
            if header.metadata().reference() != reference || header.record_bytes() > *bound {
                return Err(invalid_resolution());
            }
        }
        for (pointer, slot) in pointers.iter().zip(&self.slots) {
            let Some(slot) = slot else {
                continue;
            };
            let header = headers[slot.current].as_ref().ok_or_else(missing_history)?;
            if header.metadata() != pointer.metadata()
                || header.record_bytes() != pointer.record_bytes()
            {
                return Err(invalid_resolution());
            }
            if let Some(previous) = slot.predecessor
                && headers[previous]
                    .as_ref()
                    .ok_or_else(missing_history)?
                    .metadata()
                    .kind()
                    != ItemHistoryKind::Image
            {
                return Err(invalid_resolution());
            }
        }
        Ok(())
    }

    fn resolve_entries(
        self,
        headers: Vec<Option<ItemHistoryRecordHeader>>,
        entries: Vec<Option<ItemHistoryEntry>>,
    ) -> StorageResult<Vec<Option<ResolvedItemHistory>>> {
        if entries.len() != self.references.len() {
            return Err(invalid_resolution());
        }
        let mut resolved = Vec::with_capacity(entries.len());
        for (header, entry) in headers.into_iter().zip(entries) {
            let header = header.ok_or_else(missing_history)?;
            let entry = entry.ok_or_else(missing_history)?;
            if entry.metadata() != header.metadata() {
                return Err(invalid_resolution());
            }
            resolved.push(Arc::new(entry));
        }
        Ok(self
            .slots
            .into_iter()
            .map(|slot| {
                slot.map(|slot| ResolvedItemHistory {
                    table: slot.table,
                    current: Arc::clone(&resolved[slot.current]),
                    predecessor: slot.predecessor.map(|index| Arc::clone(&resolved[index])),
                })
            })
            .collect())
    }
}

fn invalid_resolution() -> StorageError {
    StorageError::internal("Exact history resolution differs from its pointer")
}
fn missing_history() -> StorageError {
    storage_types::StorageEnum::HistoryUnavailable.into()
}
