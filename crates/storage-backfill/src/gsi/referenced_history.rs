use storage_types::{ItemStreamVersion, StorageError, StorageResult, StreamName};
use stream_provider::{StoredStreamPointer, StreamItem};

use super::{GsiReferencedHistoryItem, GsiStreamCandidate};

pub struct GsiReferencedHistoryRead<'a> {
    pub history_id: storage_types::TableHistoryId,
    /// Position of the SystemStream candidate that owns this item history.
    pub candidate_ordinal: usize,
    /// Item stream containing the old and new images referenced by the
    /// candidate.
    pub stream_name: &'a StreamName,
    /// Latest item-stream version that belongs to the candidate mutation.
    pub through: ItemStreamVersion,
}

impl<'a> GsiReferencedHistoryRead<'a> {
    #[must_use]
    pub fn physical_stream_name(&self) -> StreamName {
        storage_types::ItemHistoryAddress {
            history_id: self.history_id,
            stream_name: self.stream_name.clone(),
        }
        .physical_stream_name()
    }

    #[must_use]
    pub fn plan(
        target_table: &storage_types::BoundTableInfo,
        candidates: &'a [GsiStreamCandidate],
    ) -> Vec<Self> {
        candidates
            .iter()
            .enumerate()
            .filter_map(|(candidate_ordinal, candidate)| match &candidate.pointer {
                StoredStreamPointer::Pointer {
                    table_name,
                    stream_name,
                    item_stream_version,
                    ..
                } if table_name == &target_table.table_name
                    && candidate
                        .pointer
                        .matches_table_history(target_table.history_id) =>
                {
                    Some(Self {
                        history_id: target_table.history_id,
                        candidate_ordinal,
                        stream_name,
                        through: *item_stream_version,
                    })
                }
                StoredStreamPointer::Pointer { .. } | StoredStreamPointer::Embedded { .. } => None,
            })
            .collect()
    }
}

pub struct GsiReferencedHistory {
    /// Position of the SystemStream candidate that owns these items.
    pub candidate_ordinal: usize,
    pub items: Vec<StreamItem>,
}

impl GsiReferencedHistory {
    pub fn group(
        items: impl IntoIterator<Item = GsiReferencedHistoryItem>,
    ) -> StorageResult<Vec<Self>> {
        let mut histories: Vec<Self> = Vec::new();
        for observed in items {
            match histories.last_mut() {
                Some(previous) if previous.candidate_ordinal > observed.candidate_ordinal => {
                    return Err(StorageError::internal(
                        "GSI referenced history items are not ordered by candidate",
                    ));
                }
                Some(previous) if previous.candidate_ordinal == observed.candidate_ordinal => {
                    if !matches!(previous.items.as_slice(), [current] if observed.item.id < current.id)
                    {
                        return Err(StorageError::internal(
                            "GSI referenced history must contain at most two descending images",
                        ));
                    }
                    previous.items.push(observed.item);
                }
                _ => histories.push(Self {
                    candidate_ordinal: observed.candidate_ordinal,
                    items: vec![observed.item],
                }),
            }
        }
        Ok(histories)
    }

    pub fn attach_all(
        candidates: &mut [GsiStreamCandidate],
        histories: impl IntoIterator<Item = Self>,
    ) -> StorageResult<()> {
        for history in histories {
            let candidate = candidates
                .get_mut(history.candidate_ordinal)
                .ok_or_else(|| {
                    StorageError::internal("GSI referenced history candidate ordinal is invalid")
                })?;
            if !matches!(candidate.pointer, StoredStreamPointer::Pointer { .. }) {
                return Err(StorageError::internal(
                    "GSI referenced history belongs to an embedded pointer",
                ));
            }
            if candidate.referenced_items.replace(history.items).is_some() {
                return Err(StorageError::internal(
                    "GSI referenced history was returned more than once",
                ));
            }
        }
        Ok(())
    }
}
