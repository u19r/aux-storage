//! Materialized history uses ordinary native value/chunk ownership. The
//! generic item-stream port resolves its snapshot without external bodies.

use storage_types::{
    ItemHistoryChange, ItemHistoryEntry, ItemHistoryKind, ItemHistoryMetadata,
    ItemHistoryPublication, ItemHistoryRecord, ItemHistoryRecordHeader, ItemHistoryReference,
    ItemStreamVersion, MaxIndexers, StorageError, StorageResult, StreamItemId, storage_serde,
};

use crate::{StreamDataType, StreamItem};

/// Domain-admitted aggregate bytes include native stream envelopes.
#[derive(Clone, Copy)]
pub struct ItemHistoryReadBatch<'a> {
    pub references: &'a [ItemHistoryReference],
    pub maximum_bytes: std::num::NonZeroUsize,
}

/// Exact rows selected by the table cleanup owner in the same native attempt.
pub enum HistoryRowDelete {
    Item(ItemHistoryReference),
    Table {
        lane: storage_types::TableHistoryLane,
        sequence: storage_types::TableStreamSequence,
    },
}

impl HistoryRowDelete {
    pub fn encode_address(&self) -> StorageResult<(storage_types::StreamName, StreamItemId)> {
        match self {
            Self::Item(reference) => Ok((
                reference.address().physical_stream_name()?,
                reference.version().into(),
            )),
            Self::Table { lane, sequence } => {
                Ok((lane.physical_stream_name()?, sequence.native_key()))
            }
        }
    }
}

pub trait HistoryRowDeleteAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn delete_history_rows(
        &mut self,
        rows: &[HistoryRowDelete],
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

/// Exact metadata and body reads remain in the caller's native snapshot.
/// The operation owner admits body bytes from headers or qualified pointers
/// before loading images, and owns eligibility from the catalog observations.
pub trait ItemHistoryReadAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn load_item_history_table_states(
        &mut self,
        histories: &[storage_types::TableHistoryId],
    ) -> impl Future<Output = StorageResult<Vec<Option<storage_types::TableHistoryState>>>> + Send;

    fn load_item_history_headers(
        &mut self,
        read: ItemHistoryReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<Vec<Option<ItemHistoryRecordHeader>>>> + Send;

    fn load_item_history_entries(
        &mut self,
        read: ItemHistoryReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<Vec<Option<ItemHistoryEntry>>>> + Send;
}

impl TryFrom<ItemHistoryEntry> for StreamItem {
    type Error = StorageError;

    fn try_from(entry: ItemHistoryEntry) -> StorageResult<Self> {
        let metadata = entry.metadata();
        Ok(Self {
            id: metadata.version().into(),
            stream_name: Some(metadata.address().physical_stream_name()?),
            data: entry.encode()?,
            data_type: match metadata.kind() {
                ItemHistoryKind::Image => StreamDataType::DynamoDbJson,
                ItemHistoryKind::Deleted => StreamDataType::DeleteMarker,
            },
            created_at: match metadata.publication() {
                ItemHistoryPublication::Event { created_at, .. } => *created_at,
                ItemHistoryPublication::PredecessorAnchor => {
                    storage_types::TimestampMillis::from_timestamp(0)
                }
            },
        })
    }
}

pub fn decode_item_history_entry(
    bytes: &[u8],
    expected: &ItemHistoryReference,
    maximum: usize,
) -> StorageResult<ItemHistoryEntry> {
    if bytes.len() > maximum {
        return Err(storage_types::InfrastructureResultKind::NativeCapacityRejected.into());
    }
    let row =
        crate::stream_row::decode_stream_image_row(StreamItemId::from(expected.version()), bytes)?;
    let entry = decode_item_history(&row, maximum)?;
    if entry.metadata().reference() != expected {
        return Err(StorageError::internal(
            "Item history differs from its exact reference",
        ));
    }
    Ok(entry)
}

pub(crate) fn decode_item_history(
    row: &StreamItem,
    maximum: usize,
) -> StorageResult<ItemHistoryEntry> {
    let invalid = || StorageError::internal("Invalid materialized stream history row");
    let stream = row.stream_name.as_ref().ok_or_else(invalid)?;
    let version = ItemStreamVersion::from(row.id);
    if StreamItemId::from(version) != row.id {
        return Err(invalid());
    }
    let record = ItemHistoryRecord::decode_physical(&row.data, (stream, version), maximum)?;
    let kind = match row.data_type {
        StreamDataType::DynamoDbJson => ItemHistoryKind::Image,
        StreamDataType::DeleteMarker => ItemHistoryKind::Deleted,
        _ => return Err(invalid()),
    };
    require_history_envelope(record.metadata(), kind, row.created_at)?;
    record.into_entry(MaxIndexers::try_new(storage_types::MAX_INDEXERS_CAPACITY)?)
}

pub(crate) fn require_history_envelope(
    metadata: &ItemHistoryMetadata,
    kind: ItemHistoryKind,
    timestamp: storage_types::TimestampMillis,
) -> StorageResult<()> {
    if metadata.kind() != kind {
        return Err(StorageError::internal(
            "History kind differs from its envelope",
        ));
    }
    if let ItemHistoryPublication::Event { created_at, .. } = metadata.publication()
        && *created_at != timestamp
    {
        return Err(StorageError::internal(
            "History time differs from its envelope",
        ));
    }
    Ok(())
}

pub(crate) fn resolve_item_history(
    mut row: StreamItem,
    entry: &ItemHistoryEntry,
) -> StorageResult<StreamItem> {
    row.data = match entry.change() {
        ItemHistoryChange::Image { item, .. } => {
            storage_serde::compress_json_bytes(&item.dynamo_json_bytes()?)
        }
        ItemHistoryChange::Deleted { key } => {
            storage_serde::compress_json_bytes(&serde_json::to_vec(&key)?)
        }
    };
    Ok(row)
}
