//! One bounded materialized history value; metadata reads never parse its
//! image.

use crate::{
    IndexerDeclaration, ItemHistoryAddress, ItemHistoryChange, ItemHistoryEntry, ItemHistoryKind,
    ItemHistoryMetadata, ItemStreamVersion, MaxIndexers, StorageError, StorageResult, StreamName,
    WireItem,
    constants::{
        ITEM_HISTORY_METADATA_MAX_BYTES, ITEM_HISTORY_RECORD_FORMAT,
        ITEM_HISTORY_RECORD_HEADER_BYTES,
    },
};

/// Borrowed native record after framing, authority and byte admission checks.
pub struct ItemHistoryRecord<'a> {
    metadata: ItemHistoryMetadata,
    indexers: Option<Vec<String>>,
    payload: &'a [u8],
}

impl ItemHistoryEntry {
    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        let (indexers, payload) = match self.change() {
            ItemHistoryChange::Image { item, indexers } => {
                (Some(indexers.names()), item.dynamo_json_bytes()?)
            }
            ItemHistoryChange::Deleted { key } => (None, serde_json::to_vec(key)?.into()),
        };
        let metadata = serde_json::to_vec(&(self.metadata(), indexers))?;
        if metadata.is_empty() || metadata.len() > ITEM_HISTORY_METADATA_MAX_BYTES {
            return Err(invalid_record());
        }
        let metadata_len = u32::try_from(metadata.len()).map_err(|_| invalid_record())?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| invalid_record())?;
        let size = ITEM_HISTORY_RECORD_HEADER_BYTES
            .checked_add(metadata.len())
            .and_then(|size| size.checked_add(payload.len()))
            .ok_or_else(invalid_record)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| crate::InfrastructureResultKind::NativeCapacityRejected)?;
        bytes.push(ITEM_HISTORY_RECORD_FORMAT);
        bytes.extend_from_slice(&metadata_len.to_be_bytes());
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(&self.metadata().native_versionstamp().unwrap_or([0; 12]));
        bytes.extend_from_slice(&metadata);
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }
}

/// Validated metadata and byte dimensions obtained without an image read.
pub struct ItemHistoryRecordHeader {
    metadata: ItemHistoryMetadata,
    record_bytes: usize,
    payload_bytes: usize,
}

impl ItemHistoryRecordHeader {
    pub fn metadata(&self) -> &ItemHistoryMetadata {
        &self.metadata
    }
    pub fn into_metadata(self) -> ItemHistoryMetadata {
        self.metadata
    }
    pub fn record_bytes(&self) -> usize {
        self.record_bytes
    }
    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }
}

impl<'a> ItemHistoryRecord<'a> {
    pub const fn encoded_length_bound() -> usize {
        Self::metadata_length_bound() + crate::LogicalItemSize::maximum_json_projection_bytes()
    }

    /// Offset of FDB's twelve-byte commit stamp in the logical value. The
    /// native lowerer adds any enclosing row header and stages one atomic
    /// versionstamped-value mutation, including for chunked large values.
    pub const VERSIONSTAMP_OFFSET: u32 = 9;

    pub fn decode(
        bytes: &'a [u8],
        expected: (&ItemHistoryAddress, ItemStreamVersion),
        maximum: usize,
    ) -> StorageResult<Self> {
        let frame = HistoryRecordFrame::decode(bytes, maximum)?;
        if frame.record_bytes != bytes.len() {
            return Err(invalid_record());
        }
        let (metadata, indexers) = frame.decode_metadata(bytes, expected)?;
        Ok(Self {
            metadata,
            indexers,
            payload: &bytes[frame.metadata_end..],
        })
    }

    /// A native envelope supplies an immutable physical address rather than
    /// its descriptive logical source name.
    pub fn decode_physical(
        bytes: &'a [u8],
        expected: (&StreamName, ItemStreamVersion),
        maximum: usize,
    ) -> StorageResult<Self> {
        let frame = HistoryRecordFrame::decode(bytes, maximum)?;
        if frame.record_bytes != bytes.len() {
            return Err(invalid_record());
        }
        let (metadata, indexers) = frame.decode_metadata_value(bytes)?;
        if metadata.address().physical_stream_name()? != *expected.0
            || metadata.version() != expected.1
        {
            return Err(invalid_record());
        }
        Ok(Self {
            metadata,
            indexers,
            payload: &bytes[frame.metadata_end..],
        })
    }

    /// Native prefix reads inspect cleanup authority without fetching the
    /// snapshot. The native head supplies its exact logical value length.
    pub fn decode_header_prefix(
        bytes: &[u8],
        expected: (&ItemHistoryAddress, ItemStreamVersion),
        expected_length: usize,
    ) -> StorageResult<ItemHistoryRecordHeader> {
        let frame = HistoryRecordFrame::decode(bytes, expected_length)?;
        if frame.record_bytes != expected_length {
            return Err(invalid_record());
        }
        let (metadata, _) = frame.decode_metadata(bytes, expected)?;
        Ok(ItemHistoryRecordHeader {
            metadata,
            record_bytes: frame.record_bytes,
            payload_bytes: frame.record_bytes - frame.metadata_end,
        })
    }

    pub const fn metadata_length_bound() -> usize {
        ITEM_HISTORY_RECORD_HEADER_BYTES + ITEM_HISTORY_METADATA_MAX_BYTES
    }

    pub fn metadata(&self) -> &ItemHistoryMetadata {
        &self.metadata
    }

    pub fn into_entry(self, capacity: MaxIndexers) -> StorageResult<ItemHistoryEntry> {
        let change = match self.indexers {
            Some(names) => {
                let indexers = IndexerDeclaration::try_new(names, capacity)?;
                let item = WireItem::dynamo_json(self.payload.to_vec());
                item.logical_size()?;
                indexers.require_values(&item)?;
                ItemHistoryChange::Image { item, indexers }
            }
            None => ItemHistoryChange::Deleted {
                key: serde_json::from_slice(self.payload)?,
            },
        };
        ItemHistoryEntry::try_new(self.metadata, change)
    }
}

/// Validated offsets admit metadata before allocating its decoded fields.
struct HistoryRecordFrame {
    metadata_end: usize,
    record_bytes: usize,
    stamp: [u8; 12],
}

impl HistoryRecordFrame {
    fn decode(bytes: &[u8], maximum: usize) -> StorageResult<Self> {
        if bytes.len() < ITEM_HISTORY_RECORD_HEADER_BYTES || bytes[0] != ITEM_HISTORY_RECORD_FORMAT
        {
            return Err(invalid_record());
        }
        let metadata_bytes =
            u32::from_be_bytes(bytes[1..5].try_into().map_err(|_| invalid_record())?) as usize;
        let payload_bytes =
            u32::from_be_bytes(bytes[5..9].try_into().map_err(|_| invalid_record())?) as usize;
        if metadata_bytes == 0
            || metadata_bytes > ITEM_HISTORY_METADATA_MAX_BYTES
            || payload_bytes == 0
        {
            return Err(invalid_record());
        }
        let metadata_end = ITEM_HISTORY_RECORD_HEADER_BYTES
            .checked_add(metadata_bytes)
            .ok_or_else(invalid_record)?;
        let record_bytes = metadata_end
            .checked_add(payload_bytes)
            .filter(|size| *size <= maximum)
            .ok_or_else(invalid_record)?;
        if bytes.len() > record_bytes {
            return Err(invalid_record());
        }
        Ok(Self {
            metadata_end,
            record_bytes,
            stamp: bytes[9..21].try_into().map_err(|_| invalid_record())?,
        })
    }

    fn decode_metadata(
        &self,
        bytes: &[u8],
        expected: (&ItemHistoryAddress, ItemStreamVersion),
    ) -> StorageResult<(ItemHistoryMetadata, Option<Vec<String>>)> {
        let (metadata, indexers) = self.decode_metadata_value(bytes)?;
        if metadata.address() != expected.0 || metadata.version() != expected.1 {
            return Err(invalid_record());
        }
        Ok((metadata, indexers))
    }

    fn decode_metadata_value(
        &self,
        bytes: &[u8],
    ) -> StorageResult<(ItemHistoryMetadata, Option<Vec<String>>)> {
        let metadata_bytes = bytes
            .get(ITEM_HISTORY_RECORD_HEADER_BYTES..self.metadata_end)
            .ok_or_else(invalid_record)?;
        let (metadata, indexers): (ItemHistoryMetadata, Option<Vec<String>>) =
            serde_json::from_slice(metadata_bytes)?;
        let metadata = metadata.bind_versionstamp(self.stamp)?;
        if indexers.is_none() != (metadata.kind() == ItemHistoryKind::Deleted) {
            return Err(invalid_record());
        }
        Ok((metadata, indexers))
    }
}

fn invalid_record() -> StorageError {
    StorageError::internal("Invalid materialized item history record")
}
