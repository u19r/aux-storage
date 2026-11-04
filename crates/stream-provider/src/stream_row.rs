//! Binary stream envelope shared by every physical store.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, TimestampMillis};

use crate::{
    StreamDataType, StreamItem,
    constants::{STREAM_ROW_HEADER_BYTES, STREAM_ROW_PREFIX},
};

/// Durable row payload. The ordered physical address owns the item ID, so
/// versionstamped keys and preallocated IDs use the same stored value format.
#[derive(Clone)]
pub struct StreamRowValue {
    stream_name: Option<StreamName>,
    data: Vec<u8>,
    data_type: StreamDataType,
    created_at: TimestampMillis,
    encoding: StreamRowEncoding,
}

impl StreamRowValue {
    #[must_use]
    pub fn new(
        stream_name: StreamName,
        data: Vec<u8>,
        data_type: StreamDataType,
        created_at: TimestampMillis,
    ) -> Self {
        Self {
            stream_name: Some(stream_name),
            data,
            data_type,
            created_at,
            encoding: StreamRowEncoding::Opaque,
        }
    }

    /// Encode the envelope once; the payload is already serialized bytes.
    pub fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        let name = self.stream_name.as_ref().map(AsRef::as_ref);
        let name_len = match name {
            Some(name) => u32::try_from(name.len())
                .ok()
                .filter(|length| *length != u32::MAX)
                .ok_or_else(|| StorageError::internal("stream row name is too large"))?,
            None => u32::MAX,
        };
        let data_len = u32::try_from(self.data.len())
            .map_err(|_| StorageError::internal("stream row payload is too large"))?;
        let mut bytes = Vec::with_capacity(
            STREAM_ROW_HEADER_BYTES + name.map_or(0, <[u8]>::len) + self.data.len(),
        );
        bytes.extend_from_slice(match self.encoding {
            StreamRowEncoding::Opaque => STREAM_ROW_PREFIX,
            StreamRowEncoding::Image => crate::constants::STREAM_IMAGE_ROW_PREFIX,
        });
        bytes.push(self.data_type as u8);
        bytes.extend_from_slice(&self.created_at.timestamp_millis().to_be_bytes());
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(&data_len.to_be_bytes());
        if let Some(name) = name {
            bytes.extend_from_slice(name);
        }
        bytes.extend_from_slice(&self.data);
        Ok(bytes)
    }

    #[must_use]
    pub fn into_item(self, id: StreamItemId) -> StreamItem {
        StreamItem {
            id,
            stream_name: self.stream_name,
            data: self.data,
            data_type: self.data_type,
            created_at: self.created_at,
        }
    }

    #[must_use]
    pub fn stream_name(&self) -> Option<&StreamName> {
        self.stream_name.as_ref()
    }

    #[must_use]
    pub fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    /// Native value stamps and prefix reads include the envelope's variable
    /// stream-name bytes before the logical payload.
    pub fn payload_offset(&self) -> StorageResult<u32> {
        let length = STREAM_ROW_HEADER_BYTES
            .checked_add(
                self.stream_name
                    .as_ref()
                    .map_or(0, |name| name.as_ref().len()),
            )
            .ok_or_else(|| StorageError::internal("stream row header is too large"))?;
        u32::try_from(length).map_err(|_| StorageError::internal("stream row header is too large"))
    }

    pub fn history_metadata_prefix_bytes(
        history: &storage_types::ItemHistoryAddress,
    ) -> StorageResult<std::num::NonZeroUsize> {
        let bytes = Self::history_envelope_bytes(history)?
            + storage_types::ItemHistoryRecord::metadata_length_bound();
        std::num::NonZeroUsize::new(bytes)
            .ok_or_else(|| StorageError::internal("Invalid history prefix length"))
    }

    pub fn history_envelope_bytes(
        history: &storage_types::ItemHistoryAddress,
    ) -> StorageResult<usize> {
        Ok(STREAM_ROW_HEADER_BYTES + history.physical_stream_name()?.as_ref().len())
    }

    pub fn decode_history_header_prefix(
        prefix: &[u8],
        value_length: usize,
        expected: (
            &storage_types::ItemHistoryAddress,
            storage_types::ItemStreamVersion,
        ),
    ) -> StorageResult<storage_types::ItemHistoryRecordHeader> {
        let invalid = || StorageError::internal("Invalid history envelope prefix");
        let (bytes, _) = prefix
            .split_first_chunk::<STREAM_ROW_HEADER_BYTES>()
            .ok_or_else(invalid)?;
        if &bytes[..4] != crate::constants::STREAM_IMAGE_ROW_PREFIX {
            return Err(invalid());
        }
        let header = StreamRowHeader::parse(bytes)?;
        let name = expected.0.physical_stream_name()?;
        let offset = STREAM_ROW_HEADER_BYTES + name.as_ref().len();
        if header.name_len as usize != name.as_ref().len()
            || prefix.get(STREAM_ROW_HEADER_BYTES..offset) != Some(name.as_ref())
            || value_length.checked_sub(offset) != Some(header.data_len as usize)
        {
            return Err(invalid());
        }
        let decoded = storage_types::ItemHistoryRecord::decode_header_prefix(
            prefix.get(offset..).ok_or_else(invalid)?,
            expected,
            header.data_len as usize,
        )?;
        let kind = match header.data_type {
            StreamDataType::DynamoDbJson => storage_types::ItemHistoryKind::Image,
            StreamDataType::DeleteMarker => storage_types::ItemHistoryKind::Deleted,
            _ => return Err(invalid()),
        };
        crate::item_history::require_history_envelope(decoded.metadata(), kind, header.created_at)?;
        Ok(decoded)
    }

    fn from_item_history(item: StreamItem) -> StorageResult<Self> {
        if !matches!(
            item.data_type,
            StreamDataType::DynamoDbJson | StreamDataType::DeleteMarker
        ) {
            return Err(StorageError::internal("Invalid retained stream image type"));
        }
        let mut row = Self::from(item);
        row.encoding = StreamRowEncoding::Image;
        Ok(row)
    }

    pub fn from_planned(row: crate::StreamWriteKind<StreamItem>) -> StorageResult<Self> {
        match row {
            crate::StreamWriteKind::Item(item) => Self::from_item_history(item),
            crate::StreamWriteKind::Table(item) | crate::StreamWriteKind::System(item) => {
                Ok(Self::from(item))
            }
            crate::StreamWriteKind::User(item) => Ok(Self::from(item)),
        }
    }
}

impl From<StreamItem> for StreamRowValue {
    fn from(item: StreamItem) -> Self {
        Self {
            stream_name: item.stream_name,
            data: item.data,
            data_type: item.data_type,
            created_at: item.created_at,
            encoding: StreamRowEncoding::Opaque,
        }
    }
}

/// The physical key supplies the ID; the remaining payload is opaque bytes.
pub fn decode_stream_item_row(id: StreamItemId, bytes: &[u8]) -> StorageResult<StreamItem> {
    let mut item = decode_stream_envelope(id, bytes, STREAM_ROW_PREFIX)?;
    if matches!(
        item.data_type,
        StreamDataType::DynamoDbJson | StreamDataType::DeleteMarker
    ) {
        return Err(StorageError::internal(
            "Inline Dynamo stream image format is obsolete",
        ));
    }
    if item.data_type == StreamDataType::StreamPointer {
        let pointer = crate::table_history_index::decode_table_history_pointer(&item)?;
        item.data = storage_types::storage_serde::to_bytes(&pointer)?;
    }
    Ok(item)
}

/// Reference-only index values have an opaque envelope and a typed decoder;
/// ordinary user rows still reject removed inline Dynamo image formats.
pub fn decode_table_history_index_row(id: StreamItemId, bytes: &[u8]) -> StorageResult<StreamItem> {
    let item = decode_stream_envelope(id, bytes, STREAM_ROW_PREFIX)?;
    if item.data_type != StreamDataType::StreamPointer {
        return Err(StorageError::internal(
            "Invalid table history index row type",
        ));
    }
    Ok(item)
}

pub(crate) fn decode_stream_image_row(id: StreamItemId, bytes: &[u8]) -> StorageResult<StreamItem> {
    let item = decode_stream_envelope(id, bytes, crate::constants::STREAM_IMAGE_ROW_PREFIX)?;
    if !matches!(
        item.data_type,
        StreamDataType::DynamoDbJson | StreamDataType::DeleteMarker
    ) {
        return Err(StorageError::internal("Invalid retained stream image type"));
    }
    Ok(item)
}

/// Item history is self-contained; reference indexes keep their typed bytes.
pub fn decode_materialized_stream_row(id: StreamItemId, bytes: &[u8]) -> StorageResult<StreamItem> {
    if !bytes.starts_with(crate::constants::STREAM_IMAGE_ROW_PREFIX) {
        return decode_stream_item_row(id, bytes);
    }
    let item = decode_stream_image_row(id, bytes)?;
    let entry = crate::item_history::decode_item_history(
        &item,
        storage_types::ItemHistoryRecord::encoded_length_bound(),
    )?;
    crate::item_history::resolve_item_history(item, &entry)
}

impl StreamItem {
    pub fn require_address(&self, stream: &StreamName, id: StreamItemId) -> StorageResult<()> {
        if self.id != id || self.stream_name.as_ref() != Some(stream) {
            return Err(StorageError::internal(
                "Stream row address differs from its physical key",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum StreamRowEncoding {
    Opaque,
    Image,
}

fn decode_stream_envelope(
    id: StreamItemId,
    bytes: &[u8],
    prefix: &[u8; 4],
) -> StorageResult<StreamItem> {
    let (header, body) = bytes
        .split_first_chunk::<STREAM_ROW_HEADER_BYTES>()
        .ok_or_else(|| StorageError::internal("truncated stream row header"))?;
    if &header[..4] != prefix {
        return Err(StorageError::internal("invalid stream row format"));
    }
    let header = StreamRowHeader::parse(header)?;
    let name_len = header.name_len;
    let (stream_name, data) = if name_len == u32::MAX {
        (None, body)
    } else {
        let (name, data) = body
            .split_at_checked(name_len as usize)
            .ok_or_else(|| StorageError::internal("truncated stream row name"))?;
        (Some(StreamName::from(name.to_vec())), data)
    };
    if data.len() != header.data_len as usize {
        return Err(StorageError::internal("invalid stream row payload length"));
    }
    Ok(StreamRowValue {
        stream_name,
        data: data.to_vec(),
        data_type: header.data_type,
        created_at: header.created_at,
        encoding: StreamRowEncoding::Opaque,
    }
    .into_item(id))
}

struct StreamRowHeader {
    data_type: StreamDataType,
    created_at: TimestampMillis,
    name_len: u32,
    data_len: u32,
}

impl StreamRowHeader {
    fn parse(header: &[u8; STREAM_ROW_HEADER_BYTES]) -> StorageResult<Self> {
        let data_type = decode_data_type(header[4])?;
        let timestamp = i64::from_be_bytes(
            header[5..13]
                .try_into()
                .map_err(|_| StorageError::internal("invalid stream row timestamp"))?,
        );
        let name_len = u32::from_be_bytes(
            header[13..17]
                .try_into()
                .map_err(|_| StorageError::internal("invalid stream row name length"))?,
        );
        let data_len = u32::from_be_bytes(
            header[17..21]
                .try_into()
                .map_err(|_| StorageError::internal("invalid stream row payload length"))?,
        );
        Ok(Self {
            data_type,
            created_at: TimestampMillis::from_timestamp(timestamp),
            name_len,
            data_len,
        })
    }
}

fn decode_data_type(tag: u8) -> StorageResult<StreamDataType> {
    Ok(match tag {
        0 => StreamDataType::Binary,
        1 => StreamDataType::Json,
        2 => StreamDataType::Text,
        3 => StreamDataType::DynamoDbJson,
        4 => StreamDataType::DeleteMarker,
        5 => StreamDataType::StreamPointer,
        _ => return Err(StorageError::internal("invalid stream row data type")),
    })
}
